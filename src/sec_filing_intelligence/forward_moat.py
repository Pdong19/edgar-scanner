"""Forward Moat Scanner — trajectory signals for companies building moats.

Part of the SEC Filing Intelligence system. Detects backlog acceleration,
segment crossover, partnership mismatches, new TAM language, capex/R&D
inflection, and technology milestones.

CLI:
    python -m sec_filing_intelligence.forward_moat --run
    python -m sec_filing_intelligence.forward_moat --run --dry-run
    python -m sec_filing_intelligence.forward_moat --ticker RKLB
    python -m sec_filing_intelligence.forward_moat --latest
"""

import argparse
import csv
import re
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import requests

from .config import (
    EDGAR_FULL_TEXT_SEARCH_URL,
    EDGAR_RATE_LIMIT_RPS,
    EDGAR_REQUEST_TIMEOUT,
    EDGAR_SUBMISSIONS_URL,
    EDGAR_USER_AGENT,
    FORWARD_BACKLOG_GROWTH_THRESHOLDS,
    FORWARD_BACKLOG_KEYWORDS,
    FORWARD_CAPEX_ATH_CRASH_MIN,
    FORWARD_CAPEX_GROWTH_MIN,
    FORWARD_MILESTONE_KEYWORDS,
    FORWARD_MOAT_OUTPUT_DIR,
    FORWARD_MOAT_MAX_MARKET_CAP,
    FORWARD_MOAT_SCAN_TOP_N,
    FORWARD_MILESTONE_BIOTECH_NOISE,
    FORWARD_MILESTONE_BIOTECH_SECTORS,
    FORWARD_PARTNERSHIP_EXCLUDE_INSTITUTIONAL,
    FORWARD_PARTNERSHIP_FORTUNE_200,
    FORWARD_PARTNERSHIP_KEYWORDS,
    FORWARD_TAM_KEYWORDS,
    TABLE_DEEP_DIVE_RESULTS,
    TABLE_DISCOVERY_FLAGS,
    TABLE_FORWARD_MOAT_SCORES,
    TABLE_KILL_LIST,
)
from .db import get_connection, run_migration
from .filing_scanner import _cik_to_ticker_lookup, _load_cik_maps
from .utils import get_logger, rate_limiter

logger = get_logger("forward_moat", "screener_forward_moat.log")

_headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# Regex for extracting ticker from EFTS display_names like "Company (TICK) (CIK ...)"
_TICKER_FROM_DN_RE = re.compile(r"\(([A-Z]{2,5})\)")

# ── Backlog amount extraction ────────────────────────────────────────────────

_BACKLOG_AMOUNT_RE = re.compile(
    r"(?:backlog|order book|unfunded orders|contract awards)"
    r"[^.]{0,80}?"
    r"\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?",
    re.IGNORECASE,
)

_BACKLOG_GROWTH_RE = re.compile(
    r"(?:backlog|order book)[^.]{0,60}?"
    r"(?:increased|grew|rose|doubled|tripled|record)",
    re.IGNORECASE,
)


def _extract_backlog_amount(text: str) -> float | None:
    """Extract the largest dollar amount near backlog keywords from 10-K text."""
    amounts = []
    for m in _BACKLOG_AMOUNT_RE.finditer(text):
        raw = float(m.group(1).replace(",", ""))
        multiplier_str = (m.group(2) or "").lower()
        if multiplier_str == "billion":
            raw *= 1e9
        elif multiplier_str == "million":
            raw *= 1e6
        elif multiplier_str == "thousand":
            raw *= 1e3
        elif raw < 1000:
            raw *= 1e6
        amounts.append(raw)
    return max(amounts) if amounts else None


def score_backlog(
    current_amount: float | None, prior_amount: float | None
) -> tuple[float, dict]:
    """Score backlog acceleration signal (0-8 points).

    Args:
        current_amount: Current year backlog in dollars (or None).
        prior_amount: Prior year backlog in dollars (or None).

    Returns:
        (score, metadata_dict)
    """
    meta = {
        "backlog_current": current_amount,
        "backlog_prior": prior_amount,
        "backlog_growth_pct": None,
    }

    if current_amount is None or prior_amount is None or prior_amount <= 0:
        return 0, meta

    growth = (current_amount - prior_amount) / prior_amount
    meta["backlog_growth_pct"] = round(growth, 4)

    for threshold, points in FORWARD_BACKLOG_GROWTH_THRESHOLDS:
        if growth >= threshold:
            return points, meta

    return 0, meta


# ── 10-K text fetcher (SEC submissions API) ─────────────────────────────────

_10K_TEXT_MAX_BYTES = 100_000  # first 100KB of filing text (backlog is usually early)


@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _sec_get(url: str) -> requests.Response | None:
    """Rate-limited GET against SEC EDGAR APIs with retry."""
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=_headers, timeout=EDGAR_REQUEST_TIMEOUT)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(1)
            else:
                logger.debug("SEC GET failed for %s: %s", url, e)
    return None


def _fetch_10k_text(ticker: str, cik: str, year: int) -> str | None:
    """Download the first 100KB of the most recent 10-K filing for a given year.

    Args:
        ticker: Ticker symbol (for logging only).
        cik: CIK string (any padding -- will be zero-padded to 10 digits).
        year: Fiscal year to target (e.g. 2025). Searches for 10-K or 10-K/A
              with a filing date in [year-01-01, year+1-06-30].

    Returns:
        Filing text (first 100KB) or None if no filing found.
    """
    cik_padded = cik.zfill(10)
    submissions_url = f"{EDGAR_SUBMISSIONS_URL}/CIK{cik_padded}.json"
    resp = _sec_get(submissions_url)
    if resp is None:
        return None

    try:
        data = resp.json()
    except Exception:
        return None

    # The recent filings are in data["filings"]["recent"]
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    if not forms:
        return None

    # Find the best 10-K or 10-K/A for the target year
    target_start = f"{year}-01-01"
    target_end = f"{year + 1}-06-30"

    best_idx = None
    for i, form in enumerate(forms):
        if form not in ("10-K", "10-K/A"):
            continue
        if i >= len(dates) or i >= len(accessions) or i >= len(primary_docs):
            continue
        filing_date = dates[i]
        if target_start <= filing_date <= target_end:
            # Prefer 10-K over 10-K/A, and most recent within range
            if best_idx is None:
                best_idx = i
            elif forms[best_idx] == "10-K/A" and form == "10-K":
                best_idx = i  # prefer original over amendment
            # First match within date range is most recent (SEC sorts desc)
            break

    if best_idx is None:
        return None

    # Build the filing document URL
    accession_raw = accessions[best_idx]
    accession_nodash = accession_raw.replace("-", "")
    primary_doc = primary_docs[best_idx]
    doc_url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik_padded)}/{accession_nodash}/{primary_doc}"
    )

    doc_resp = _sec_get(doc_url)
    if doc_resp is None:
        return None

    # Return first 100KB of text
    text = doc_resp.text[:_10K_TEXT_MAX_BYTES]
    logger.debug("Fetched 10-K for %s (%s): %d chars", ticker, year, len(text))
    return text


# ── EFTS helper (global search, CIK-based filtering) ──────────────────────────

@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _efts_search(
    query: str,
    forms: str = "10-K",
    start_date: str | None = None,
    end_date: str | None = None,
    entity: str | None = None,
    start_from: int = 0,
) -> dict | None:
    """Rate-limited EFTS query. Always searches globally (entity param is ignored).

    The ``entity`` parameter is accepted for backward-compat signatures but is
    never injected into the query string because EFTS has no native per-company
    filter.  Caller-side CIK mapping handles per-ticker attribution.
    """
    if start_date is None:
        start_date = (date.today() - timedelta(days=400)).isoformat()
    if end_date is None:
        end_date = date.today().isoformat()

    params = {
        "q": f'"{query}"',
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "forms": forms,
        "from": start_from,
    }
    # NOTE: entity is intentionally NOT added to params["q"].  The old code
    # did `params["q"] = f'"{query}" "{entity}"'` which was the bug — short
    # tickers like "FLY" or "MASS" are common English words and match
    # everything.  CIK-based post-filtering is the correct approach.

    for attempt in range(4):
        try:
            resp = requests.get(
                EDGAR_FULL_TEXT_SEARCH_URL,
                params=params,
                headers=_headers,
                timeout=EDGAR_REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < 3:
                time.sleep(1)
            else:
                logger.error("EFTS failed for %r: %s", query, e)
    return None


def _resolve_ticker_from_hit(src: dict) -> str | None:
    """Resolve a ticker symbol from a single EFTS hit _source dict.

    Uses display_names first (fast), CIK fallback second.  Returns None if
    the hit cannot be attributed to a known ticker.
    """
    # Try display_names first — e.g. "Rocket Lab USA Inc  (RKLB)  (CIK ...)"
    for dn in src.get("display_names", []):
        m = _TICKER_FROM_DN_RE.search(dn)
        if m:
            return m.group(1)

    # CIK fallback
    for cik in src.get("ciks", []):
        t = _cik_to_ticker_lookup(cik)
        if t:
            return t

    return None


_EFTS_MAX_PAGES = 10
_EFTS_HITS_PER_PAGE = 100


def _global_efts_search_mapped(
    keywords: tuple | list,
    forms: str,
    start_date: str,
    end_date: str,
    target_tickers: set[str] | None = None,
    max_pages: int = _EFTS_MAX_PAGES,
) -> dict[str, dict[str, list[str]]]:
    """Search EFTS globally for each keyword, map hits to tickers via CIK.

    This is the core fix: instead of one EFTS call per (keyword, ticker) pair
    which embeds the ticker in the query string, we search once per keyword
    globally and post-filter by CIK/display_name.

    Args:
        keywords: Iterable of keyword strings to search.
        forms: Filing types, e.g. "10-K" or "8-K".
        start_date: ISO date string.
        end_date: ISO date string.
        target_tickers: If provided, only retain hits for these tickers.
            If None, retain all resolved tickers.
        max_pages: Max EFTS result pages to fetch per keyword.

    Returns:
        Dict of ``{ticker: {keyword: [text_snippets]}}``.
        Each text_snippet is the keyword itself (proving it appeared in the
        filing).  EFTS ``file_description`` is just metadata ("Annual Report
        (10-K)") and never contains the matched text.  A non-empty list means
        the keyword was found in at least one filing for that ticker.
    """
    # Ensure CIK maps are loaded
    _load_cik_maps()

    # ticker -> keyword -> [texts]
    result: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    for kw in keywords:
        page = 0
        while page < max_pages:
            data = _efts_search(
                kw, forms=forms,
                start_date=start_date, end_date=end_date,
                start_from=page * _EFTS_HITS_PER_PAGE,
            )
            if not data:
                break

            raw_hits = data.get("hits", {}).get("hits", [])
            if not raw_hits:
                break

            for hit in raw_hits:
                src = hit.get("_source", {})
                ticker = _resolve_ticker_from_hit(src)
                if not ticker:
                    continue
                # Skip tickers with digits (warrants like AMPXW) or single-char
                if len(ticker) < 2 or any(c.isdigit() for c in ticker):
                    continue
                if target_tickers is not None and ticker not in target_tickers:
                    continue

                # Store the keyword itself as the snippet.  The EFTS hit proves
                # this keyword appeared in the filing text, even though
                # file_description is just metadata.
                result[ticker][kw].append(kw)

            total_hits = data.get("hits", {}).get("total", {}).get("value", 0)
            fetched = (page + 1) * _EFTS_HITS_PER_PAGE
            if fetched >= total_hits:
                break
            page += 1

    return dict(result)


# ── Partnership mismatch helpers ────────────────────────────────────────────

_FORTUNE_200_SET = set(FORWARD_PARTNERSHIP_FORTUNE_200)

_TICKER_TO_NAME: dict[str, str] = {
    "AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "Nvidia", "AMZN": "Amazon",
    "GOOGL": "Google", "META": "Meta", "TSLA": "Tesla", "WMT": "Walmart",
    "JPM": "JPMorgan", "V": "Visa", "MA": "Mastercard", "JNJ": "Johnson & Johnson",
    "PG": "Procter & Gamble", "XOM": "ExxonMobil", "CVX": "Chevron",
    "BA": "Boeing", "LMT": "Lockheed Martin", "RTX": "Raytheon",
    "NOC": "Northrop Grumman", "GD": "General Dynamics", "GE": "General Electric",
    "HON": "Honeywell", "CAT": "Caterpillar", "DE": "Deere",
    "IBM": "IBM", "ORCL": "Oracle", "CRM": "Salesforce", "CSCO": "Cisco",
    "INTC": "Intel", "AMD": "AMD", "QCOM": "Qualcomm", "T": "AT&T",
    "VZ": "Verizon", "TMUS": "T-Mobile", "VOD": "Vodafone",
    "DIS": "Disney", "F": "Ford", "GM": "General Motors",
}

_NAME_TO_TICKER: dict[str, str] = {v.lower(): k for k, v in _TICKER_TO_NAME.items()}

_PARTNERSHIP_KW_RE = re.compile(
    "|".join(re.escape(kw) for kw in FORWARD_PARTNERSHIP_KEYWORDS),
    re.IGNORECASE,
)


_PARTNERSHIP_NEGATIVE_KW_RE = re.compile(
    r"\b(?:competitor|risk|compared to|unlike|versus)\b", re.IGNORECASE,
)


def _detect_partnerships(text: str) -> list[str]:
    """Find Fortune 200 company names mentioned near partnership keywords.

    Tighter matching rules to reduce false positives:
    1. Window reduced to 100 chars AFTER the keyword (keyword must appear before
       the company name — natural language order).
    2. Negative keywords between keyword and company name disqualify the match
       ("competitor", "risk", "compared to", "unlike", "versus").

    Returns list of partner tickers (deduplicated).
    """
    found: set[str] = set()
    for m in _PARTNERSHIP_KW_RE.finditer(text):
        # Only look forward (keyword before company name), 100-char window
        end = min(len(text), m.end() + 100)
        window = text[m.end():end]

        # If negative keywords appear in the window, skip this match entirely
        if _PARTNERSHIP_NEGATIVE_KW_RE.search(window):
            continue

        # Check company names
        for name, ticker in _NAME_TO_TICKER.items():
            if re.search(r"\b" + re.escape(name) + r"\b", window, re.IGNORECASE):
                found.add(ticker)
        # Check tickers directly (e.g. "partnership with NVDA")
        for ticker in _FORTUNE_200_SET:
            if re.search(r"\b" + re.escape(ticker) + r"\b", window):
                found.add(ticker)
    return sorted(found)


def score_partnership_mismatch(
    partner_tickers: list[str],
    ticker_market_cap: float,
    partner_market_caps: dict[str, float],
) -> tuple[float, dict]:
    """Score partnership quality mismatch (0-5 points).

    Higher scores when small companies partner with much larger Fortune companies.
    """
    meta = {
        "partner_tickers": ",".join(partner_tickers) if partner_tickers else "",
        "partner_count": len(partner_tickers),
        "max_ratio": 0,
    }

    if not partner_tickers or ticker_market_cap <= 0:
        return 0, meta

    # Fortune 100 = first 100 in the tuple
    fortune_100 = set(FORWARD_PARTNERSHIP_FORTUNE_200[:100])
    fortune_500 = _FORTUNE_200_SET  # Our list approximates Fortune 500

    ratios = []
    for pt in partner_tickers:
        pcap = partner_market_caps.get(pt, 0)
        if pcap > 0:
            ratios.append(pcap / ticker_market_cap)

    max_ratio = max(ratios) if ratios else 0
    meta["max_ratio"] = round(max_ratio, 1)

    has_fortune_100 = any(t in fortune_100 for t in partner_tickers)
    fortune_500_count = sum(1 for t in partner_tickers if t in fortune_500)

    # Tier 1: Fortune 100 invested + 2+ partners + ratio 100x+
    if has_fortune_100 and len(partner_tickers) >= 2 and max_ratio >= 100:
        return 5, meta

    # Tier 2: 2+ Fortune 500 + ratio 50x+
    if fortune_500_count >= 2 and max_ratio >= 50:
        return 4, meta

    # Tier 3: 1 Fortune 500 + ratio 10x+ + ticker < $2B
    if fortune_500_count >= 1 and max_ratio >= 10 and ticker_market_cap < 2e9:
        return 3, meta

    # Tier 4: Ratio 10x+
    if max_ratio >= 10:
        return 2, meta

    # Tier 5: Any Fortune partner
    if fortune_500_count >= 1:
        return 1, meta

    return 0, meta


# ── Segment revenue crossover ──────────────────────────────────────────────

def score_segment_crossover(
    primary_revenue: float | None,
    primary_growth: float | None,
    secondary_revenue: float | None,
    secondary_growth: float | None,
) -> tuple[float, dict]:
    """Score segment revenue crossover signal (0-5 points).

    Detects when a secondary business segment is rapidly growing
    toward overtaking the primary segment.
    """
    meta = {
        "primary_revenue": primary_revenue,
        "secondary_revenue": secondary_revenue,
        "primary_growth": primary_growth,
        "secondary_growth": secondary_growth,
        "secondary_pct_of_primary": None,
    }

    if (
        primary_revenue is None
        or secondary_revenue is None
        or primary_revenue <= 0
        or secondary_revenue <= 0
        or secondary_growth is None
    ):
        return 0, meta

    ratio = secondary_revenue / primary_revenue
    meta["secondary_pct_of_primary"] = round(ratio, 4)

    # Secondary grew 50%+ AND within 70% of primary size
    if secondary_growth >= 0.50 and ratio >= 0.70:
        return 5, meta

    # Secondary grew 50%+ AND within 50% of primary
    if secondary_growth >= 0.50 and ratio >= 0.50:
        return 4, meta

    # Secondary grew 30%+ AND within 50%
    if secondary_growth >= 0.30 and ratio >= 0.50:
        return 3, meta

    # Secondary grew 50%+ but small
    if secondary_growth >= 0.50:
        return 1, meta

    return 0, meta


# ── New TAM language ───────────────────────────────────────────────────────

def score_new_tam_language(
    current_keywords: set[str],
    prior_keywords: set[str],
) -> tuple[float, dict]:
    """Score new TAM language signal (0-3 points).

    Detects when a company introduces new market/technology keywords
    in its filings that weren't present in prior filings.
    """
    new_kw = current_keywords - prior_keywords
    meta = {
        "new_tam_keywords": ",".join(sorted(new_kw)) if new_kw else "",
        "new_count": len(new_kw),
    }

    if len(new_kw) >= 3:
        return 3, meta
    if len(new_kw) == 2:
        return 2, meta
    if len(new_kw) == 1:
        return 1, meta
    return 0, meta


# ── Capex / R&D inflection ─────────────────────────────────────────────────

def score_capex_inflection(
    capex_current: float | None,
    capex_prior: float | None,
    rd_current: float | None,
    rd_prior: float | None,
    pct_from_ath: float | None,
) -> tuple[float, dict]:
    """Score capex/R&D inflection signal (0-2 points).

    Only scores if the stock is beaten down (pct_from_ath <= -0.50).
    Takes best of capex growth or R&D growth.
    """
    meta = {"capex_growth_pct": None, "rd_growth_pct": None}

    if pct_from_ath is None or pct_from_ath > FORWARD_CAPEX_ATH_CRASH_MIN:
        return 0, meta

    # Compute capex growth (use abs since capex is often negative in cash flow)
    capex_growth = None
    if capex_current is not None and capex_prior is not None and abs(capex_prior) > 0:
        capex_growth = (abs(capex_current) - abs(capex_prior)) / abs(capex_prior)
        meta["capex_growth_pct"] = round(capex_growth, 4)

    # Compute R&D growth
    rd_growth = None
    if rd_current is not None and rd_prior is not None and rd_prior > 0:
        rd_growth = (rd_current - rd_prior) / rd_prior
        meta["rd_growth_pct"] = round(rd_growth, 4)

    best_growth = max(
        capex_growth if capex_growth is not None else -999,
        rd_growth if rd_growth is not None else -999,
    )

    if best_growth <= -999:
        return 0, meta  # No data

    # 50%+ growth AND 60%+ below ATH
    if best_growth >= 0.50 and pct_from_ath <= -0.60:
        return 2, meta

    # 30%+ growth AND 50%+ below ATH
    if best_growth >= FORWARD_CAPEX_GROWTH_MIN and pct_from_ath <= FORWARD_CAPEX_ATH_CRASH_MIN:
        return 1, meta

    return 0, meta


# ── Technology milestone ───────────────────────────────────────────────────

_MILESTONE_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in FORWARD_MILESTONE_KEYWORDS
]


def score_tech_milestone(
    filing_texts: list[str], sector: str | None = None
) -> tuple[float, dict]:
    """Score technology milestone signal (0-2 points).

    Searches filing texts for milestone keywords.  When *sector* indicates
    biotech/pharma/healthcare AND the only hits are from the noise set
    (``FORWARD_MILESTONE_BIOTECH_NOISE``), score 0 — those milestones are
    table-stakes in those sectors, not differentiated signals.
    """
    combined = " ".join(filing_texts)
    found: set[str] = set()
    for i, pattern in enumerate(_MILESTONE_PATTERNS):
        if pattern.search(combined):
            found.add(FORWARD_MILESTONE_KEYWORDS[i])

    meta = {"milestone_keywords": ",".join(sorted(found)) if found else ""}

    if not found:
        return 0, meta

    # Sector-aware noise reduction: if the ticker is biotech/pharma and
    # every milestone hit is noise, score 0.
    if sector and sector in FORWARD_MILESTONE_BIOTECH_SECTORS:
        non_noise = found - FORWARD_MILESTONE_BIOTECH_NOISE
        if not non_noise:
            return 0, meta

    if len(found) >= 2:
        return 2, meta
    if len(found) == 1:
        return 1, meta
    return 0, meta


# ── Task 6: Score composition + DB writer ─────────────────────────────────────

def compute_forward_score(
    sig_backlog: float,
    sig_segment: float,
    sig_partnership: float,
    sig_tam: float,
    sig_capex: float,
    sig_milestone: float,
) -> float:
    """Sum all 6 forward signals, capped at 25."""
    return min(sig_backlog + sig_segment + sig_partnership + sig_tam + sig_capex + sig_milestone, 25.0)


def write_forward_scores(rows: list[dict]) -> int:
    """INSERT OR REPLACE rows into scr_forward_moat_scores. Returns row count."""
    if not rows:
        return 0
    with get_connection() as conn:
        conn.executemany(
            f"""INSERT OR REPLACE INTO {TABLE_FORWARD_MOAT_SCORES}
                (ticker, scan_date, sig_backlog, sig_segment_crossover,
                 sig_partnership_mismatch, sig_new_tam, sig_capex_inflection,
                 sig_tech_milestone, forward_score, backlog_current, backlog_prior,
                 backlog_growth_pct, partnership_names, partnership_verified,
                 new_tam_keywords, capex_growth_pct, rd_growth_pct,
                 milestone_keywords, forward_verdict, forward_verdict_reason)
                VALUES (:ticker, :scan_date, :sig_backlog, :sig_segment_crossover,
                        :sig_partnership_mismatch, :sig_new_tam, :sig_capex_inflection,
                        :sig_tech_milestone, :forward_score, :backlog_current, :backlog_prior,
                        :backlog_growth_pct, :partnership_names, :partnership_verified,
                        :new_tam_keywords, :capex_growth_pct, :rd_growth_pct,
                        :milestone_keywords, :forward_verdict, :forward_verdict_reason)""",
            rows,
        )
        conn.commit()
    return len(rows)


# ── Task 7: Per-ticker signal fetchers (CIK-mapped batch mode) ───────────────
#
# These functions now accept pre-fetched EFTS data (mapped by ticker via CIK)
# instead of doing per-ticker EFTS calls.  The orchestrator calls
# _global_efts_search_mapped() once per keyword set and passes the results here.

def _fetch_backlog_signal(
    ticker: str,
    scan_date: str,
    current_hits: dict[str, list[str]] | None = None,
    prior_hits: dict[str, list[str]] | None = None,
) -> tuple[float, dict]:
    """Score backlog acceleration from real 10-K text extraction.

    When EFTS confirms backlog keywords exist for this ticker, downloads
    actual 10-K filing text and extracts dollar amounts with regex.
    Falls back to yfinance revenue proxy if 10-K text fetch fails.

    Args:
        ticker: Ticker symbol.
        scan_date: ISO date string.
        current_hits: Pre-fetched ``{keyword: [texts]}`` for current period.
        prior_hits: Pre-fetched ``{keyword: [texts]}`` for prior period.
    """
    _zero_meta = {"backlog_current": None, "backlog_prior": None, "backlog_growth_pct": None}
    has_current = bool(current_hits and any(current_hits.values()))

    if not has_current:
        return 0, _zero_meta

    # EFTS confirmed backlog language -- try real 10-K text extraction first
    _load_cik_maps()
    from .filing_scanner import _ticker_to_cik as _ttc
    cik = _ttc.get(ticker)

    current_amount = None
    prior_amount = None
    used_10k_text = False

    if cik:
        today = date.fromisoformat(scan_date)
        current_year = today.year if today.month >= 7 else today.year - 1
        prior_year = current_year - 1

        current_text = _fetch_10k_text(ticker, cik, current_year)
        if current_text:
            current_amount = _extract_backlog_amount(current_text)

        # Only fetch prior year if current had a backlog dollar amount
        if current_amount is not None:
            prior_text = _fetch_10k_text(ticker, cik, prior_year)
            if prior_text:
                prior_amount = _extract_backlog_amount(prior_text)
            used_10k_text = True
            logger.debug(
                "10-K backlog for %s: current=%s prior=%s",
                ticker, current_amount, prior_amount,
            )

    # Fallback: if 10-K text fetch failed or extracted nothing, use yfinance
    # revenue as a proxy (old behavior) so we don't lose data.
    if not used_10k_text:
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            inc = t.income_stmt
            if inc is not None and not inc.empty:
                cols = inc.columns.tolist()
                for label in ["Total Revenue", "TotalRevenue", "Revenue", "totalRevenue"]:
                    if label in inc.index:
                        val = inc.loc[label].iloc[0]
                        if val is not None and val > 0:
                            current_amount = float(val)
                        if len(cols) >= 2:
                            val2 = inc.loc[label].iloc[1]
                            if val2 is not None and val2 > 0:
                                prior_amount = float(val2)
                        break
        except Exception as e:
            logger.debug("yfinance revenue fallback failed for %s: %s", ticker, e)

    return score_backlog(current_amount, prior_amount)


def _fetch_partnership_signal(
    ticker: str,
    ticker_market_cap: float,
    scan_date: str,
    partnership_hits: dict[str, list[str]] | None = None,
) -> tuple[float, dict]:
    """Score partnership mismatch from pre-fetched EFTS data + yfinance fallback.

    Scoring strategy:
    1. If EFTS found partnership keywords mapped to this ticker's 10-K, use
       yfinance summary to identify partner names and score up to 5 (full).
    2. If EFTS found nothing, fall back to yfinance summary alone but cap the
       score at 2 (summary-only mentions are unreliable).
    3. Always exclude institutional holders from FORWARD_PARTNERSHIP_EXCLUDE_INSTITUTIONAL.

    Args:
        ticker: Ticker symbol.
        ticker_market_cap: Target company market cap.
        scan_date: ISO date string.
        partnership_hits: Pre-fetched ``{keyword: [texts]}`` for partnership keywords.
    """
    has_efts_partnership = bool(
        partnership_hits and any(partnership_hits.values())
    )

    # Collect partners from yfinance summary (used by both EFTS and fallback paths)
    all_partners: set[str] = set()
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        summary = info.get("longBusinessSummary", "") or ""
        if summary:
            # Use _detect_partnerships for tighter matching (keyword-before-name,
            # negative keyword filtering, 100-char window)
            detected = _detect_partnerships(summary)
            all_partners.update(detected)

        # Check institutional holders — but only add non-excluded ones
        try:
            holders = yf.Ticker(ticker).institutional_holders
            if holders is not None and not holders.empty:
                holder_col = "Holder" if "Holder" in holders.columns else (
                    holders.columns[0] if len(holders.columns) > 0 else None
                )
                if holder_col:
                    for _, row in holders.iterrows():
                        holder_name = str(row[holder_col]).lower()
                        for name, partner_ticker in _NAME_TO_TICKER.items():
                            if name.lower() in holder_name:
                                all_partners.add(partner_ticker)
        except Exception:
            pass
    except Exception as e:
        logger.debug("yfinance partnership lookup failed for %s: %s", ticker, e)

    # Remove self-reference
    all_partners.discard(ticker)

    # Remove institutional holders (JPM, GS, BLK, etc.) — they hold everything
    all_partners -= FORWARD_PARTNERSHIP_EXCLUDE_INSTITUTIONAL

    if not all_partners:
        return 0, {"partner_tickers": "", "partner_count": 0, "max_ratio": 0}

    # Get partner market caps via yfinance
    partner_caps = {}
    try:
        import yfinance as yf
        for pt in all_partners:
            try:
                info = yf.Ticker(pt).fast_info
                mcap = getattr(info, "market_cap", None) or 0
                if mcap > 0:
                    partner_caps[pt] = mcap
            except Exception:
                pass
    except ImportError:
        logger.warning("yfinance not available for partnership market cap lookup")

    score, meta = score_partnership_mismatch(
        sorted(all_partners), ticker_market_cap, partner_caps,
    )

    # Cap at 2 if partnership was detected only from yfinance summary (no EFTS)
    if not has_efts_partnership and score > 2:
        score = 2
        meta["capped_yfinance_only"] = True

    return score, meta


def _fetch_capex_signal(ticker: str, pct_from_ath: float | None) -> tuple[float, dict]:
    """yfinance cash_flow + income_stmt for capex/R&D, score."""
    capex_current = capex_prior = rd_current = rd_prior = None
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)

        cf = t.cash_flow
        if cf is not None and not cf.empty:
            cols = cf.columns.tolist()
            if len(cols) >= 1:
                for label in ["Capital Expenditure", "CapitalExpenditure", "capitalExpenditure"]:
                    if label in cf.index:
                        capex_current = cf.loc[label].iloc[0]
                        if len(cols) >= 2:
                            capex_prior = cf.loc[label].iloc[1]
                        break

        inc = t.income_stmt
        if inc is not None and not inc.empty:
            cols = inc.columns.tolist()
            for label in ["Research Development", "ResearchAndDevelopment",
                          "Research And Development Expenses", "researchDevelopment"]:
                if label in inc.index:
                    rd_current = inc.loc[label].iloc[0]
                    if len(cols) >= 2:
                        rd_prior = inc.loc[label].iloc[1]
                    break
    except Exception as e:
        logger.debug("yfinance capex fetch failed for %s: %s", ticker, e)

    return score_capex_inflection(capex_current, capex_prior, rd_current, rd_prior, pct_from_ath)


def _fetch_segment_signal(ticker: str, scan_date: str) -> tuple[float, dict]:
    """Best-effort segment crossover via yfinance income statement.

    yfinance doesn't expose clean segment data, so we look for multiple
    revenue/sales line items in the income statement.  If at least two
    distinct revenue rows exist and both have two periods of data, we
    compare their growth rates and feed them into ``score_segment_crossover``.
    Returns (0, {}) when data is unavailable — same as the old stub.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        inc = t.income_stmt
        if inc is None or inc.empty:
            return 0, {}

        cols = inc.columns.tolist()
        if len(cols) < 2:
            return 0, {}

        # Find revenue-like rows
        revenue_rows = [
            idx for idx in inc.index
            if "revenue" in str(idx).lower() or "sales" in str(idx).lower()
        ]
        if len(revenue_rows) < 2:
            return 0, {}

        # Gather (current, prior) for each revenue row
        segments: list[tuple[float, float]] = []
        for row_label in revenue_rows:
            cur = inc.loc[row_label].iloc[0]
            pri = inc.loc[row_label].iloc[1]
            if cur is not None and pri is not None:
                try:
                    cur_f, pri_f = float(cur), float(pri)
                    if cur_f > 0 and pri_f > 0:
                        segments.append((cur_f, pri_f))
                except (ValueError, TypeError):
                    continue

        if len(segments) < 2:
            return 0, {}

        # Sort by current revenue descending — first = primary, second = secondary
        segments.sort(key=lambda s: s[0], reverse=True)
        pri_cur, pri_pri = segments[0]
        sec_cur, sec_pri = segments[1]

        pri_growth = (pri_cur - pri_pri) / pri_pri
        sec_growth = (sec_cur - sec_pri) / sec_pri

        return score_segment_crossover(pri_cur, pri_growth, sec_cur, sec_growth)
    except Exception as e:
        logger.debug("yfinance segment fetch failed for %s: %s", ticker, e)
    return 0, {}


def _fetch_tam_signal(
    ticker: str,
    scan_date: str,
    current_hits: dict[str, list[str]] | None = None,
    prior_hits: dict[str, list[str]] | None = None,
) -> tuple[float, dict]:
    """Score new TAM language from pre-fetched EFTS data.

    A keyword counts as "present" for a period if the ticker has any hits
    for that keyword in that period.
    """
    current_kw: set[str] = set()
    prior_kw: set[str] = set()

    if current_hits:
        for kw, texts in current_hits.items():
            if texts:
                current_kw.add(kw)

    if prior_hits:
        for kw, texts in prior_hits.items():
            if texts:
                prior_kw.add(kw)

    return score_new_tam_language(current_kw, prior_kw)


def _fetch_milestone_signal(
    ticker: str,
    scan_date: str,
    milestone_hits: dict[str, list[str]] | None = None,
    sector: str | None = None,
) -> tuple[float, dict]:
    """Score technology milestones from pre-fetched EFTS data.

    EFTS hits prove the milestone keyword appeared in an 8-K filing.  Since
    ``_global_efts_search_mapped`` now stores the keyword itself as the
    snippet, ``score_tech_milestone`` will match the keyword patterns
    directly against the keyword text.

    ``sector`` is forwarded to ``score_tech_milestone`` for biotech noise
    reduction.
    """
    texts: list[str] = []
    if milestone_hits:
        for kw, kw_texts in milestone_hits.items():
            texts.extend(kw_texts)

    return score_tech_milestone(texts, sector=sector)


# ── CSV writer ────────────────────────────────────────────────────────────────

def _write_csv(rows: list[dict], scan_date: str) -> Path:
    """Write forward moat scores to CSV."""
    output_dir = Path(FORWARD_MOAT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"forward-{scan_date}.csv"

    if not rows:
        csv_path.write_text("")
        return csv_path

    fieldnames = [
        "rank", "ticker", "forward_score", "sig_backlog", "sig_segment_crossover",
        "sig_partnership_mismatch", "sig_new_tam", "sig_capex_inflection",
        "sig_tech_milestone", "backlog_growth_pct", "partnership_names",
        "new_tam_keywords", "milestone_keywords", "forward_verdict",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for i, row in enumerate(rows, 1):
            row_copy = dict(row)
            row_copy["rank"] = i
            writer.writerow(row_copy)

    return csv_path


# ── Scan report ──────────────────────────────────────────────────────────────

def _log_scan_report(top_rows: list[dict], scan_date: str, csv_path: Path) -> None:
    """Format and log top forward scorers."""
    lines = [f"FORWARD MOAT SCAN — {scan_date}", f"Top {len(top_rows)} trajectory signals:\n"]
    for i, row in enumerate(top_rows, 1):
        parts = []
        if row.get("sig_backlog", 0) > 0:
            parts.append(f"BK={row['sig_backlog']:.0f}")
        if row.get("sig_partnership_mismatch", 0) > 0:
            parts.append(f"PR={row['sig_partnership_mismatch']:.0f}")
        if row.get("sig_new_tam", 0) > 0:
            parts.append(f"TAM={row['sig_new_tam']:.0f}")
        if row.get("sig_segment_crossover", 0) > 0:
            parts.append(f"SEG={row['sig_segment_crossover']:.0f}")
        if row.get("sig_capex_inflection", 0) > 0:
            parts.append(f"CX={row['sig_capex_inflection']:.0f}")
        if row.get("sig_tech_milestone", 0) > 0:
            parts.append(f"MS={row['sig_tech_milestone']:.0f}")
        signal_str = " ".join(parts) if parts else "none"
        lines.append(f"{i:>2}. {row['ticker']:<6} {row['forward_score']:>4.0f}  [{signal_str}]")

    lines.append(f"\nCSV: {csv_path.name}")
    report = "\n".join(lines)
    logger.info("Forward moat scan report:\n%s", report)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_forward_scan(dry_run: bool = False, scan_date: str | None = None) -> dict:
    """Run the forward moat scan on top discovery candidates.

    Uses batch global EFTS searches (one per keyword) with CIK-based mapping
    to attribute hits to individual tickers.  This replaces the old per-ticker
    EFTS queries that embedded the ticker symbol in the search string.

    Args:
        dry_run: If True, skip report send.
        scan_date: Override scan date (default today).

    Returns:
        Summary dict with scan_date, total_scored, csv_path, top_forward list.
    """
    run_migration()

    if scan_date is None:
        scan_date = date.today().isoformat()

    # Read top N from discovery flags (include sector for milestone noise reduction)
    with get_connection() as conn:
        candidates = conn.execute(
            f"""SELECT ticker, composite_score, company_name, sector
                FROM {TABLE_DISCOVERY_FLAGS}
                WHERE scan_date = (SELECT MAX(scan_date) FROM {TABLE_DISCOVERY_FLAGS})
                ORDER BY composite_score DESC
                LIMIT ?""",
            (FORWARD_MOAT_SCAN_TOP_N,),
        ).fetchall()

    if not candidates:
        logger.warning("No discovery candidates found — run discovery first")
        return {"scan_date": scan_date, "total_scored": 0, "csv_path": None, "top_forward": []}

    # ── Kill list exclusion ────────────────────────────────────────────────
    with get_connection() as conn:
        killed_tickers: set[str] = {
            r[0] for r in conn.execute(
                f"SELECT ticker FROM {TABLE_KILL_LIST}"
            ).fetchall()
        }
        deepdive_kills: set[str] = {
            r[0] for r in conn.execute(
                f"SELECT ticker FROM {TABLE_DEEP_DIVE_RESULTS} WHERE verdict='KILL'"
            ).fetchall()
        }
    all_kills = killed_tickers | deepdive_kills

    pre_kill = len(candidates)
    candidates = [c for c in candidates if c["ticker"] not in all_kills]
    if pre_kill != len(candidates):
        logger.info("Kill list excluded %d tickers (%d kill list + %d deep-dive kills)",
                     pre_kill - len(candidates), len(killed_tickers), len(deepdive_kills))

    # ── Get price metrics + market cap for capex/partnership signals ───────
    # Load CIK maps once for the entire scan
    _load_cik_maps()

    with get_connection() as conn:
        price_rows = conn.execute(
            """SELECT p.ticker, p.pct_from_ath, p.current_price,
                      f.market_cap
               FROM scr_price_metrics p
               LEFT JOIN scr_fundamentals f ON f.ticker = p.ticker
                 AND f.date = (SELECT MAX(f2.date) FROM scr_fundamentals f2 WHERE f2.ticker = f.ticker)"""
        ).fetchall()
    price_map = {r["ticker"]: dict(r) for r in price_rows}

    # ── Market cap ceiling — filter out large-caps ────────────────────────
    pre_mcap = len(candidates)
    candidates = [
        c for c in candidates
        if (price_map.get(c["ticker"], {}).get("market_cap") or 0) <= FORWARD_MOAT_MAX_MARKET_CAP
    ]
    if pre_mcap != len(candidates):
        logger.info("Market cap ceiling ($%dB) excluded %d tickers",
                     FORWARD_MOAT_MAX_MARKET_CAP // 1_000_000_000,
                     pre_mcap - len(candidates))

    # Build sector lookup from discovery flags
    sector_map: dict[str, str] = {
        c["ticker"]: (c["sector"] or "unknown") for c in candidates
    }

    ticker_set = {c["ticker"] for c in candidates}
    logger.info("Forward scan starting: %d candidates after filters", len(candidates))

    # ── Batch global EFTS searches ──────────────────────────────────────────
    today = date.fromisoformat(scan_date)
    current_start = (today - timedelta(days=400)).isoformat()
    current_end = scan_date
    prior_start = (today - timedelta(days=800)).isoformat()
    prior_end = (today - timedelta(days=400)).isoformat()
    milestone_start = (today - timedelta(days=180)).isoformat()

    logger.info("Batch EFTS: backlog keywords (current period)")
    backlog_current = _global_efts_search_mapped(
        FORWARD_BACKLOG_KEYWORDS, forms="10-K",
        start_date=current_start, end_date=current_end,
        target_tickers=ticker_set,
    )
    logger.info("Batch EFTS: backlog keywords (prior period)")
    backlog_prior = _global_efts_search_mapped(
        FORWARD_BACKLOG_KEYWORDS, forms="10-K",
        start_date=prior_start, end_date=prior_end,
        target_tickers=ticker_set,
    )

    logger.info("Batch EFTS: partnership keywords")
    partnership_map = _global_efts_search_mapped(
        FORWARD_PARTNERSHIP_KEYWORDS, forms="10-K",
        start_date=current_start, end_date=current_end,
        target_tickers=ticker_set,
    )

    logger.info("Batch EFTS: TAM keywords (current period)")
    tam_current = _global_efts_search_mapped(
        FORWARD_TAM_KEYWORDS, forms="10-K",
        start_date=current_start, end_date=current_end,
        target_tickers=ticker_set,
    )
    logger.info("Batch EFTS: TAM keywords (prior period)")
    tam_prior = _global_efts_search_mapped(
        FORWARD_TAM_KEYWORDS, forms="10-K",
        start_date=prior_start, end_date=prior_end,
        target_tickers=ticker_set,
    )

    logger.info("Batch EFTS: milestone keywords (8-K)")
    milestone_map = _global_efts_search_mapped(
        FORWARD_MILESTONE_KEYWORDS, forms="8-K",
        start_date=milestone_start, end_date=current_end,
        target_tickers=ticker_set,
    )

    logger.info("Batch EFTS complete — scoring %d tickers", len(candidates))

    # ── Per-ticker scoring ──────────────────────────────────────────────────
    scored_rows: list[dict] = []

    for cand in candidates:
        ticker = cand["ticker"]
        pm = price_map.get(ticker, {})
        pct_from_ath = pm.get("pct_from_ath")
        market_cap = pm.get("market_cap") or 0

        try:
            sig_bk, meta_bk = _fetch_backlog_signal(
                ticker, scan_date,
                current_hits=backlog_current.get(ticker),
                prior_hits=backlog_prior.get(ticker),
            )
            sig_pr, meta_pr = _fetch_partnership_signal(
                ticker, market_cap, scan_date,
                partnership_hits=partnership_map.get(ticker),
            )
            sig_cx, meta_cx = _fetch_capex_signal(ticker, pct_from_ath)
            sig_seg, meta_seg = _fetch_segment_signal(ticker, scan_date)
            sig_tam, meta_tam = _fetch_tam_signal(
                ticker, scan_date,
                current_hits=tam_current.get(ticker),
                prior_hits=tam_prior.get(ticker),
            )
            sig_ms, meta_ms = _fetch_milestone_signal(
                ticker, scan_date,
                milestone_hits=milestone_map.get(ticker),
                sector=sector_map.get(ticker),
            )
        except Exception as e:
            logger.error("Error scoring %s: %s", ticker, e)
            continue

        total = compute_forward_score(sig_bk, sig_seg, sig_pr, sig_tam, sig_cx, sig_ms)

        row = {
            "ticker": ticker,
            "scan_date": scan_date,
            "sig_backlog": sig_bk,
            "sig_segment_crossover": sig_seg,
            "sig_partnership_mismatch": sig_pr,
            "sig_new_tam": sig_tam,
            "sig_capex_inflection": sig_cx,
            "sig_tech_milestone": sig_ms,
            "forward_score": total,
            "backlog_current": str(meta_bk.get("backlog_current")) if meta_bk.get("backlog_current") else None,
            "backlog_prior": str(meta_bk.get("backlog_prior")) if meta_bk.get("backlog_prior") else None,
            "backlog_growth_pct": meta_bk.get("backlog_growth_pct"),
            "partnership_names": meta_pr.get("partner_tickers", ""),
            "partnership_verified": 1 if meta_pr.get("partner_count", 0) > 0 else 0,
            "new_tam_keywords": meta_tam.get("new_tam_keywords", ""),
            "capex_growth_pct": meta_cx.get("capex_growth_pct"),
            "rd_growth_pct": meta_cx.get("rd_growth_pct"),
            "milestone_keywords": meta_ms.get("milestone_keywords", ""),
            "forward_verdict": None,
            "forward_verdict_reason": None,
        }
        scored_rows.append(row)

    # Sort by forward_score DESC
    scored_rows.sort(key=lambda r: r["forward_score"], reverse=True)

    # Write to DB
    if scored_rows:
        write_forward_scores(scored_rows)

    # Write CSV
    csv_path = _write_csv(scored_rows, scan_date)

    # Log scan report
    top_n = scored_rows[:10]
    if not dry_run and top_n:
        _log_scan_report(top_n, scan_date, csv_path)

    summary = {
        "scan_date": scan_date,
        "total_scored": len(scored_rows),
        "csv_path": str(csv_path),
        "top_forward": [
            {"ticker": r["ticker"], "forward_score": r["forward_score"]}
            for r in top_n
        ],
    }
    logger.info("Forward scan complete: %d scored, CSV at %s", len(scored_rows), csv_path)
    return summary


# ── Task 8: Combined CSV merge ───────────────────────────────────────────────

def merge_combined_csv(
    moat_rows: list[dict],
    forward_rows: list[dict],
    output_path: Path | None = None,
) -> Path:
    """Merge moat + forward scores into a combined ranked CSV.

    Args:
        moat_rows: dicts with 'ticker', 'composite_score', 'rank'.
        forward_rows: dicts with 'ticker', 'forward_score', 'rank'.
        output_path: Override output file path.

    Returns:
        Path to the combined CSV.
    """
    scan_date = date.today().isoformat()
    if output_path is None:
        output_dir = Path(FORWARD_MOAT_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"combined-{scan_date}.csv"

    # Index by ticker
    moat_by_ticker = {r["ticker"]: r for r in moat_rows}
    forward_by_ticker = {r["ticker"]: r for r in forward_rows}

    all_tickers = set(moat_by_ticker.keys()) | set(forward_by_ticker.keys())
    combined = []

    for ticker in all_tickers:
        moat = moat_by_ticker.get(ticker, {})
        fwd = forward_by_ticker.get(ticker, {})
        moat_rank = moat.get("rank", len(moat_rows) + 1)
        forward_rank = fwd.get("rank", len(forward_rows) + 1)
        moat_score = moat.get("composite_score", 0)
        forward_score = fwd.get("forward_score", 0)

        # Convergence reward: forward_rank weighted 1.5x when moat_score > 20
        weight = 1.5 if moat_score > 20 else 1.0
        combined_score = moat_rank + forward_rank * weight

        combined.append({
            "ticker": ticker,
            "moat_rank": moat_rank,
            "moat_score": moat_score,
            "forward_rank": forward_rank,
            "forward_score": forward_score,
            "combined_score": round(combined_score, 1),
        })

    # Sort by combined_score ASC (lower = better)
    combined.sort(key=lambda r: r["combined_score"])

    # Assign combined_rank
    for i, row in enumerate(combined, 1):
        row["combined_rank"] = i

    fieldnames = ["combined_rank", "ticker", "combined_score", "moat_rank", "moat_score",
                  "forward_rank", "forward_score"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(combined)

    return output_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point for the forward moat scanner."""
    parser = argparse.ArgumentParser(
        description="Forward Moat Scanner — trajectory signals for companies building moats"
    )
    parser.add_argument("--run", action="store_true", help="Full scan with report logging")
    parser.add_argument("--dry-run", action="store_true", help="Full scan without report logging")
    parser.add_argument("--ticker", type=str, help="Show forward moat profile for one ticker")
    parser.add_argument("--latest", action="store_true", help="Show latest forward scores")
    parser.add_argument("--top", type=int, default=30, help="Number of results (default 30)")
    parser.add_argument("--history", type=str, metavar="TICKER", help="Show forward score history")

    args = parser.parse_args()

    if args.run:
        summary = run_forward_scan(dry_run=False)
        csv_name = Path(summary["csv_path"]).name if summary.get("csv_path") else ""
        print(f"Scan complete: {summary['total_scored']} scored, CSV: {csv_name}")
    elif args.dry_run:
        summary = run_forward_scan(dry_run=True)
        csv_name = Path(summary["csv_path"]).name if summary.get("csv_path") else ""
        print(f"\nScan complete: {summary['total_scored']} scored, CSV: {csv_name}")
    elif args.latest:
        run_migration()
        with get_connection() as conn:
            rows = conn.execute(
                f"""SELECT ticker, forward_score, sig_backlog, sig_segment_crossover,
                           sig_partnership_mismatch, sig_new_tam, sig_capex_inflection,
                           sig_tech_milestone, forward_verdict
                    FROM {TABLE_FORWARD_MOAT_SCORES}
                    WHERE scan_date = (SELECT MAX(scan_date) FROM {TABLE_FORWARD_MOAT_SCORES})
                    ORDER BY forward_score DESC
                    LIMIT ?""",
                (args.top,),
            ).fetchall()
        if not rows:
            print("No forward moat scores found.")
            return
        print(f"Latest forward moat scores (top {len(rows)}):\n")
        for i, r in enumerate(rows, 1):
            parts = []
            if r["sig_backlog"]:
                parts.append(f"BK={r['sig_backlog']:.0f}")
            if r["sig_partnership_mismatch"]:
                parts.append(f"PR={r['sig_partnership_mismatch']:.0f}")
            if r["sig_new_tam"]:
                parts.append(f"TAM={r['sig_new_tam']:.0f}")
            if r["sig_segment_crossover"]:
                parts.append(f"SEG={r['sig_segment_crossover']:.0f}")
            if r["sig_capex_inflection"]:
                parts.append(f"CX={r['sig_capex_inflection']:.0f}")
            if r["sig_tech_milestone"]:
                parts.append(f"MS={r['sig_tech_milestone']:.0f}")
            signal_str = " ".join(parts) if parts else "none"
            print(f"  {i:>3}. {r['ticker']:<6} | Score: {r['forward_score']:>4.0f} | [{signal_str}]")
    elif args.history:
        run_migration()
        ticker = args.history.upper()
        with get_connection() as conn:
            rows = conn.execute(
                f"""SELECT scan_date, forward_score, sig_backlog, sig_partnership_mismatch,
                           sig_new_tam, sig_capex_inflection, sig_tech_milestone
                    FROM {TABLE_FORWARD_MOAT_SCORES}
                    WHERE ticker = ?
                    ORDER BY scan_date ASC""",
                (ticker,),
            ).fetchall()
        if not rows:
            print(f"No forward score history for {ticker}")
            return
        print(f"Forward score history for {ticker}:")
        prev_score = None
        for r in rows:
            score = r["forward_score"] or 0
            delta_str = ""
            if prev_score is not None:
                delta = score - prev_score
                delta_str = f" ({delta:+.1f})"
            print(f"  {r['scan_date']}: {score:.1f}{delta_str}")
            prev_score = score
    elif args.ticker:
        run_migration()
        ticker = args.ticker.upper()
        with get_connection() as conn:
            row = conn.execute(
                f"""SELECT * FROM {TABLE_FORWARD_MOAT_SCORES}
                    WHERE ticker = ?
                    ORDER BY scan_date DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        if not row:
            print(f"No forward moat data for {ticker}")
            return
        print(f"Forward moat profile for {ticker} (scan {row['scan_date']}):")
        print(f"  Forward Score:   {row['forward_score']:.1f}")
        print(f"  Backlog:         {row['sig_backlog']:.0f}  (current={row['backlog_current']}, prior={row['backlog_prior']}, growth={row['backlog_growth_pct']})")
        print(f"  Partnership:     {row['sig_partnership_mismatch']:.0f}  ({row['partnership_names']})")
        print(f"  New TAM:         {row['sig_new_tam']:.0f}  ({row['new_tam_keywords']})")
        print(f"  Segment X-over:  {row['sig_segment_crossover']:.0f}")
        print(f"  Capex/R&D:       {row['sig_capex_inflection']:.0f}  (capex={row['capex_growth_pct']}, R&D={row['rd_growth_pct']})")
        print(f"  Milestone:       {row['sig_tech_milestone']:.0f}  ({row['milestone_keywords']})")
        print(f"  Verdict:         {row['forward_verdict'] or 'N/A'}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
