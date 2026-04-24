"""Multibagger Discovery Pipeline for the SEC Filing Intelligence system.

3-layer parallel discovery engine:
  Layer 2A: Hard keywords via SEC EDGAR EFTS (all 10-Ks, not universe-filtered)
  Layer 2B: Soft keywords via EFTS (broader moat indicators)
  Layer 2C: Structural signals from existing screener DB tables

Union -> moat type classification -> composite scoring -> top 50 report.

CLI:
    python -m sec_filing_intelligence.discovery --run
    python -m sec_filing_intelligence.discovery --run --dry-run
    python -m sec_filing_intelligence.discovery --ticker NBIS
    python -m sec_filing_intelligence.discovery --latest
"""

import argparse
import csv
import json
import re
import time
from datetime import date, timedelta
from pathlib import Path

import requests

from .config import (
    DISCOVERY_ALREADY_DISCOVERED_MAX_PCT_FROM_HIGH,
    DISCOVERY_ALREADY_DISCOVERED_MIN_ANALYSTS,
    DISCOVERY_ALREADY_DISCOVERED_MIN_CAP,
    DISCOVERY_ALREADY_DISCOVERED_PENALTY,
    DISCOVERY_DEBT_FREE_BONUS,
    DISCOVERY_DEBT_FREE_DE_THRESHOLD,
    DISCOVERY_FILING_FORMS,
    DISCOVERY_HARD_KEYWORDS,
    DISCOVERY_HISTORY_MIN_SCORE_DELTA,
    DISCOVERY_HISTORY_NEW_TICKER_BONUS,
    DISCOVERY_INSIDER_CLUSTER_MIN,
    DISCOVERY_INSIDER_CLUSTER_VALUE_MIN,
    DISCOVERY_KEYWORD_MOAT_MAP,
    DISCOVERY_LOW_ANALYST_BONUS,
    DISCOVERY_MAX_ANALYST_COUNT,
    DISCOVERY_MAX_REVENUE_M,
    DISCOVERY_MCAP_SCORES,
    DISCOVERY_MIN_PATENT_COUNT_3YR,
    DISCOVERY_MIN_REVENUE_GROWTH_PCT,
    DISCOVERY_OUTPUT_DIR,
    DISCOVERY_REVENUE_DECLINE_PENALTY,
    DISCOVERY_SECTOR_MAP,
    DISCOVERY_SECTOR_SCORE,
    DISCOVERY_SOFT_KEYWORDS,
    DISCOVERY_THESIS_MATCH_BONUS,
    DISCOVERY_UNFUNDED_DEBT_DE_THRESHOLD,
    DISCOVERY_UNFUNDED_DEBT_PENALTY,
    DISCOVERY_UNFUNDED_DEBT_RUNWAY_THRESHOLD,
    DISCOVERY_WEIGHT_FLAG_COUNT,
    DISCOVERY_WEIGHT_HARD_KEYWORD,
    DISCOVERY_WEIGHT_MOAT_DIVERSITY,
    DISCOVERY_WEIGHT_SOFT_KEYWORD,
    DISCOVERY_WEIGHT_STRUCTURAL,
    DISCOVERY_ZERO_ANALYST_BONUS,
    DISCOVERY_GOV_SOLE_SOURCE_BONUS,
    DISCOVERY_GOV_SINGLE_BIDDER_BONUS,
    DISCOVERY_GOV_RESTRICTED_BONUS,
    DISCOVERY_GOV_CONTRACT_BONUS,
    DISCOVERY_CUSTOMER_XVAL_MAX_TICKERS,
    DISCOVERY_CUSTOMER_XVAL_BONUS,
    DISCOVERY_CUSTOMER_XVAL_MULTI_BONUS,
    DISCOVERY_CONTEXT_MAX_TICKERS,
    DISCOVERY_CONTEXT_MDA_BONUS,
    DISCOVERY_CONTEXT_RISK_FACTOR_PENALTY,
    DISCOVERY_CONTEXT_SELF_CLAIM_BONUS,
    DISCOVERY_10K_SECTIONS,
    USASPENDING_API_URL,
    USASPENDING_SEARCH_YEARS,
    USASPENDING_TOP_CONTRACTS,
    USASPENDING_BATCH_DELAY,
    USASPENDING_MAX_TICKERS,
    EDGAR_FULL_TEXT_SEARCH_URL,
    EDGAR_RATE_LIMIT_RPS,
    EDGAR_REQUEST_TIMEOUT,
    EDGAR_USER_AGENT,
    TABLE_DISCOVERY_FLAGS,
    TABLE_DISCOVERY_HISTORY,
    TABLE_KILL_LIST,
    TABLE_TEXT_SEARCH_HITS,
)
from .db import get_connection, run_migration
from .filing_scanner import _cik_to_ticker_lookup, _load_cik_maps
from .utils import get_logger, rate_limiter

logger = get_logger("discovery", "screener_discovery.log")

_headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}


def _ensure_phase2_columns() -> None:
    """Add Phase 2+3 columns (sic, sector, market_cap, fundamentals, etc.) if they don't exist.

    Safe to call multiple times. Uses ALTER TABLE so it doesn't touch db.py _DDL
    and won't break existing tests that don't need Phase 2/3 features.
    """
    with get_connection() as conn:
        tsh_cols = {r[1] for r in conn.execute(
            f"PRAGMA table_info({TABLE_TEXT_SEARCH_HITS})"
        ).fetchall()}
        if "sic" not in tsh_cols:
            conn.execute(f"ALTER TABLE {TABLE_TEXT_SEARCH_HITS} ADD COLUMN sic TEXT DEFAULT ''")

        df_cols = {r[1] for r in conn.execute(
            f"PRAGMA table_info({TABLE_DISCOVERY_FLAGS})"
        ).fetchall()}
        for col_name, col_def in [
            # Phase 2 columns
            ("sector", "TEXT DEFAULT 'unknown'"),
            ("market_cap", "REAL"),
            ("analyst_count", "INTEGER"),
            ("thesis_match_count", "INTEGER DEFAULT 0"),
            # Phase 3 columns
            ("revenue", "REAL"),
            ("revenue_growth", "REAL"),
            ("debt_to_equity", "REAL"),
            ("cash_runway", "REAL"),
            ("pct_from_52w_high", "REAL"),
            # Phase 4 columns
            ("gov_contracts", "INTEGER"),
            ("gov_total_value", "REAL"),
            ("gov_sole_source", "INTEGER DEFAULT 0"),
            ("gov_single_bidder", "INTEGER DEFAULT 0"),
            # Phase 4b columns
            ("customer_mentions", "INTEGER DEFAULT 0"),
            ("mentioning_companies", "TEXT"),
            # Phase 5b: 10-K context analysis columns
            ("self_claim_count", "INTEGER DEFAULT 0"),
            ("risk_factor_count", "INTEGER DEFAULT 0"),
            ("keyword_contexts", "TEXT"),
            # Diamond score columns
            ("diamond_score", "INTEGER DEFAULT 0"),
            ("diamond_crash", "INTEGER DEFAULT 0"),
            ("diamond_undiscovered", "INTEGER DEFAULT 0"),
            ("diamond_growth", "INTEGER DEFAULT 0"),
            ("diamond_balance", "INTEGER DEFAULT 0"),
            ("diamond_moat", "INTEGER DEFAULT 0"),
            # Fresh crash column
            ("fresh_crash", "INTEGER DEFAULT 0"),
            ("pct_1m_change", "REAL"),
        ]:
            if col_name not in df_cols:
                conn.execute(
                    f"ALTER TABLE {TABLE_DISCOVERY_FLAGS} ADD COLUMN {col_name} {col_def}"
                )

# ── EFTS search (discovery-specific: 10-K only) ────────────────────────────


@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _efts_search(
    query: str,
    start_date: str,
    end_date: str,
    forms: str = "10-K",
    start_from: int = 0,
) -> dict | None:
    """Execute a rate-limited EFTS query against SEC full-text search.

    Args:
        query: Search term (will be quoted).
        start_date: YYYY-MM-DD start of date range.
        end_date: YYYY-MM-DD end of date range.
        forms: Filing types to search (default "10-K").
        start_from: Pagination offset.

    Returns:
        JSON response dict or None on failure after retries.
    """
    params = {
        "q": f'"{query}"',
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "forms": forms,
        "from": start_from,
    }

    for attempt in range(4):
        try:
            resp = requests.get(
                EDGAR_FULL_TEXT_SEARCH_URL,
                params=params,
                headers=_headers,
                timeout=EDGAR_REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("EFTS 429 rate limit, backing off %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < 3:
                logger.warning("EFTS attempt %d failed: %s", attempt + 1, e)
                time.sleep(1)
            else:
                logger.error("EFTS failed after 4 attempts for %r: %s", query, e)
    return None


# ── Hit parser ──────────────────────────────────────────────────────────────

# Regex for extracting ticker from display_names — matches first parenthesised
# group that looks like a ticker (letters only, no digits).
_TICKER_RE = re.compile(r"\(([A-Z]{2,5})\)")


def _parse_discovery_hits(
    data: dict, keyword: str, layer: str
) -> list[dict]:
    """Parse EFTS response into discovery hit dicts.

    Extracts ticker from display_names, assigns moat types from config map,
    and builds filing URLs. Does NOT filter by universe — the whole point
    is finding NEW tickers.

    Args:
        data: Raw EFTS JSON response.
        keyword: The keyword that was searched.
        layer: "2a" or "2b".

    Returns:
        List of hit dicts ready for storage.
    """
    hits = []
    raw_hits = data.get("hits", {}).get("hits", [])

    moat_types = DISCOVERY_KEYWORD_MOAT_MAP.get(keyword, ["technology"])
    relevance = 1.0 if layer == "2a" else 0.7

    for hit in raw_hits:
        src = hit.get("_source", {})

        # --- Extract ticker ---
        display_names = src.get("display_names", [])
        ticker = None
        company_name = None

        for dn in display_names:
            company_name = dn.split("(")[0].strip() if "(" in dn else dn.strip()
            m = _TICKER_RE.search(dn)
            if m:
                ticker = m.group(1)
                break

        # CIK fallback: if display_names didn't yield a ticker
        if not ticker:
            ciks = src.get("ciks", [])
            for cik in ciks:
                ticker = _cik_to_ticker_lookup(cik)
                if ticker:
                    break

        if not ticker:
            continue

        # Skip tickers with digits (warrants/units like AMPXW, NBIS+) or single-char
        if len(ticker) < 2 or any(c.isdigit() for c in ticker):
            continue

        # --- Build filing URL from accession number ---
        adsh = src.get("adsh", "")
        cik_str = src.get("ciks", [""])[0] if src.get("ciks") else ""
        filing_url = ""
        if adsh and cik_str:
            cik_num = cik_str.lstrip("0") or "0"
            adsh_nodash = adsh.replace("-", "")
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik_num}/{adsh_nodash}/{adsh}-index.htm"
            )

        # Extract SIC code from EFTS response
        sic = src.get("sics", [""])[0] if src.get("sics") else ""

        hits.append(
            {
                "ticker": ticker,
                "keyword": keyword,
                "keyword_layer": layer,
                "moat_types": ",".join(moat_types),
                "filing_date": src.get("file_date", ""),
                "filing_type": src.get("form", "10-K"),
                "filing_url": filing_url,
                "cik": cik_str,
                "company_name": company_name or "",
                "relevance_score": relevance,
                "sic": sic,
            }
        )

    return hits


# ── Paginated keyword search ────────────────────────────────────────────────

_MAX_PAGES = 20
_HITS_PER_PAGE = 100


def _search_keyword_all_pages(
    keyword: str, layer: str, start_date: str, end_date: str
) -> list[dict]:
    """Search one keyword across all pages, up to _MAX_PAGES.

    Args:
        keyword: Text to search in 10-K filings.
        layer: "2a" or "2b".
        start_date: YYYY-MM-DD.
        end_date: YYYY-MM-DD.

    Returns:
        Aggregated list of parsed hit dicts.
    """
    all_hits: list[dict] = []

    for page in range(_MAX_PAGES):
        offset = page * _HITS_PER_PAGE
        data = _efts_search(keyword, start_date, end_date, forms=DISCOVERY_FILING_FORMS, start_from=offset)
        if data is None:
            logger.warning("EFTS returned None for %r page %d, stopping", keyword, page)
            break

        total = data.get("hits", {}).get("total", {}).get("value", 0)
        page_hits = _parse_discovery_hits(data, keyword, layer)
        all_hits.extend(page_hits)

        logger.info(
            "Keyword %r layer=%s page=%d: %d hits (total=%d)",
            keyword, layer, page, len(page_hits), total,
        )

        # Stop if we've fetched everything
        if offset + _HITS_PER_PAGE >= total:
            break

    return all_hits


# ── Layer entry points ──────────────────────────────────────────────────────


def search_layer_2a(start_date: str, end_date: str) -> list[dict]:
    """Layer 2A: Hard keywords — strong sole-source / monopoly language in 10-K.

    Searches all DISCOVERY_HARD_KEYWORDS via EFTS with full pagination.
    Returns aggregated parsed hits with relevance_score=1.0.
    """
    all_hits: list[dict] = []
    for kw in DISCOVERY_HARD_KEYWORDS:
        hits = _search_keyword_all_pages(kw, "2a", start_date, end_date)
        all_hits.extend(hits)
        logger.info("Layer 2a keyword %r: %d hits", kw, len(hits))
    logger.info("Layer 2a complete: %d total hits from %d keywords",
                len(all_hits), len(DISCOVERY_HARD_KEYWORDS))
    return all_hits


def search_layer_2b(start_date: str, end_date: str) -> list[dict]:
    """Layer 2B: Soft keywords — broader moat indicators needing context.

    Searches all DISCOVERY_SOFT_KEYWORDS via EFTS with full pagination.
    Returns aggregated parsed hits with relevance_score=0.7.
    """
    all_hits: list[dict] = []
    for kw in DISCOVERY_SOFT_KEYWORDS:
        hits = _search_keyword_all_pages(kw, "2b", start_date, end_date)
        all_hits.extend(hits)
        logger.info("Layer 2b keyword %r: %d hits", kw, len(hits))
    logger.info("Layer 2b complete: %d total hits from %d keywords",
                len(all_hits), len(DISCOVERY_SOFT_KEYWORDS))
    return all_hits


# ── Storage ─────────────────────────────────────────────────────────────────


def _store_text_search_hits(hits: list[dict], scan_date: str) -> int:
    """Persist discovery hits to scr_text_search_hits.

    Uses INSERT OR IGNORE for dedup on (ticker, scan_date, keyword, filing_date).

    Args:
        hits: List of parsed hit dicts from _parse_discovery_hits.
        scan_date: YYYY-MM-DD scan date stamp.

    Returns:
        Count of new rows inserted.
    """
    if not hits:
        return 0

    _ensure_phase2_columns()

    inserted = 0
    with get_connection() as conn:
        for h in hits:
            cur = conn.execute(
                """INSERT OR IGNORE INTO scr_text_search_hits
                   (ticker, scan_date, keyword, keyword_layer, moat_type,
                    filing_date, filing_type, filing_url, cik, company_name,
                    relevance_score, sic)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    h["ticker"],
                    scan_date,
                    h["keyword"],
                    h["keyword_layer"],
                    h["moat_types"],
                    h["filing_date"],
                    h["filing_type"],
                    h["filing_url"],
                    h["cik"],
                    h["company_name"],
                    h["relevance_score"],
                    h.get("sic", ""),
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()

    logger.info("Stored %d new text search hits (scan_date=%s)", inserted, scan_date)
    return inserted


# ── Layer 2C: Structural signals from existing screener tables ─────────────


def search_layer_2c(scan_date: str) -> list[dict]:
    """Layer 2C: Structural signals from existing scr_* tables.

    Queries analyst coverage, insider transactions, patent summaries, and
    fundamentals for tickers in the active universe (not killed, not on kill list).

    Args:
        scan_date: YYYY-MM-DD scan date (used for recency windows).

    Returns:
        List of signal dicts with keys: ticker, signal_type, evidence, company_name.
    """
    signals: list[dict] = []
    with get_connection() as conn:
        # --- 1. under_followed: analyst_count <= threshold (latest date per ticker) ---
        rows = conn.execute(
            f"""
            SELECT ac.ticker, ac.analyst_count, u.company_name
            FROM scr_analyst_coverage ac
            INNER JOIN scr_universe u ON u.ticker = ac.ticker
                AND u.is_active = 1 AND u.is_killed = 0
            LEFT JOIN {TABLE_KILL_LIST} kl ON kl.ticker = ac.ticker
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM scr_analyst_coverage
                GROUP BY ticker
            ) latest ON latest.ticker = ac.ticker AND latest.max_date = ac.date
            WHERE kl.ticker IS NULL
              AND ac.analyst_count <= ?
            """,
            (DISCOVERY_MAX_ANALYST_COUNT,),
        ).fetchall()
        for r in rows:
            signals.append({
                "ticker": r["ticker"],
                "signal_type": "under_followed",
                "evidence": f"analyst_count={r['analyst_count']}",
                "company_name": r["company_name"] or "",
            })

        # --- 2. insider_cluster: 2+ distinct open-market purchasers in last 90 days ---
        rows = conn.execute(
            f"""
            SELECT it.ticker, COUNT(DISTINCT it.insider_name) AS buyer_count,
                   u.company_name
            FROM scr_insider_transactions it
            INNER JOIN scr_universe u ON u.ticker = it.ticker
                AND u.is_active = 1 AND u.is_killed = 0
            LEFT JOIN {TABLE_KILL_LIST} kl ON kl.ticker = it.ticker
            WHERE kl.ticker IS NULL
              AND it.transaction_type = 'purchase'
              AND it.is_open_market = 1
              AND it.total_value >= ?
              AND it.transaction_date >= date(?, '-90 days')
            GROUP BY it.ticker
            HAVING buyer_count >= ?
            """,
            (DISCOVERY_INSIDER_CLUSTER_VALUE_MIN, scan_date, DISCOVERY_INSIDER_CLUSTER_MIN),
        ).fetchall()
        for r in rows:
            signals.append({
                "ticker": r["ticker"],
                "signal_type": "insider_cluster",
                "evidence": f"distinct_buyers={r['buyer_count']}",
                "company_name": r["company_name"] or "",
            })

        # --- 3. patent_concentration: patents_last_3yr >= threshold ---
        rows = conn.execute(
            f"""
            SELECT ps.ticker, ps.patents_last_3yr, u.company_name
            FROM scr_patent_summary ps
            INNER JOIN scr_universe u ON u.ticker = ps.ticker
                AND u.is_active = 1 AND u.is_killed = 0
            LEFT JOIN {TABLE_KILL_LIST} kl ON kl.ticker = ps.ticker
            WHERE kl.ticker IS NULL
              AND ps.patents_last_3yr >= ?
            """,
            (DISCOVERY_MIN_PATENT_COUNT_3YR,),
        ).fetchall()
        for r in rows:
            signals.append({
                "ticker": r["ticker"],
                "signal_type": "patent_concentration",
                "evidence": f"patents_last_3yr={r['patents_last_3yr']}",
                "company_name": r["company_name"] or "",
            })

        # --- 4. small_growing: revenue < $200M AND growth > 30% (latest date) ---
        rows = conn.execute(
            f"""
            SELECT f.ticker, f.revenue_ttm, f.revenue_growth_yoy, u.company_name
            FROM scr_fundamentals f
            INNER JOIN scr_universe u ON u.ticker = f.ticker
                AND u.is_active = 1 AND u.is_killed = 0
            LEFT JOIN {TABLE_KILL_LIST} kl ON kl.ticker = f.ticker
            INNER JOIN (
                SELECT ticker, MAX(date) AS max_date
                FROM scr_fundamentals
                GROUP BY ticker
            ) latest ON latest.ticker = f.ticker AND latest.max_date = f.date
            WHERE kl.ticker IS NULL
              AND f.revenue_ttm < ?
              AND f.revenue_growth_yoy > ?
            """,
            (DISCOVERY_MAX_REVENUE_M * 1_000_000, DISCOVERY_MIN_REVENUE_GROWTH_PCT),
        ).fetchall()
        for r in rows:
            signals.append({
                "ticker": r["ticker"],
                "signal_type": "small_growing",
                "evidence": (
                    f"revenue_ttm={r['revenue_ttm']:.0f},"
                    f"growth_yoy={r['revenue_growth_yoy']:.1f}%"
                ),
                "company_name": r["company_name"] or "",
            })

    logger.info("Layer 2c complete: %d structural signals", len(signals))
    return signals


# ── Task 4: Union + Moat Detection + Scoring ──────────────────────────────


def _build_discovery_flags(
    text_hits: list[dict],
    structural_signals: list[dict],
    scan_date: str,
) -> dict[str, dict]:
    """Union text search hits (2A/2B) and structural signals (2C) into per-ticker flags.

    Args:
        text_hits: Parsed hits from search_layer_2a / search_layer_2b.
        structural_signals: Dicts from search_layer_2c.
        scan_date: YYYY-MM-DD.

    Returns:
        Dict keyed by ticker, each value a flag record dict.
    """
    flags: dict[str, dict] = {}

    def _ensure(ticker: str, company_name: str = "", cik: str = "") -> dict:
        if ticker not in flags:
            flags[ticker] = {
                "layer_2a": 0,
                "layer_2b": 0,
                "layer_2c": 0,
                "flag_count": 0,
                "keywords_matched": [],
                "structural_signals": [],
                "moat_types": set(),
                "moat_type_count": 0,
                "company_name": company_name,
                "cik": cik,
            }
        # Update name/cik if we get a non-empty one later
        if company_name and not flags[ticker]["company_name"]:
            flags[ticker]["company_name"] = company_name
        if cik and not flags[ticker]["cik"]:
            flags[ticker]["cik"] = cik
        return flags[ticker]

    # --- Text hits (2A / 2B) ---
    for h in text_hits:
        ticker = h["ticker"]
        rec = _ensure(ticker, h.get("company_name", ""), h.get("cik", ""))

        layer = h.get("keyword_layer", "")
        if layer == "2a":
            rec["layer_2a"] = 1
        elif layer == "2b":
            rec["layer_2b"] = 1

        kw = h.get("keyword", "")
        if kw and kw not in rec["keywords_matched"]:
            rec["keywords_matched"].append(kw)

        # moat_types comes as comma-separated string from EFTS parser
        raw_moats = h.get("moat_types", "")
        if raw_moats:
            for mt in raw_moats.split(","):
                mt = mt.strip()
                if mt:
                    rec["moat_types"].add(mt)

    # --- Structural signals (2C) ---
    for s in structural_signals:
        ticker = s["ticker"]
        rec = _ensure(ticker, s.get("company_name", ""), "")
        rec["layer_2c"] = 1

        sig_type = s.get("signal_type", "")
        if sig_type and sig_type not in rec["structural_signals"]:
            rec["structural_signals"].append(sig_type)

    # --- Finalize: compute flag_count, sort moat_types ---
    for rec in flags.values():
        rec["flag_count"] = rec["layer_2a"] + rec["layer_2b"] + rec["layer_2c"]
        rec["moat_types"] = sorted(rec["moat_types"])
        rec["moat_type_count"] = len(rec["moat_types"])

    return flags


def _classify_sector(sic_str: str) -> str:
    """Map SIC code to discovery sector category.

    Args:
        sic_str: SIC code as string (e.g. "3761").

    Returns:
        Sector name from DISCOVERY_SECTOR_MAP, or "unknown".
    """
    if not sic_str or not sic_str.isdigit():
        return "unknown"
    sic = int(sic_str)
    for sector, ranges in DISCOVERY_SECTOR_MAP.items():
        for lo, hi in ranges:
            if lo <= sic <= hi:
                return sector
    return "unknown"


def _enrich_flags(flags: dict[str, dict]) -> dict[str, dict]:
    """Enrich discovery flags with sector, market cap, analyst count, thesis matches.

    Queries DB for:
      1. SIC codes from scr_text_search_hits (most common per ticker)
      2. Market cap from scr_universe
      3. Analyst count from scr_analyst_coverage (latest per ticker)
      4. Thesis match count from scr_thesis_hits (distinct thesis_ids)

    Mutates and returns the flags dict with enrichment fields added.
    """
    tickers = list(flags.keys())
    if not tickers:
        return flags

    # Initialize enrichment fields
    for f in flags.values():
        f.setdefault("sector", "unknown")
        f.setdefault("market_cap", None)
        f.setdefault("analyst_count", None)
        f.setdefault("thesis_match_count", 0)

    with get_connection() as conn:
        # 1. Sector from SIC: find most common SIC per ticker
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"""SELECT ticker, sic, COUNT(*) AS cnt
                FROM {TABLE_TEXT_SEARCH_HITS}
                WHERE ticker IN ({placeholders})
                  AND sic IS NOT NULL AND sic != ''
                GROUP BY ticker, sic
                ORDER BY ticker, cnt DESC""",
            tickers,
        ).fetchall()

        # Pick the most frequent SIC per ticker
        seen_tickers: set[str] = set()
        for r in rows:
            t = r["ticker"]
            if t not in seen_tickers and t in flags:
                flags[t]["sector"] = _classify_sector(r["sic"])
                seen_tickers.add(t)

        # 2. Market cap from scr_universe (stored as market_cap_m in millions)
        rows = conn.execute(
            f"""SELECT ticker, market_cap_m
                FROM scr_universe
                WHERE ticker IN ({placeholders})
                  AND market_cap_m IS NOT NULL""",
            tickers,
        ).fetchall()
        for r in rows:
            if r["ticker"] in flags:
                flags[r["ticker"]]["market_cap"] = r["market_cap_m"] * 1_000_000

        # 3. Analyst count from scr_analyst_coverage (latest date per ticker)
        rows = conn.execute(
            f"""SELECT ac.ticker, ac.analyst_count
                FROM scr_analyst_coverage ac
                INNER JOIN (
                    SELECT ticker, MAX(date) AS max_date
                    FROM scr_analyst_coverage
                    WHERE ticker IN ({placeholders})
                    GROUP BY ticker
                ) latest ON latest.ticker = ac.ticker AND latest.max_date = ac.date
                WHERE ac.ticker IN ({placeholders})""",
            tickers + tickers,
        ).fetchall()
        for r in rows:
            if r["ticker"] in flags:
                flags[r["ticker"]]["analyst_count"] = r["analyst_count"]

        # 4. Thesis matches from scr_thesis_hits (count distinct thesis_id per ticker)
        # Table may not exist yet — handle gracefully
        try:
            rows = conn.execute(
                f"""SELECT ticker, COUNT(DISTINCT thesis_id) AS thesis_count
                    FROM scr_thesis_hits
                    WHERE ticker IN ({placeholders})
                    GROUP BY ticker""",
                tickers,
            ).fetchall()
            for r in rows:
                if r["ticker"] in flags:
                    flags[r["ticker"]]["thesis_match_count"] = r["thesis_count"]
        except Exception:
            # scr_thesis_hits may not exist if asymmetry scanner hasn't run
            pass

    return flags


def _enrich_top_candidates(flags: dict[str, dict], top_n: int = 200) -> dict[str, dict]:
    """Fetch yfinance data for top N candidates missing fundamentals.

    After initial keyword/moat scoring, takes the top_n tickers by score that
    are missing market_cap (not already enriched from the DB), fetches basic
    fundamentals via yfinance, and stores the enrichment data in the flags dict.

    Keeps runtime reasonable: 200 tickers / 50 per batch = 4 batches * 2s = ~10s.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _enrich_flags.
        top_n: Max tickers to enrich via yfinance.

    Returns:
        The mutated flags dict with fundamentals added for enriched tickers.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available — skipping top candidate enrichment")
        return flags

    # Sort by composite_score desc, pick top_n where market_cap is still None
    sorted_tickers = sorted(
        flags.keys(),
        key=lambda t: flags[t].get("composite_score", 0),
        reverse=True,
    )
    to_enrich = [
        t for t in sorted_tickers
        if flags[t].get("market_cap") is None
    ][:top_n]

    if not to_enrich:
        logger.info("No candidates need yfinance enrichment")
        return flags

    logger.info("Enriching %d top candidates via yfinance", len(to_enrich))

    batch_size = 50
    max_batches = 20
    batches_done = 0

    for i in range(0, len(to_enrich), batch_size):
        if batches_done >= max_batches:
            logger.info("Reached max batch limit (%d), stopping enrichment", max_batches)
            break

        batch = to_enrich[i : i + batch_size]
        ticker_str = " ".join(batch)

        try:
            tickers_obj = yf.Tickers(ticker_str)

            for ticker in batch:
                try:
                    info = tickers_obj.tickers[ticker].info
                    if not info:
                        continue

                    f = flags[ticker]
                    mcap = info.get("marketCap")
                    if mcap is not None:
                        f["market_cap"] = float(mcap)

                    revenue = info.get("totalRevenue")
                    if revenue is not None:
                        f["revenue"] = float(revenue)

                    rev_growth = info.get("revenueGrowth")
                    if rev_growth is not None:
                        f["revenue_growth"] = float(rev_growth)

                    de = info.get("debtToEquity")
                    if de is not None:
                        f["debt_to_equity"] = float(de)

                    total_cash = info.get("totalCash")
                    total_debt = info.get("totalDebt")

                    # Calculate cash runway: quarters of cash vs quarterly debt service
                    if total_debt and total_debt > 0 and total_cash is not None:
                        runway = total_cash / (total_debt / 4)
                        f["cash_runway"] = min(runway, 40.0)
                    elif total_debt is not None and total_debt == 0 and total_cash is not None:
                        f["cash_runway"] = 40.0  # No debt = max runway

                    current_ratio = info.get("currentRatio")
                    if current_ratio is not None:
                        f["current_ratio"] = float(current_ratio)

                    analysts = info.get("numberOfAnalystOpinions")
                    if analysts is not None:
                        f["analyst_count"] = int(analysts)

                    inst_own = info.get("heldPercentInstitutions")
                    if inst_own is not None:
                        f["institutional_ownership"] = float(inst_own)

                    shares_current = info.get("sharesOutstanding")
                    shares_implied = info.get("impliedSharesOutstanding")
                    if shares_current and shares_implied and shares_implied > 0:
                        f["shares_growth"] = (shares_current - shares_implied) / shares_implied

                    high_52w = info.get("fiftyTwoWeekHigh")
                    current = info.get("currentPrice")
                    if high_52w and current and high_52w > 0:
                        f["pct_from_52w_high"] = ((current - high_52w) / high_52w) * 100

                    # Fresh crash detection: 1-month price change
                    try:
                        ticker_obj = tickers_obj.tickers[ticker]
                        hist = ticker_obj.history(period="1mo")
                        if hist is not None and not hist.empty and len(hist) > 1:
                            month_ago_price = hist['Close'].iloc[0]
                            current_price = hist['Close'].iloc[-1]
                            if month_ago_price > 0:
                                pct_1m = ((current_price - month_ago_price) / month_ago_price) * 100
                                f["pct_1m_change"] = pct_1m
                                f["fresh_crash"] = pct_1m < -30
                    except Exception:
                        pass  # Non-critical — skip if history unavailable

                except Exception as e:
                    logger.debug("yfinance enrichment failed for %s: %s", ticker, e)
                    continue

        except Exception as e:
            logger.warning("yfinance batch failed: %s", e)

        batches_done += 1
        if i + batch_size < len(to_enrich):
            time.sleep(2)

    enriched_count = sum(
        1 for t in to_enrich if flags[t].get("market_cap") is not None
    )
    logger.info("yfinance enrichment complete: %d/%d tickers enriched", enriched_count, len(to_enrich))
    return flags


# ── Phase 4: USAspending.gov contract validation ─────────────────────────────


def _clean_company_name(name: str) -> str:
    """Clean company name for USAspending search.

    Strips common corporate suffixes, slashes like /DE, and trims to
    a reasonable length for search matching.

    Args:
        name: Raw company name from SEC filings.

    Returns:
        Cleaned name suitable for USAspending recipient search.
    """
    if not name:
        return ""
    # Strip common suffixes — loop until stable since multiple may stack
    suffixes = [
        "/DE/", "/DE", ", Inc.", ", Inc", " Inc.", " Inc",
        ", Corp.", ", Corp", " Corp.", " Corp",
        ", Ltd.", ", Ltd", " Ltd.", " Ltd",
        " Co.", " Holdings", " Group",
        " plc", " PLC", " LLC", " LP", " L.P.",
        " N.V.", " S.A.", " AG",
    ]
    prev = None
    while prev != name:
        prev = name
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

    # Collapse whitespace and strip
    name = re.sub(r"\s+", " ", name).strip()
    # Remove trailing commas/periods
    name = name.rstrip(",. ")

    # If > 4 words, take first 3
    words = name.split()
    if len(words) > 4:
        name = " ".join(words[:3])

    return name


def _search_usaspending(company_name: str) -> dict:
    """Search USAspending for federal contracts by company name.

    Queries the USAspending spending_by_award endpoint for contracts,
    then fetches detail for each to check competition status.

    Args:
        company_name: Cleaned company name for recipient search.

    Returns:
        Dict with:
            - total_contracts: int
            - total_value: float
            - sole_source_count: int (extent_competed in B,C,G,NDO)
            - single_bidder_count: int (number_of_offers = 1)
            - restricted_count: int (extent_competed = D)
            - top_contract_value: float
            - top_contract_desc: str
    """
    result = {
        "total_contracts": 0,
        "total_value": 0.0,
        "sole_source_count": 0,
        "single_bidder_count": 0,
        "restricted_count": 0,
        "top_contract_value": 0.0,
        "top_contract_desc": "",
    }

    if not company_name:
        return result

    # Build time period (last N years)
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=365 * USASPENDING_SEARCH_YEARS)).isoformat()

    search_payload = {
        "filters": {
            "recipient_search_text": [company_name],
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
        },
        "fields": [
            "Award ID", "Recipient Name", "Award Amount",
            "Description", "generated_internal_id",
        ],
        "limit": USASPENDING_TOP_CONTRACTS,
        "sort": "Award Amount",
        "order": "desc",
        "page": 1,
    }

    try:
        resp = requests.post(
            f"{USASPENDING_API_URL}/search/spending_by_award/",
            json=search_payload,
            timeout=30,
        )
        if not resp.ok:
            logger.debug("USAspending search failed for %s: %s", company_name, resp.status_code)
            return result

        data = resp.json()
        results_list = data.get("results", [])
        result["total_contracts"] = data.get("page_metadata", {}).get("total", len(results_list))

    except (requests.RequestException, ValueError) as e:
        logger.debug("USAspending search error for %s: %s", company_name, e)
        return result

    if not results_list:
        return result

    # Sole-source extent_competed codes
    sole_source_codes = {"B", "C", "G", "NDO"}

    # Fetch detail for each award to get competition data
    for award in results_list:
        award_amount = award.get("Award Amount") or 0
        result["total_value"] += float(award_amount)

        internal_id = award.get("generated_internal_id")
        if not internal_id:
            continue

        # Track top contract
        if float(award_amount) > result["top_contract_value"]:
            result["top_contract_value"] = float(award_amount)
            result["top_contract_desc"] = (award.get("Description") or "")[:100]

        time.sleep(USASPENDING_BATCH_DELAY)

        try:
            detail_resp = requests.get(
                f"{USASPENDING_API_URL}/awards/{internal_id}/",
                timeout=30,
            )
            if not detail_resp.ok:
                continue

            detail = detail_resp.json()
            contract_data = detail.get("latest_transaction_contract_data") or {}
            extent_competed = contract_data.get("extent_competed") or ""
            offers = contract_data.get("number_of_offers_received")

            if extent_competed in sole_source_codes:
                result["sole_source_count"] += 1
            elif extent_competed == "D":
                result["restricted_count"] += 1

            if offers is not None:
                try:
                    if int(offers) == 1:
                        result["single_bidder_count"] += 1
                except (ValueError, TypeError):
                    pass

        except (requests.RequestException, ValueError) as e:
            logger.debug("USAspending detail error for award %s: %s", internal_id, e)
            continue

    return result


def _enrich_gov_contracts(
    flags: dict[str, dict], top_n: int = USASPENDING_MAX_TICKERS
) -> dict[str, dict]:
    """Enrich top N candidates with USAspending contract data.

    Sorts flags by score descending, takes top_n tickers that have a
    company_name, queries USAspending for each, and stores government
    contract metadata in the flags dict.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + enrichment.
        top_n: Max tickers to check against USAspending.

    Returns:
        The mutated flags dict with gov contract fields added.
    """
    sorted_tickers = sorted(
        flags.keys(),
        key=lambda t: flags[t].get("composite_score", 0),
        reverse=True,
    )
    to_check = [
        t for t in sorted_tickers
        if flags[t].get("company_name")
    ][:top_n]

    if not to_check:
        logger.info("No candidates with company_name for USAspending enrichment")
        return flags

    logger.info("Checking %d tickers against USAspending.gov", len(to_check))

    checked = 0
    gov_found = 0
    for ticker in to_check:
        raw_name = flags[ticker].get("company_name", "")
        clean_name = _clean_company_name(raw_name)

        if not clean_name:
            continue

        usa_data = _search_usaspending(clean_name)
        checked += 1

        f = flags[ticker]
        f["gov_contracts"] = usa_data["total_contracts"]
        f["gov_total_value"] = usa_data["total_value"]
        f["gov_sole_source"] = usa_data["sole_source_count"] > 0
        f["gov_single_bidder"] = (
            usa_data["single_bidder_count"] > 0
            and usa_data["sole_source_count"] == 0
        )
        f["gov_restricted"] = (
            usa_data["restricted_count"] > 0
            and usa_data["sole_source_count"] == 0
            and usa_data["single_bidder_count"] == 0
        )

        if usa_data["total_contracts"] > 0:
            gov_found += 1
            # Build human-readable summary
            parts = [f"{usa_data['total_contracts']} contracts, ${usa_data['total_value']:,.0f} total"]
            if usa_data["sole_source_count"] > 0:
                parts.append(f"{usa_data['sole_source_count']} sole-source")
            if usa_data["single_bidder_count"] > 0:
                parts.append(f"{usa_data['single_bidder_count']} single-bidder")
            f["gov_contract_summary"] = "; ".join(parts)

        if checked % 10 == 0:
            logger.info("USAspending progress: %d/%d checked, %d with contracts", checked, len(to_check), gov_found)

    logger.info(
        "USAspending enrichment complete: %d/%d checked, %d with government contracts",
        checked, len(to_check), gov_found,
    )
    return flags


# ── Phase 4b: Customer 10-K cross-validation ────────────────────────────────


# Common English words that are too generic to search as company names
_GENERIC_WORDS = frozenset({
    "the", "one", "new", "first", "next", "global", "national", "american",
    "united", "general", "central", "western", "eastern", "northern", "southern",
    "pacific", "atlantic", "international", "digital", "advanced", "applied",
    "premier", "prime", "summit", "apex", "alpha", "delta", "omega", "pioneer",
    "standard", "modern", "core", "focus", "target", "energy", "power", "solar",
    "clean", "green", "blue", "gold", "silver", "iron", "steel", "copper",
})


def _search_customer_mentions(company_name: str, own_cik: str) -> dict:
    """Search EFTS for company name appearing in OTHER companies' 10-Ks.

    If CompanyA's name appears in CompanyB's 10-K, that's external validation
    of a customer/partner relationship. We search the last 3 years of 10-K
    filings and separate "own filings" from "other company mentions".

    Args:
        company_name: Cleaned company name to search for.
        own_cik: The company's own CIK (to exclude self-filings).

    Returns:
        {
            "mention_count": int,        # filings from other companies
            "mentioning_companies": list, # names of companies that mention us
            "own_filings": int,          # our own filings (for context)
        }
    """
    result = {
        "mention_count": 0,
        "mentioning_companies": [],
        "own_filings": 0,
    }

    if not company_name or len(company_name) < 5:
        return result

    # Skip single-word generic names
    words = company_name.split()
    if len(words) == 1 and company_name.lower() in _GENERIC_WORDS:
        return result

    # Date range: last 3 years
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=365 * 3)).isoformat()

    # Normalize own CIK for comparison (strip leading zeros)
    own_cik_stripped = own_cik.lstrip("0") if own_cik else ""

    seen_companies: dict[str, str] = {}  # cik -> company_name

    # Paginate up to 3 pages (300 results max)
    max_pages = 3
    for page in range(max_pages):
        offset = page * _HITS_PER_PAGE
        data = _efts_search(company_name, start_date, end_date, forms=DISCOVERY_FILING_FORMS, start_from=offset)
        if data is None:
            break

        total = data.get("hits", {}).get("total", {}).get("value", 0)
        raw_hits = data.get("hits", {}).get("hits", [])

        for hit in raw_hits:
            src = hit.get("_source", {})
            ciks = src.get("ciks", [])
            display_names = src.get("display_names", [])

            # Get first CIK for this filing
            filing_cik = ciks[0] if ciks else ""
            filing_cik_stripped = filing_cik.lstrip("0") if filing_cik else ""

            if not filing_cik_stripped:
                continue

            # Compare CIK to determine if it's own filing or another company
            if own_cik_stripped and filing_cik_stripped == own_cik_stripped:
                result["own_filings"] += 1
            else:
                # Extract company name from display_names
                other_name = ""
                for dn in display_names:
                    other_name = dn.split("(")[0].strip() if "(" in dn else dn.strip()
                    break

                if filing_cik_stripped not in seen_companies:
                    seen_companies[filing_cik_stripped] = other_name or f"CIK {filing_cik}"

        # Stop if we've fetched everything
        if offset + _HITS_PER_PAGE >= total:
            break

    result["mention_count"] = len(seen_companies)
    result["mentioning_companies"] = sorted(seen_companies.values())

    return result


def _enrich_customer_mentions(
    flags: dict[str, dict], top_n: int = DISCOVERY_CUSTOMER_XVAL_MAX_TICKERS
) -> dict[str, dict]:
    """Check if top N tickers are mentioned in other companies' 10-Ks.

    Sorts flags by score descending, takes top_n tickers that have both
    company_name and cik, and queries EFTS for external mentions.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + enrichment.
        top_n: Max tickers to check for customer cross-validation.

    Returns:
        The mutated flags dict with customer_mentions and mentioning_companies added.
    """
    sorted_tickers = sorted(
        flags.keys(),
        key=lambda t: flags[t].get("composite_score", 0),
        reverse=True,
    )
    to_check = [
        t for t in sorted_tickers
        if flags[t].get("company_name") and flags[t].get("cik")
    ][:top_n]

    if not to_check:
        logger.info("No candidates with company_name+cik for customer cross-validation")
        return flags

    logger.info("Checking %d tickers for customer 10-K cross-validation", len(to_check))

    checked = 0
    found = 0
    for ticker in to_check:
        raw_name = flags[ticker].get("company_name", "")
        clean_name = _clean_company_name(raw_name)
        own_cik = flags[ticker].get("cik", "")

        if not clean_name or len(clean_name) < 5:
            continue

        xval = _search_customer_mentions(clean_name, own_cik)
        checked += 1

        f = flags[ticker]
        f["customer_mentions"] = xval["mention_count"]
        f["mentioning_companies"] = xval["mentioning_companies"]

        if xval["mention_count"] > 0:
            found += 1
            logger.info(
                "Customer xval %s: %d mentions by %s",
                ticker, xval["mention_count"],
                ", ".join(xval["mentioning_companies"][:5]),
            )

        if checked % 10 == 0:
            logger.info(
                "Customer xval progress: %d/%d checked, %d with mentions",
                checked, len(to_check), found,
            )

    logger.info(
        "Customer cross-validation complete: %d/%d checked, %d with external mentions",
        checked, len(to_check), found,
    )
    return flags


# ── Phase 5b: 10-K context analysis ──────────────────────────────────────────


@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _fetch_10k_text(filing_url: str) -> str | None:
    """Download the actual 10-K document text from an EDGAR filing index URL.

    Args:
        filing_url: EDGAR filing index URL ending in -index.htm.

    Returns:
        Plaintext of the 10-K filing, or None on any failure.
    """
    if not filing_url:
        return None
    try:
        resp = requests.get(filing_url, headers=_headers, timeout=30)
        if not resp.ok:
            logger.debug("Failed to fetch filing index %s: %s", filing_url, resp.status_code)
            return None
        index_html = resp.text
        base_url = filing_url.rsplit("/", 1)[0]
        link_re = re.compile(r'href="([^"]+\.htm)"', re.IGNORECASE)
        candidates = []
        for match in link_re.finditer(index_html):
            href = match.group(1)
            if "-index.htm" in href.lower():
                continue
            if href.lower().endswith(".xml"):
                continue
            candidates.append(href)
        if not candidates:
            logger.debug("No .htm document links found in %s", filing_url)
            return None
        doc_href = candidates[0]
        if doc_href.startswith("http"):
            doc_url = doc_href
        else:
            doc_url = f"{base_url}/{doc_href}"
        doc_resp = requests.get(doc_url, headers=_headers, timeout=30)
        if not doc_resp.ok:
            logger.debug("Failed to fetch 10-K document %s: %s", doc_url, doc_resp.status_code)
            return None
        raw_html = doc_resp.text[:600_000]
        text = re.sub(r"<[^>]+>", " ", raw_html)
        text = re.sub(r"\s+", " ", text)
        return text[:500_000] if text else None
    except (requests.RequestException, Exception) as e:
        logger.debug("Error fetching 10-K text from %s: %s", filing_url, e)
        return None


def _classify_keyword_section(text: str, keyword: str) -> str:
    """Determine which 10-K section a keyword appears in.

    Finds the keyword in the text (case-insensitive), then looks backwards
    from the keyword position for the nearest Item N section header.

    Args:
        text: Plaintext of the 10-K filing.
        keyword: The keyword to locate.

    Returns:
        Section name: business, risk_factors, properties, mda,
        financials, or unknown.
    """
    if not text or not keyword:
        return "unknown"
    kw_lower = keyword.lower()
    text_lower = text.lower()
    kw_pos = text_lower.find(kw_lower)
    if kw_pos < 0:
        return "unknown"
    text_before = text[:kw_pos]
    best_section = "unknown"
    best_pos = -1
    ordered_sections = [
        ("risk_factors", DISCOVERY_10K_SECTIONS["risk_factors"]),
        ("mda", DISCOVERY_10K_SECTIONS["mda"]),
        ("financials", DISCOVERY_10K_SECTIONS["financials"]),
        ("properties", DISCOVERY_10K_SECTIONS["properties"]),
        ("business", DISCOVERY_10K_SECTIONS["business"]),
    ]
    for section_name, pattern in ordered_sections:
        for match in re.finditer(pattern, text_before):
            if match.start() > best_pos:
                best_pos = match.start()
                best_section = section_name
    return best_section


def _enrich_keyword_context(
    flags: dict[str, dict],
    scan_date: str,
    top_n: int = DISCOVERY_CONTEXT_MAX_TICKERS,
) -> dict[str, dict]:
    """Download 10-K filings for top candidates and classify keyword sections.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + enrichment.
        scan_date: YYYY-MM-DD scan date stamp.
        top_n: Max tickers to analyze (default 50).

    Returns:
        The mutated flags dict with keyword_contexts, self_claim_count,
        risk_factor_count, and mda_count added.
    """
    sorted_tickers = sorted(
        flags.keys(),
        key=lambda t: flags[t].get("composite_score", 0),
        reverse=True,
    )[:top_n]
    if not sorted_tickers:
        logger.info("No tickers for 10-K context analysis")
        return flags
    ticker_hits: dict[str, list[dict]] = {}
    with get_connection() as conn:
        placeholders = ",".join("?" for _ in sorted_tickers)
        rows = conn.execute(
            f"""SELECT ticker, keyword, filing_url
                FROM {TABLE_TEXT_SEARCH_HITS}
                WHERE ticker IN ({placeholders})
                  AND scan_date = ?
                  AND filing_url IS NOT NULL
                  AND filing_url != ''""",
            sorted_tickers + [scan_date],
        ).fetchall()
        for r in rows:
            ticker_hits.setdefault(r["ticker"], []).append({
                "keyword": r["keyword"],
                "filing_url": r["filing_url"],
            })
    url_to_text: dict[str, str | None] = {}
    all_urls = set()
    for hits_list in ticker_hits.values():
        for h in hits_list:
            all_urls.add(h["filing_url"])
    logger.info(
        "10-K context analysis: %d tickers, %d unique filings to fetch",
        len(sorted_tickers), len(all_urls),
    )
    fetched = 0
    for url in all_urls:
        if url not in url_to_text:
            url_to_text[url] = _fetch_10k_text(url)
            fetched += 1
            if fetched % 10 == 0:
                logger.info("10-K context: fetched %d/%d filings", fetched, len(all_urls))
    logger.info("10-K context: fetched %d filings (%d had text)", fetched,
                sum(1 for v in url_to_text.values() if v is not None))
    analyzed = 0
    for ticker in sorted_tickers:
        hits_list = ticker_hits.get(ticker, [])
        if not hits_list:
            continue
        keyword_contexts: dict[str, str] = {}
        self_claim_count = 0
        risk_factor_count = 0
        mda_count = 0
        for h in hits_list:
            kw = h["keyword"]
            filing_text = url_to_text.get(h["filing_url"])
            if filing_text is None:
                continue
            if kw in keyword_contexts:
                continue
            section = _classify_keyword_section(filing_text, kw)
            keyword_contexts[kw] = section
            if section == "business":
                self_claim_count += 1
            elif section == "risk_factors":
                risk_factor_count += 1
            elif section == "mda":
                mda_count += 1
        f = flags[ticker]
        f["keyword_contexts"] = keyword_contexts
        f["self_claim_count"] = self_claim_count
        f["risk_factor_count"] = risk_factor_count
        f["mda_count"] = mda_count
        analyzed += 1
    logger.info("10-K context analysis complete: %d tickers analyzed", analyzed)
    return flags


def _score_flags(flags: dict[str, dict]) -> dict[str, dict]:
    """Compute composite discovery score for each ticker's flag record.

    Scoring formula:
        score = flag_count * WEIGHT_FLAG_COUNT
              + moat_type_count * WEIGHT_MOAT_DIVERSITY
              + hard_keyword_count * WEIGHT_HARD_KEYWORD
              + soft_keyword_count * WEIGHT_SOFT_KEYWORD
              + structural_signal_count * WEIGHT_STRUCTURAL

    Mutates and returns the flags dict with 'composite_score' added.
    """
    hard_set = set(DISCOVERY_HARD_KEYWORDS)

    for rec in flags.values():
        hard_count = sum(1 for kw in rec["keywords_matched"] if kw in hard_set)
        soft_count = len(rec["keywords_matched"]) - hard_count

        score = (
            rec["flag_count"] * DISCOVERY_WEIGHT_FLAG_COUNT
            + rec["moat_type_count"] * DISCOVERY_WEIGHT_MOAT_DIVERSITY
            + hard_count * DISCOVERY_WEIGHT_HARD_KEYWORD
            + soft_count * DISCOVERY_WEIGHT_SOFT_KEYWORD
            + len(rec["structural_signals"]) * DISCOVERY_WEIGHT_STRUCTURAL
        )

        # Phase 2: Sector bonus/penalty
        sector = rec.get("sector", "unknown")
        score += DISCOVERY_SECTOR_SCORE.get(sector, 0.0)

        # Phase 2: Market cap sweet spot
        mcap = rec.get("market_cap")
        if mcap is not None:
            if mcap < 100_000_000:
                score += DISCOVERY_MCAP_SCORES["nano"]
            elif mcap < 500_000_000:
                score += DISCOVERY_MCAP_SCORES["micro"]
            elif mcap < 2_000_000_000:
                score += DISCOVERY_MCAP_SCORES["small"]
            elif mcap < 10_000_000_000:
                score += DISCOVERY_MCAP_SCORES["mid"]
            else:
                score += DISCOVERY_MCAP_SCORES["large"]

        # Phase 2: Thesis matching bonus
        thesis_count = rec.get("thesis_match_count", 0)
        if thesis_count > 0:
            score += thesis_count * DISCOVERY_THESIS_MATCH_BONUS

        # --- Phase 3: Fundamentals adjustments ---

        # "Already discovered" penalty (ALL THREE must be true)
        analysts = rec.get("analyst_count")
        pct_from_high = rec.get("pct_from_52w_high")
        if (mcap is not None and mcap >= DISCOVERY_ALREADY_DISCOVERED_MIN_CAP
                and analysts is not None and analysts >= DISCOVERY_ALREADY_DISCOVERED_MIN_ANALYSTS
                and pct_from_high is not None and pct_from_high >= DISCOVERY_ALREADY_DISCOVERED_MAX_PCT_FROM_HIGH):
            score += DISCOVERY_ALREADY_DISCOVERED_PENALTY

        # Unfunded debt penalty (D/E > 5 AND cash runway < 4 quarters)
        de = rec.get("debt_to_equity")
        runway = rec.get("cash_runway")
        if (de is not None and de > DISCOVERY_UNFUNDED_DEBT_DE_THRESHOLD
                and runway is not None and runway < DISCOVERY_UNFUNDED_DEBT_RUNWAY_THRESHOLD):
            score += DISCOVERY_UNFUNDED_DEBT_PENALTY

        # Debt-free bonus
        if de is not None and 0 <= de <= DISCOVERY_DEBT_FREE_DE_THRESHOLD:
            score += DISCOVERY_DEBT_FREE_BONUS

        # Revenue decline penalty
        rev_growth = rec.get("revenue_growth")
        if rev_growth is not None and rev_growth < 0:
            score += DISCOVERY_REVENUE_DECLINE_PENALTY

        # Under-followed bonus
        if analysts is not None:
            if analysts == 0:
                score += DISCOVERY_ZERO_ANALYST_BONUS
            elif analysts <= 3:
                score += DISCOVERY_LOW_ANALYST_BONUS

        # --- Phase 4: Government contract validation ---
        if rec.get("gov_sole_source"):
            score += DISCOVERY_GOV_SOLE_SOURCE_BONUS
        elif rec.get("gov_single_bidder"):
            score += DISCOVERY_GOV_SINGLE_BIDDER_BONUS
        elif rec.get("gov_restricted"):
            score += DISCOVERY_GOV_RESTRICTED_BONUS
        elif rec.get("gov_contracts", 0) > 0:
            score += DISCOVERY_GOV_CONTRACT_BONUS

        # --- Phase 4b: Customer cross-validation ---
        customer_mentions = rec.get("customer_mentions", 0)
        if customer_mentions >= 3:
            score += DISCOVERY_CUSTOMER_XVAL_MULTI_BONUS
        elif customer_mentions >= 1:
            score += DISCOVERY_CUSTOMER_XVAL_BONUS

        # Phase 5: New ticker bonus
        if rec.get("is_new_ticker"):
            score += DISCOVERY_HISTORY_NEW_TICKER_BONUS

        # Phase 5b: 10-K context scoring
        self_claims = rec.get("self_claim_count", 0)
        risk_mentions = rec.get("risk_factor_count", 0)
        mda_mentions = rec.get("mda_count", 0)
        if self_claims > 0:
            score += min(self_claims, 3) * DISCOVERY_CONTEXT_SELF_CLAIM_BONUS  # Cap at 3
        if risk_mentions > 0 and self_claims == 0:
            score += DISCOVERY_CONTEXT_RISK_FACTOR_PENALTY  # Only penalize if NO self-claims
        if mda_mentions > 0:
            score += DISCOVERY_CONTEXT_MDA_BONUS

        rec["composite_score"] = score

    return flags


def _compute_diamond_score(flags: dict[str, dict]) -> dict[str, dict]:
    """Score each ticker on the 5 diamond-in-the-rough criteria.

    Unlike composite_score (keyword-heavy), diamond_score measures
    entry-point quality: how closely does this match AMPX-at-$0.61?

    Five dimensions, each 0-5, total 0-25:
      1. Crash depth (from pct_from_52w_high)
      2. Under-discovery (from analyst_count)
      3. Revenue inflection (from revenue_growth)
      4. Balance sheet (from debt_to_equity)
      5. Moat quality (from moat_type_count + hard keyword presence)
    """
    hard_set = set(DISCOVERY_HARD_KEYWORDS)

    for rec in flags.values():
        # --- 1. Crash depth ---
        pct = rec.get("pct_from_52w_high")
        if pct is None:
            d_crash = 0
        elif pct <= -90:
            d_crash = 5
        elif pct <= -75:
            d_crash = 4
        elif pct <= -60:
            d_crash = 3
        elif pct <= -40:
            d_crash = 2
        elif pct <= -20:
            d_crash = 1
        else:
            d_crash = 0

        # --- 2. Under-discovery ---
        analysts = rec.get("analyst_count")
        if analysts is None:
            d_undiscovered = 0
        elif analysts == 0:
            d_undiscovered = 5
        elif analysts == 1:
            d_undiscovered = 4
        elif analysts <= 3:
            d_undiscovered = 3
        elif analysts <= 5:
            d_undiscovered = 2
        elif analysts <= 10:
            d_undiscovered = 1
        else:
            d_undiscovered = 0

        # --- 3. Revenue inflection ---
        rev_growth = rec.get("revenue_growth")
        if rev_growth is None:
            d_growth = 0
        elif rev_growth < 0:
            d_growth = 0
        elif rev_growth <= 0.10:
            d_growth = 1
        elif rev_growth <= 0.30:
            d_growth = 2
        elif rev_growth <= 0.50:
            d_growth = 3
        elif rev_growth <= 1.00:
            d_growth = 4
        else:
            d_growth = 5

        # --- 4. Balance sheet ---
        de = rec.get("debt_to_equity")
        if de is None:
            d_balance = 0
        elif de <= 0.01:
            d_balance = 5  # essentially zero debt
        elif de <= 0.5:
            d_balance = 4
        elif de <= 2:
            d_balance = 3
        elif de <= 5:
            d_balance = 2
        elif de <= 10:
            d_balance = 1
        else:
            d_balance = 0

        # --- 5. Moat quality ---
        moat_count = rec.get("moat_type_count", 0)
        has_hard_kw = any(kw in hard_set for kw in rec.get("keywords_matched", []))
        has_gov_confirmed = bool(rec.get("gov_sole_source") or rec.get("gov_contracts", 0) > 0)

        if moat_count >= 4 and has_hard_kw and has_gov_confirmed:
            d_moat = 5
        elif moat_count >= 4 and has_hard_kw:
            d_moat = 4
        elif moat_count >= 3 or has_hard_kw:
            d_moat = 3
        elif moat_count >= 2:
            d_moat = 2
        elif moat_count >= 1:
            d_moat = 1
        else:
            d_moat = 0

        raw_diamond = d_crash + d_undiscovered + d_growth + d_balance + d_moat

        # --- NOISE FILTERS (penalize fake diamonds) ---

        # Reverse split killer: shares_outstanding_change > 0 but stock down 90%+
        # means the crash is from a RS, not a real washout. Also check via yfinance
        # data if available (shares_growth field from Phase 3 enrichment).
        # Heuristic: if crash >= 90% AND revenue < $20M AND market_cap < $50M,
        # likely a RS penny stock. Apply -10 penalty (effectively kills diamond score).
        shares_growth = rec.get("shares_growth")
        mcap = rec.get("market_cap")
        rev = rec.get("revenue")
        if (d_crash >= 5
                and (mcap is not None and mcap < 50_000_000)
                and (rev is None or rev < 20_000_000)):
            raw_diamond -= 10

        # Insider cluster selling penalty: 3+ insiders sold recently.
        # Detected by insider data from the deep-dive or enrichment.
        # For now, use a proxy: if we have insider data showing net selling.
        insider_sells = rec.get("insider_sell_count", 0)
        if insider_sells >= 3:
            raw_diamond -= 5

        # Dilution guard: if shares grew >25% in the past year, penalize.
        if shares_growth is not None and shares_growth > 0.25:
            raw_diamond -= 3

        # Current ratio distress: below 1.0 means can't cover short-term liabilities.
        cr = rec.get("current_ratio")
        if cr is not None and cr < 1.0:
            raw_diamond -= 4
            d_balance = max(0, d_balance - 2)

        # Store sub-dimensions
        rec["diamond_crash"] = d_crash
        rec["diamond_undiscovered"] = d_undiscovered
        rec["diamond_growth"] = d_growth
        rec["diamond_balance"] = d_balance
        rec["diamond_moat"] = d_moat
        rec["diamond_score"] = max(0, raw_diamond)

    return flags


def _store_history_snapshot(flags: dict[str, dict], scan_date: str) -> int:
    """Persist a snapshot of current discovery scores to scr_discovery_history.

    Stores one row per ticker with the key scoring fields for later delta
    computation. Uses INSERT OR REPLACE on (ticker, scan_date) PK.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _score_flags.
        scan_date: YYYY-MM-DD.

    Returns:
        Count of rows written.
    """
    if not flags:
        return 0

    written = 0
    with get_connection() as conn:
        for ticker, rec in flags.items():
            conn.execute(
                f"""INSERT OR REPLACE INTO {TABLE_DISCOVERY_HISTORY}
                   (ticker, scan_date, composite_score, flag_count,
                    moat_type_count, sector, gov_sole_source, customer_mentions)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ticker,
                    scan_date,
                    rec.get("composite_score", 0),
                    rec.get("flag_count", 0),
                    rec.get("moat_type_count", 0),
                    rec.get("sector", "unknown"),
                    1 if rec.get("gov_sole_source") else 0,
                    rec.get("customer_mentions", 0),
                ),
            )
            written += 1
        conn.commit()

    logger.info("Stored %d history snapshot rows (scan_date=%s)", written, scan_date)
    return written


def _compute_deltas(flags: dict[str, dict], scan_date: str) -> dict:
    """Compute week-over-week score deltas and detect new/lost tickers.

    1. Get the previous scan date from scr_discovery_history (most recent before current).
    2. For each ticker in current flags, look up its previous score.
    3. Compute delta = current_score - previous_score.
    4. Identify NEW tickers (in current but not in previous).
    5. Identify LOST tickers (in previous top 200 but not in current).
    6. Store delta in flags[ticker]["score_delta"].

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _score_flags.
        scan_date: YYYY-MM-DD current scan date.

    Returns:
        Summary dict: {"new_tickers": [...], "biggest_gainers": [...],
                       "biggest_losers": [...], "dropped_out": [...],
                       "prev_scan_date": str|None}
    """
    result = {
        "new_tickers": [],
        "biggest_gainers": [],
        "biggest_losers": [],
        "dropped_out": [],
        "prev_scan_date": None,
    }

    with get_connection() as conn:
        # Find the most recent scan_date BEFORE the current one
        row = conn.execute(
            f"""SELECT MAX(scan_date) AS prev_date
                FROM {TABLE_DISCOVERY_HISTORY}
                WHERE scan_date < ?""",
            (scan_date,),
        ).fetchone()

        prev_date = row["prev_date"] if row else None
        if not prev_date:
            # First scan — mark all tickers as new
            for ticker, rec in flags.items():
                rec["is_new_ticker"] = True
                rec["score_delta"] = 0.0
                result["new_tickers"].append(ticker)
            return result

        result["prev_scan_date"] = prev_date

        # Load previous scan data
        prev_rows = conn.execute(
            f"""SELECT ticker, composite_score, sector
                FROM {TABLE_DISCOVERY_HISTORY}
                WHERE scan_date = ?""",
            (prev_date,),
        ).fetchall()

    prev_scores: dict[str, float] = {}
    prev_sectors: dict[str, str] = {}
    for r in prev_rows:
        prev_scores[r["ticker"]] = r["composite_score"] or 0.0
        prev_sectors[r["ticker"]] = r["sector"] or "unknown"

    # Compute deltas for current tickers
    current_tickers = set(flags.keys())


    for ticker, rec in flags.items():
        current_score = rec.get("composite_score", 0.0)
        if ticker in prev_scores:
            prev_score = prev_scores[ticker]
            delta = current_score - prev_score
            rec["score_delta"] = delta
            rec["is_new_ticker"] = False
        else:
            rec["score_delta"] = 0.0
            rec["is_new_ticker"] = True
            result["new_tickers"].append(ticker)

    # Biggest gainers (score increased >= threshold)
    deltas_list = []
    for ticker, rec in flags.items():
        if ticker in prev_scores:
            delta = rec.get("score_delta", 0.0)
            deltas_list.append((ticker, delta, prev_scores[ticker], rec.get("composite_score", 0.0)))

    # Sort by delta descending for gainers
    deltas_list.sort(key=lambda x: x[1], reverse=True)
    result["biggest_gainers"] = [
        {"ticker": t, "delta": d, "prev_score": ps, "current_score": cs}
        for t, d, ps, cs in deltas_list
        if d >= DISCOVERY_HISTORY_MIN_SCORE_DELTA
    ][:20]

    # Sort by delta ascending for losers
    deltas_list.sort(key=lambda x: x[1])
    result["biggest_losers"] = [
        {"ticker": t, "delta": d, "prev_score": ps, "current_score": cs}
        for t, d, ps, cs in deltas_list
        if d <= -DISCOVERY_HISTORY_MIN_SCORE_DELTA
    ][:20]

    # Dropped out: in previous top 200 but not in current
    prev_top200 = sorted(prev_scores.keys(), key=lambda t: prev_scores[t], reverse=True)[:200]
    for ticker in prev_top200:
        if ticker not in current_tickers:
            result["dropped_out"].append({
                "ticker": ticker,
                "last_score": prev_scores[ticker],
                "sector": prev_sectors.get(ticker, "unknown"),
            })

    return result


def _format_delta_report(deltas: dict, scan_date: str) -> str:
    """Format a formatted delta report.

    Args:
        deltas: Summary dict from _compute_deltas.
        scan_date: YYYY-MM-DD.

    Returns:
        Formatted report string.
    """
    lines = [f"WEEKLY DISCOVERY DELTAS — {scan_date}", ""]

    if deltas["prev_scan_date"] is None:
        lines.append("First scan — no historical data for comparison. Deltas will appear next week.")
        return "\n".join(lines)

    lines.append(f"Compared to previous scan: {deltas['prev_scan_date']}")
    lines.append("")

    # NEW THIS WEEK
    new_tickers = deltas.get("new_tickers", [])
    if new_tickers:
        lines.append(f"NEW THIS WEEK ({len(new_tickers)} first appearances):")
        for ticker in new_tickers[:15]:
            lines.append(f"  {ticker}")
        if len(new_tickers) > 15:
            lines.append(f"  +{len(new_tickers) - 15} more")
        lines.append("")

    # BIGGEST GAINERS
    gainers = deltas.get("biggest_gainers", [])
    if gainers:
        lines.append("BIGGEST GAINERS (score increased):")
        for g in gainers[:10]:
            lines.append(
                f"  {g['ticker']:<6} | +{g['delta']:.1f} (was {g['prev_score']:.1f}, now {g['current_score']:.1f})"
            )
        lines.append("")

    # BIGGEST LOSERS
    losers = deltas.get("biggest_losers", [])
    if losers:
        lines.append("BIGGEST LOSERS (score decreased):")
        for loser in losers[:10]:
            lines.append(
                f"  {loser['ticker']:<6} | {loser['delta']:.1f} (was {loser['prev_score']:.1f}, now {loser['current_score']:.1f})"
            )
        lines.append("")

    # DROPPED OUT
    dropped = deltas.get("dropped_out", [])
    if dropped:
        lines.append("DROPPED OUT (was in top 200, now gone):")
        for d in dropped[:10]:
            lines.append(
                f"  {d['ticker']:<6} | last score: {d['last_score']:.1f} | {d['sector']}"
            )
        if len(dropped) > 10:
            lines.append(f"  +{len(dropped) - 10} more")
        lines.append("")

    if not new_tickers and not gainers and not losers and not dropped:
        lines.append("No significant changes from previous scan.")

    return "\n".join(lines)


def _store_discovery_flags(flags: dict[str, dict], scan_date: str) -> int:
    """Persist per-ticker discovery flags to scr_discovery_flags.

    Uses INSERT OR REPLACE on (ticker, scan_date) PK. JSON-encodes list fields.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _score_flags.
        scan_date: YYYY-MM-DD.

    Returns:
        Count of rows written.
    """
    if not flags:
        return 0

    _ensure_phase2_columns()

    written = 0
    with get_connection() as conn:
        for ticker, rec in flags.items():
            conn.execute(
                """INSERT OR REPLACE INTO scr_discovery_flags
                   (ticker, scan_date, company_name, cik,
                    layer_2a, layer_2b, layer_2c, flag_count,
                    keywords_matched, structural_signals,
                    moat_types, moat_type_count, composite_score,
                    sector, market_cap, analyst_count, thesis_match_count,
                    revenue, revenue_growth, debt_to_equity, cash_runway,
                    pct_from_52w_high,
                    gov_contracts, gov_total_value, gov_sole_source, gov_single_bidder,
                    customer_mentions, mentioning_companies,
                    self_claim_count, risk_factor_count, keyword_contexts,
                    diamond_score, diamond_crash, diamond_undiscovered,
                    diamond_growth, diamond_balance, diamond_moat,
                    fresh_crash, pct_1m_change)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ticker,
                    scan_date,
                    rec.get("company_name", ""),
                    rec.get("cik", ""),
                    rec["layer_2a"],
                    rec["layer_2b"],
                    rec["layer_2c"],
                    rec["flag_count"],
                    json.dumps(rec["keywords_matched"]),
                    json.dumps(rec["structural_signals"]),
                    json.dumps(rec["moat_types"]),
                    rec["moat_type_count"],
                    rec.get("composite_score", 0),
                    rec.get("sector", "unknown"),
                    rec.get("market_cap"),
                    rec.get("analyst_count"),
                    rec.get("thesis_match_count", 0),
                    rec.get("revenue"),
                    rec.get("revenue_growth"),
                    rec.get("debt_to_equity"),
                    rec.get("cash_runway"),
                    rec.get("pct_from_52w_high"),
                    rec.get("gov_contracts"),
                    rec.get("gov_total_value"),
                    1 if rec.get("gov_sole_source") else 0,
                    1 if rec.get("gov_single_bidder") else 0,
                    rec.get("customer_mentions", 0),
                    json.dumps(rec.get("mentioning_companies", [])),
                    rec.get("self_claim_count", 0),
                    rec.get("risk_factor_count", 0),
                    json.dumps(rec.get("keyword_contexts", {})),
                    rec.get("diamond_score", 0),
                    rec.get("diamond_crash", 0),
                    rec.get("diamond_undiscovered", 0),
                    rec.get("diamond_growth", 0),
                    rec.get("diamond_balance", 0),
                    rec.get("diamond_moat", 0),
                    1 if rec.get("fresh_crash") else 0,
                    rec.get("pct_1m_change"),
                ),
            )
            written += 1
        conn.commit()

    logger.info("Stored %d discovery flags (scan_date=%s)", written, scan_date)
    return written


def _store_moat_signals(flags: dict[str, dict], scan_date: str) -> int:
    """Persist per-(ticker, moat_type) rows to scr_moat_signals.

    One row per moat type per ticker. Evidence is the comma-joined keywords
    that mapped to that moat type.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags.
        scan_date: YYYY-MM-DD.

    Returns:
        Count of rows written.
    """
    if not flags:
        return 0

    # Build reverse map: for each ticker, which keywords contribute to each moat type
    rows_to_write: list[tuple] = []
    for ticker, rec in flags.items():
        moat_keyword_map: dict[str, list[str]] = {}
        for kw in rec["keywords_matched"]:
            mapped_types = DISCOVERY_KEYWORD_MOAT_MAP.get(kw, ["technology"])
            for mt in mapped_types:
                moat_keyword_map.setdefault(mt, []).append(kw)

        for mt in rec["moat_types"]:
            evidence_kws = moat_keyword_map.get(mt, [])
            rows_to_write.append((
                ticker,
                scan_date,
                mt,
                "text_search",
                0.5,
                ",".join(evidence_kws) if evidence_kws else "",
            ))

    written = 0
    with get_connection() as conn:
        for row in rows_to_write:
            cur = conn.execute(
                """INSERT OR REPLACE INTO scr_moat_signals
                   (ticker, scan_date, moat_type, signal_source,
                    signal_strength, evidence)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                row,
            )
            if cur.rowcount > 0:
                written += 1
        conn.commit()

    logger.info("Stored %d moat signals (scan_date=%s)", written, scan_date)
    return written


# ── Task 5: Scan Report + CSV Output ─────────────────────────────────────

MOAT_LABELS = {
    "regulatory": "Reg",
    "technology": "Tech",
    "infrastructure": "Infra",
    "network": "Net",
    "supply_chain": "Supply",
    "government": "Gov",
    "platform": "Plat",
    "switching_cost": "Switch",
    "data": "Data",
    "regulated_non_defense": "RegCiv",
    "qualified_supplier": "QualSup",
}


def _assign_ranks(flags: dict[str, dict]) -> None:
    """Sort flags by score descending and assign rank field in-place."""
    sorted_tickers = sorted(
        flags.keys(), key=lambda t: flags[t].get("composite_score", 0), reverse=True
    )
    for rank, ticker in enumerate(sorted_tickers, 1):
        flags[ticker]["rank"] = rank


def _format_scan_report(
    flags: dict[str, dict], scan_date: str, top_n: int = 25,
    header: str | None = None,
) -> str:
    """Format discovery flags into a text report for logging and display.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _score_flags.
        scan_date: YYYY-MM-DD.
        top_n: Number of top results to show inline.
        header: Optional header line (e.g. "PHASE 2 — pre-fundamentals").

    Returns:
        Formatted report string.
    """
    sorted_tickers = sorted(
        flags.keys(), key=lambda t: flags[t].get("composite_score", 0), reverse=True
    )
    total = len(sorted_tickers)

    title = header or "MULTIBAGGER DISCOVERY SCAN"
    lines = [
        f"{title} — {scan_date}",
        f"Scanned ALL SEC 10-K filings | {total} tickers flagged",
        "",
    ]

    for i, ticker in enumerate(sorted_tickers[:top_n], 1):
        rec = flags[ticker]
        name = (rec.get("company_name") or "")[:30]
        sector = rec.get("sector", "unknown")

        # Market cap display
        mcap = rec.get("market_cap")
        if mcap is not None:
            if mcap >= 1_000_000_000:
                mcap_str = f"${mcap / 1_000_000_000:.1f}B"
            elif mcap >= 1_000_000:
                mcap_str = f"${mcap / 1_000_000:.0f}M"
            else:
                mcap_str = f"${mcap / 1_000:.0f}K"
        else:
            mcap_str = "—"

        # Moat labels
        moat_types = rec.get("moat_types", [])
        if isinstance(moat_types, str):
            moat_types = json.loads(moat_types) if moat_types.startswith("[") else moat_types.split(",")
        moat_short = [MOAT_LABELS.get(m.strip(), m.strip()) for m in moat_types if m.strip()]
        moat_str = "+".join(moat_short) if moat_short else "None"

        # Layer tags
        layer_tags = []
        if rec.get("layer_2a"):
            layer_tags.append("2A")
        if rec.get("layer_2b"):
            layer_tags.append("2B")
        if rec.get("layer_2c"):
            layer_tags.append("2C")
        layer_str = "+".join(layer_tags) if layer_tags else "—"

        # Gov contract indicator
        gov_tag = ""
        if rec.get("gov_sole_source"):
            gov_tag = " | SOLE SRC \u2713"
        elif rec.get("gov_single_bidder"):
            gov_tag = " | GOV 1BID \u2713"
        elif rec.get("gov_contracts", 0) > 0:
            gov_tag = " | GOV \u2713"

        # Customer cross-validation indicator
        cust_tag = ""
        cust_mentions = rec.get("customer_mentions", 0)
        if cust_mentions >= 3:
            cust_tag = f" | CUST {cust_mentions}+"
        elif cust_mentions >= 1:
            cust_tag = " | CUST \u2713"

        # 10-K self-claim indicator
        self_tag = ""
        if rec.get("self_claim_count", 0) > 0:
            self_tag = " | SELF \u2713"

        score = rec.get("composite_score", 0)
        lines.append(
            f"{i:>2}. {ticker:<6} | {sector:<13} | {mcap_str:<7} | {name:<30} | Moat: {moat_str} | {layer_str} | Score: {score:.1f}{gov_tag}{cust_tag}{self_tag}"
        )

    remaining = total - top_n
    if remaining > 0:
        lines.append("")
        lines.append(f"+{remaining} more in CSV")

    # Fresh crashes section — tickers in top_n with fresh_crash=True
    fresh_crashes = [
        (t, flags[t]) for t in sorted_tickers[:top_n]
        if flags[t].get("fresh_crash")
    ]
    if fresh_crashes:
        lines.append("")
        lines.append("--- FRESH CRASHES (>30% drop in 1 month) ---")
        for ticker, rec in fresh_crashes:
            pct_1m = rec.get("pct_1m_change", 0)
            d_score = rec.get("diamond_score", 0)
            lines.append(
                f"  {ticker:<6} | 1m: {pct_1m:+.1f}% | Diamond: {d_score}/25 | Score: {rec.get('composite_score', 0):.1f}"
            )

    return "\n".join(lines)


def _write_csv(flags: dict[str, dict], scan_date: str) -> Path:
    """Write discovery flags to a dated CSV file.

    Args:
        flags: Dict keyed by ticker from _build_discovery_flags + _score_flags.
        scan_date: YYYY-MM-DD.

    Returns:
        Path to the written CSV file.
    """
    DISCOVERY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = DISCOVERY_OUTPUT_DIR / f"{scan_date}.csv"

    sorted_tickers = sorted(
        flags.keys(), key=lambda t: flags[t].get("composite_score", 0), reverse=True
    )

    fieldnames = [
        "rank", "ticker", "company_name", "sector", "composite_score",
        "diamond_score", "diamond_crash", "diamond_undiscovered",
        "diamond_growth", "diamond_balance", "diamond_moat",
        "fresh_crash", "pct_1m_change",
        "flag_count",
        "layer_2a", "layer_2b", "layer_2c", "moat_types", "moat_type_count",
        "keywords_matched", "structural_signals",
        "market_cap", "analyst_count", "thesis_match_count",
        "revenue", "revenue_growth", "debt_to_equity", "cash_runway",
        "pct_from_52w_high",
        "gov_contracts", "gov_total_value", "gov_sole_source", "gov_single_bidder",
        "customer_mentions", "mentioning_companies",
        "self_claim_count", "risk_factor_count", "keyword_contexts",
    ]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rank, ticker in enumerate(sorted_tickers, 1):
            rec = flags[ticker]

            # Normalize moat_types to comma-separated string
            moat_types = rec.get("moat_types", [])
            if isinstance(moat_types, list):
                moat_str = ",".join(moat_types)
            else:
                moat_str = str(moat_types)

            # Normalize keywords_matched to pipe-separated string
            kw = rec.get("keywords_matched", [])
            if isinstance(kw, list):
                kw_str = "|".join(kw)
            else:
                kw_str = str(kw)

            # Normalize structural_signals to pipe-separated string
            ss = rec.get("structural_signals", [])
            if isinstance(ss, list):
                ss_str = "|".join(ss)
            else:
                ss_str = str(ss)

            writer.writerow({
                "rank": rank,
                "ticker": ticker,
                "company_name": rec.get("company_name", ""),
                "sector": rec.get("sector", "unknown"),
                "composite_score": rec.get("composite_score", 0),
                "diamond_score": rec.get("diamond_score", 0),
                "diamond_crash": rec.get("diamond_crash", 0),
                "diamond_undiscovered": rec.get("diamond_undiscovered", 0),
                "diamond_growth": rec.get("diamond_growth", 0),
                "diamond_balance": rec.get("diamond_balance", 0),
                "diamond_moat": rec.get("diamond_moat", 0),
                "fresh_crash": 1 if rec.get("fresh_crash") else 0,
                "pct_1m_change": rec.get("pct_1m_change", ""),
                "flag_count": rec.get("flag_count", 0),
                "layer_2a": rec.get("layer_2a", 0),
                "layer_2b": rec.get("layer_2b", 0),
                "layer_2c": rec.get("layer_2c", 0),
                "moat_types": moat_str,
                "moat_type_count": rec.get("moat_type_count", 0),
                "keywords_matched": kw_str,
                "structural_signals": ss_str,
                "market_cap": rec.get("market_cap", ""),
                "analyst_count": rec.get("analyst_count", ""),
                "thesis_match_count": rec.get("thesis_match_count", 0),
                "revenue": rec.get("revenue", ""),
                "revenue_growth": rec.get("revenue_growth", ""),
                "debt_to_equity": rec.get("debt_to_equity", ""),
                "cash_runway": rec.get("cash_runway", ""),
                "pct_from_52w_high": rec.get("pct_from_52w_high", ""),
                "gov_contracts": rec.get("gov_contracts", ""),
                "gov_total_value": rec.get("gov_total_value", ""),
                "gov_sole_source": 1 if rec.get("gov_sole_source") else 0,
                "gov_single_bidder": 1 if rec.get("gov_single_bidder") else 0,
                "customer_mentions": rec.get("customer_mentions", 0),
                "mentioning_companies": "|".join(rec.get("mentioning_companies", [])) if isinstance(rec.get("mentioning_companies"), list) else rec.get("mentioning_companies", ""),
                "self_claim_count": rec.get("self_claim_count", 0),
                "risk_factor_count": rec.get("risk_factor_count", 0),
                "keyword_contexts": json.dumps(rec.get("keyword_contexts", {})),
            })

    logger.info("Wrote CSV: %s (%d rows)", csv_path, len(sorted_tickers))
    return csv_path


def _log_report(report: str) -> None:
    """Log the discovery report for monitoring."""
    logger.info("Discovery report:\n%s", report)


# ── Task 6: Orchestrator + Query Helper + CLI ────────────────────────────────


def run_discovery(dry_run: bool = False) -> dict:
    """Run the full multibagger discovery pipeline.

    Args:
        dry_run: If True, print report to stdout instead of sending report.

    Returns:
        Summary dict with scan_date, total_flagged, layer counts, csv_path.
    """
    # 1. Ensure schema + load CIK maps + Phase 2 runtime migrations
    run_migration()
    _load_cik_maps()
    _ensure_phase2_columns()

    # 2. Set dates
    scan_date = date.today().isoformat()
    end_date = scan_date
    start_date = (date.today() - timedelta(days=365)).isoformat()

    logger.info("Discovery scan starting: %s (range %s to %s)", scan_date, start_date, end_date)

    # 3. Run text search layers
    hits_2a = search_layer_2a(start_date, end_date)
    hits_2b = search_layer_2b(start_date, end_date)
    all_text_hits = hits_2a + hits_2b

    # 4. Store text search hits
    stored = _store_text_search_hits(all_text_hits, scan_date)
    logger.info("Stored %d text search hits", stored)

    # 5. Run structural signals layer
    signals_2c = search_layer_2c(scan_date)

    # 6. Build discovery flags (union all layers)
    flags = _build_discovery_flags(all_text_hits, signals_2c, scan_date)

    # 7. Filter out kill list tickers
    with get_connection() as conn:
        kill_rows = conn.execute(f"SELECT ticker FROM {TABLE_KILL_LIST}").fetchall()
    kill_set = {r["ticker"] for r in kill_rows}
    before_kill = len(flags)
    flags = {t: f for t, f in flags.items() if t not in kill_set}
    logger.info("Kill list filtered: %d -> %d tickers", before_kill, len(flags))

    # 8. Enrich with sector, market cap, analyst count, thesis matches
    flags = _enrich_flags(flags)

    # 8b. Phase 2 score (keywords + moats + sector + market cap from DB)
    flags = _score_flags(flags)

    # 8c. Send Phase 2 intermediate report (before yfinance enrichment)
    _assign_ranks(flags)
    phase2_report = _format_scan_report(
        flags, scan_date, header="PHASE 2 — Keyword + Sector Scoring (pre-fundamentals)"
    )
    if dry_run:
        print(phase2_report)
        print("\n" + "=" * 60 + "\n")
    else:
        _log_report(phase2_report)

    # 8d. Enrich top candidates with yfinance fundamentals (Phase 3)
    flags = _enrich_top_candidates(flags, top_n=200)

    # 8e. Phase 4: USAspending government contract validation
    flags = _enrich_gov_contracts(flags, top_n=100)

    # 8f. Phase 4b: Customer 10-K cross-validation
    flags = _enrich_customer_mentions(flags, top_n=75)

    # 8g. Phase 5b: 10-K context analysis (keyword section classification)
    flags = _enrich_keyword_context(flags, scan_date, top_n=50)

    # 9. Re-score with fundamentals + gov contract + customer xval + context bonuses
    flags = _score_flags(flags)

    # 9b. Phase 5: Historical tracking — compute deltas and mark new tickers
    deltas = _compute_deltas(flags, scan_date)

    # 9c. Re-score to apply new ticker bonus (is_new_ticker set by _compute_deltas)
    flags = _score_flags(flags)

    # 9d. Compute diamond score (entry-point quality, separate from composite)
    flags = _compute_diamond_score(flags)

    # 9e. Store history snapshot (after final scoring)
    _store_history_snapshot(flags, scan_date)

    # 10. Sort by score descending, assign ranks
    _assign_ranks(flags)

    # 11. Store flags + moat signals
    _store_discovery_flags(flags, scan_date)
    _store_moat_signals(flags, scan_date)

    # 12. Write CSV
    csv_path = _write_csv(flags, scan_date)

    # 13. Send Phase 3 final report
    report = _format_scan_report(
        flags, scan_date, header="PHASE 3 — Final (with fundamentals scoring)"
    )
    if dry_run:
        print(report)
    else:
        _log_report(report)

    # 14. Send Phase 5 delta report
    delta_report = _format_delta_report(deltas, scan_date)
    if dry_run:
        print("\n" + "=" * 60 + "\n")
        print(delta_report)
    else:
        _log_report(delta_report)

    # Phase 6: Auto deep-dive on top candidates
    try:
        from .deep_dive import run_deep_dives
        deep_dive_result = run_deep_dives(top_n=50, dry_run=dry_run)
        logger.info("Phase 6 deep-dive complete: %s",
                     {k: v for k, v in deep_dive_result.items() if k != "results"})
    except Exception as e:
        logger.error("Phase 6 deep-dive failed (non-fatal): %s", e)

    # Count layers
    layer_2a_count = sum(1 for r in flags.values() if r.get("layer_2a"))
    layer_2b_count = sum(1 for r in flags.values() if r.get("layer_2b"))
    layer_2c_count = sum(1 for r in flags.values() if r.get("layer_2c"))

    summary = {
        "scan_date": scan_date,
        "total_flagged": len(flags),
        "layer_2a_count": layer_2a_count,
        "layer_2b_count": layer_2b_count,
        "layer_2c_count": layer_2c_count,
        "csv_path": str(csv_path),
    }

    logger.info("Discovery scan complete: %s", summary)
    return summary


def get_latest_results(top_n: int = 50) -> list[dict]:
    """Query scr_discovery_flags for the latest scan's top results.

    Args:
        top_n: Max rows to return.

    Returns:
        List of dicts with flag data, ordered by score descending.
    """
    _ensure_phase2_columns()
    with get_connection() as conn:
        # Find latest scan_date
        row = conn.execute(
            f"SELECT MAX(scan_date) AS latest FROM {TABLE_DISCOVERY_FLAGS}"
        ).fetchone()
        if not row or not row["latest"]:
            return []

        latest = row["latest"]
        rows = conn.execute(
            f"""SELECT ticker, scan_date, company_name, cik,
                       layer_2a, layer_2b, layer_2c, flag_count,
                       keywords_matched, structural_signals,
                       moat_types, moat_type_count, composite_score, rank,
                       diamond_score
                FROM {TABLE_DISCOVERY_FLAGS}
                WHERE scan_date = ?
                ORDER BY composite_score DESC
                LIMIT ?""",
            (latest, top_n),
        ).fetchall()

    return [dict(r) for r in rows]


def _get_moat_rows_for_merge() -> list[dict]:
    """Fetch latest discovery flags with rank for combined merge."""
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT ticker, composite_score, rank
                FROM {TABLE_DISCOVERY_FLAGS}
                WHERE scan_date = (SELECT MAX(scan_date) FROM {TABLE_DISCOVERY_FLAGS})
                ORDER BY composite_score DESC"""
        ).fetchall()
    result = []
    for i, r in enumerate(rows, 1):
        result.append({
            "ticker": r["ticker"],
            "composite_score": r["composite_score"] or 0,
            "rank": r["rank"] if r["rank"] else i,
        })
    return result


def _get_forward_rows_for_merge() -> list[dict]:
    """Fetch latest forward moat scores with rank for combined merge."""
    from .config import TABLE_FORWARD_MOAT_SCORES
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT ticker, forward_score
                FROM {TABLE_FORWARD_MOAT_SCORES}
                WHERE scan_date = (SELECT MAX(scan_date) FROM {TABLE_FORWARD_MOAT_SCORES})
                ORDER BY forward_score DESC"""
        ).fetchall()
    result = []
    for i, r in enumerate(rows, 1):
        result.append({
            "ticker": r["ticker"],
            "forward_score": r["forward_score"] or 0,
            "rank": i,
        })
    return result


def main():
    """CLI entry point for the discovery pipeline."""
    parser = argparse.ArgumentParser(
        description="Multibagger Discovery Pipeline — SEC 10-K full-text search + structural signals"
    )
    parser.add_argument("--run", action="store_true", help="Full scan with scan report")
    parser.add_argument("--dry-run", action="store_true", help="Full scan without report output (prints to stdout)")
    parser.add_argument("--latest", action="store_true", help="Show latest results table")
    parser.add_argument("--top", type=int, default=50, help="Number of results (default 50)")
    parser.add_argument("--ticker", type=str, help="Show discovery profile for one ticker")
    parser.add_argument("--history", type=str, metavar="TICKER", help="Show score history for a ticker")
    parser.add_argument("--diamonds", action="store_true", help="Show diamond leaderboard (sorted by entry-point quality)")
    parser.add_argument("--combined", action="store_true", help="Run forward moat scan after discovery and produce combined CSV")

    args = parser.parse_args()

    if args.run:
        summary = run_discovery(dry_run=False)
        csv_name = Path(summary['csv_path']).name if summary.get('csv_path') else ''
        print(f"Scan complete: {summary['total_flagged']} flagged, CSV: {csv_name}")
        if args.combined:
            from .forward_moat import run_forward_scan, merge_combined_csv
            run_forward_scan(dry_run=False)
            # Build moat_rows from discovery flags
            moat_rows = _get_moat_rows_for_merge()
            forward_rows = _get_forward_rows_for_merge()
            if moat_rows and forward_rows:
                combined_path = merge_combined_csv(moat_rows, forward_rows)
                print(f"Combined CSV: {combined_path.name}")
    elif args.dry_run:
        summary = run_discovery(dry_run=True)
        csv_name = Path(summary['csv_path']).name if summary.get('csv_path') else ''
        print(f"\nScan complete: {summary['total_flagged']} flagged, CSV: {csv_name}")
        if args.combined:
            from .forward_moat import run_forward_scan, merge_combined_csv
            run_forward_scan(dry_run=True)
            moat_rows = _get_moat_rows_for_merge()
            forward_rows = _get_forward_rows_for_merge()
            if moat_rows and forward_rows:
                combined_path = merge_combined_csv(moat_rows, forward_rows)
                print(f"Combined CSV: {combined_path.name}")
    elif args.latest:
        run_migration()
        results = get_latest_results(top_n=args.top)
        if not results:
            print("No discovery results found.")
            return
        print(f"Latest discovery scan — {results[0]['scan_date']} — top {len(results)}:\n")
        for r in results:
            moat_types = r.get("moat_types", "[]")
            if isinstance(moat_types, str) and moat_types.startswith("["):
                moat_types = json.loads(moat_types)
            elif isinstance(moat_types, str):
                moat_types = moat_types.split(",")
            moat_short = [MOAT_LABELS.get(m.strip(), m.strip()) for m in moat_types if m.strip()]
            d_score = r.get("diamond_score", 0) or 0
            print(
                f"  {r.get('rank') or '?':>3}. {r['ticker']:<6} | "
                f"{(r.get('company_name') or '')[:30]:<30} | "
                f"Score: {r['composite_score']:.1f} | "
                f"Diamond: {d_score}/25 | "
                f"Moat: {'+'.join(moat_short) or 'None'}"
            )
    elif args.history:
        run_migration()
        ticker = args.history.upper()
        with get_connection() as conn:
            rows = conn.execute(
                f"""SELECT scan_date, composite_score, flag_count,
                           moat_type_count, sector, gov_sole_source, customer_mentions
                    FROM {TABLE_DISCOVERY_HISTORY}
                    WHERE ticker = ?
                    ORDER BY scan_date ASC""",
                (ticker,),
            ).fetchall()

        if not rows:
            print(f"No score history for {ticker}")
            return

        print(f"Score history for {ticker}:")
        prev_score = None
        for r in rows:
            score = r["composite_score"] or 0.0
            gov_str = "yes" if r["gov_sole_source"] else "no"
            delta_str = ""
            if prev_score is not None:
                delta = score - prev_score
                delta_str = f" ({delta:+.1f})"
            print(
                f"  {r['scan_date']}: {score:.1f}{delta_str} "
                f"(flags={r['flag_count']}, moats={r['moat_type_count']}, "
                f"gov={gov_str}, cust={r['customer_mentions']})"
            )
            prev_score = score
    elif args.diamonds:
        run_migration()
        _ensure_phase2_columns()
        top_n = args.top
        with get_connection() as conn:
            row = conn.execute(
                f"SELECT MAX(scan_date) AS latest FROM {TABLE_DISCOVERY_FLAGS}"
            ).fetchone()
            if not row or not row["latest"]:
                print("No discovery results found.")
                return
            latest = row["latest"]
            rows = conn.execute(
                f"""SELECT ticker, company_name, sector, composite_score,
                           diamond_score, diamond_crash, diamond_undiscovered,
                           diamond_growth, diamond_balance, diamond_moat,
                           fresh_crash
                    FROM {TABLE_DISCOVERY_FLAGS}
                    WHERE scan_date = ?
                    ORDER BY diamond_score DESC, composite_score DESC
                    LIMIT ?""",
                (latest, top_n),
            ).fetchall()

        if not rows:
            print("No discovery results found.")
            return

        print(f"Diamond Leaderboard ({latest}) - sorted by entry-point quality:")
        print(f"{'#':>2} {'Ticker':<6}  {'Diamond':>7}  {'Crash':>5}  {'Undiscov':>8}  {'Growth':>6}  {'Balance':>7}  {'Moat':>4}  |  {'Composite':>9}  {'Sector':<13}  {'Fresh?'}")
        print("-" * 105)
        for i, r in enumerate(rows, 1):
            d_score = r["diamond_score"] or 0
            fresh_tag = "CRASH" if r["fresh_crash"] else ""
            print(
                f"{i:>2} {r['ticker']:<6}  {d_score:>3}/25    {r['diamond_crash'] or 0:>3}      {r['diamond_undiscovered'] or 0:>3}       {r['diamond_growth'] or 0:>3}      {r['diamond_balance'] or 0:>3}     {r['diamond_moat'] or 0:>3}  |  {r['composite_score'] or 0:>8.1f}  {(r['sector'] or 'unknown'):<13}  {fresh_tag}"
            )
    elif args.ticker:
        run_migration()
        ticker = args.ticker.upper()
        with get_connection() as conn:
            flag = conn.execute(
                f"""SELECT * FROM {TABLE_DISCOVERY_FLAGS}
                    WHERE ticker = ?
                    ORDER BY scan_date DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
            hits = conn.execute(
                f"""SELECT keyword, keyword_layer, moat_type, filing_date, relevance_score
                    FROM {TABLE_TEXT_SEARCH_HITS}
                    WHERE ticker = ?
                    ORDER BY scan_date DESC, relevance_score DESC""",
                (ticker,),
            ).fetchall()

        if not flag and not hits:
            print(f"No discovery data for {ticker}")
            return

        if flag:
            print(f"Discovery profile for {ticker} (scan {flag['scan_date']}):")
            print(f"  Company:    {flag['company_name']}")
            print(f"  Score:      {flag['composite_score']:.1f}")
            print(f"  Layers:     2A={flag['layer_2a']} 2B={flag['layer_2b']} 2C={flag['layer_2c']}")
            print(f"  Flag count: {flag['flag_count']}")
            kw = flag["keywords_matched"]
            if isinstance(kw, str) and kw.startswith("["):
                kw = json.loads(kw)
            print(f"  Keywords:   {kw}")
            mt = flag["moat_types"]
            if isinstance(mt, str) and mt.startswith("["):
                mt = json.loads(mt)
            print(f"  Moat types: {mt}")
        if hits:
            print(f"\n  Text search hits ({len(hits)}):")
            for h in hits:
                print(
                    f"    {h['keyword_layer'].upper()} | {h['keyword']:<30} | "
                    f"moat={h['moat_type']} | filed={h['filing_date']} | "
                    f"rel={h['relevance_score']:.1f}"
                )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
