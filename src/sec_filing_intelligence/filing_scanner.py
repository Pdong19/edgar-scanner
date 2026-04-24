"""SEC EDGAR full-text filing search for catalytic keyword themes.

Part of the SEC Filing Intelligence system. Uses the EDGAR Full-Text Search
System (EFTS) at efts.sec.gov/LATEST/search-index to find filings containing
supply-chain and catalyst keywords. EFTS returns filing metadata (CIK, date,
form type, file description) but not text snippets -- the file_description
field from the filing index is used as context instead.

Only stores results for tickers in our scr_universe (active, non-killed). Filings
where the company IS the filer score higher than third-party mentions.
"""

import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import requests

from .config import (
    EDGAR_FULL_TEXT_SEARCH_URL,
    EDGAR_RATE_LIMIT_RPS,
    EDGAR_REQUEST_TIMEOUT,
    EDGAR_USER_AGENT,
    FILING_KEYWORDS,
    FILING_LOOKBACK_DAYS,
    FILING_TYPES,
    TABLE_FILING_SIGNALS,
)
from .db import get_active_tickers, get_connection
from .utils import get_logger, rate_limiter

logger = get_logger("filing_scanner", "screener_filings.log")

_headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# ── CIK ↔ Ticker mapping ────────────────────────────────────────────────────

_cik_to_ticker: dict[str, str] = {}
_ticker_to_cik: dict[str, str] = {}


def _load_cik_maps() -> None:
    """Load bidirectional CIK↔ticker maps from SEC company_tickers.json."""
    global _cik_to_ticker, _ticker_to_cik
    if _cik_to_ticker:
        return

    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        resp = requests.get(url, headers=_headers, timeout=EDGAR_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("Failed to load SEC company tickers: %s", e)
        return

    for entry in data.values():
        ticker = entry.get("ticker", "").upper()
        cik = str(entry.get("cik_str", "")).zfill(10)
        if ticker and cik:
            _cik_to_ticker[cik] = ticker
            _ticker_to_cik[ticker] = cik

    logger.info("Loaded %d CIK↔ticker mappings", len(_cik_to_ticker))


def _cik_to_ticker_lookup(cik_raw: str) -> str | None:
    """Convert a CIK string (any padding) to a ticker symbol."""
    _load_cik_maps()
    return _cik_to_ticker.get(cik_raw.zfill(10))


def _extract_ticker_from_display(display_name: str) -> str | None:
    """Extract ticker from EFTS display_names like 'Company Name  (TICK)  (CIK ...)'."""
    m = re.search(r"\(([A-Z]{1,5})\)", display_name)
    return m.group(1) if m else None


# ── EFTS search ──────────────────────────────────────────────────────────────


@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _efts_search(query: str, start_date: str, end_date: str,
                 forms: str = "10-K,10-Q,8-K", start_from: int = 0) -> dict | None:
    """Execute a rate-limited EFTS query. Returns raw JSON response."""
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


def _parse_hits(data: dict, keyword: str, category: str,
                universe: set[str]) -> list[dict]:
    """Parse EFTS response hits into filing signal records.

    Only returns hits for tickers in our universe.
    """
    results = []
    hits = data.get("hits", {}).get("hits", [])

    for hit in hits:
        src = hit.get("_source", {})
        ciks = src.get("ciks", [])
        display_names = src.get("display_names", [])
        filing_date = src.get("file_date")
        form_type = src.get("form") or src.get("root_forms", [""])[0]
        file_desc = src.get("file_description", "")
        accession = src.get("adsh", "")

        # Resolve ticker from display_names or CIK
        ticker = None
        filing_cik = ciks[0] if ciks else None

        for dn in display_names:
            t = _extract_ticker_from_display(dn)
            if t and t in universe:
                ticker = t
                break

        if not ticker and filing_cik:
            t = _cik_to_ticker_lookup(filing_cik)
            if t and t in universe:
                ticker = t

        if not ticker:
            continue

        # Relevance scoring: own filing scores higher than third-party mention.
        # If the filing CIK matches the ticker's CIK, it's the company's own filing.
        own_cik = _ticker_to_cik.get(ticker, "").zfill(10)
        is_own_filing = filing_cik and filing_cik.zfill(10) == own_cik

        # Own 10-K/10-Q = highest relevance, own 8-K = high, third-party = lower
        if is_own_filing and form_type in ("10-K", "10-Q"):
            relevance = 1.0
        elif is_own_filing:
            relevance = 0.8
        else:
            relevance = 0.4

        # Build filing URL from accession number
        filing_url = ""
        if accession and filing_cik:
            cik_clean = filing_cik.lstrip("0") or "0"
            acc_clean = accession.replace("-", "")
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_clean}/"
                f"{accession}-index.htm"
            )

        # Context snippet: use file_description as EFTS doesn't return text excerpts
        snippet = file_desc[:200] if file_desc else f"Matched '{keyword}' in {form_type}"

        results.append({
            "ticker": ticker,
            "filing_date": filing_date,
            "filing_type": form_type,
            "keyword_matched": keyword,
            "keyword_category": category,
            "context_snippet": snippet,
            "filing_url": filing_url,
            "relevance_score": relevance,
        })

    return results


# ── Search orchestration ─────────────────────────────────────────────────────


def search_keyword(keyword: str, category: str, universe: set[str],
                   lookback_days: int = FILING_LOOKBACK_DAYS) -> list[dict]:
    """Search EFTS for a single keyword, paginating through all results.

    Returns filing signal records for tickers in our universe.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    forms = ",".join(FILING_TYPES)

    all_results = []
    offset = 0
    max_pages = 5  # Cap at 50 results per keyword (10 per page)

    for _page in range(max_pages):
        data = _efts_search(keyword, start_date, end_date, forms, start_from=offset)
        if not data:
            break

        total = data.get("hits", {}).get("total", {})
        total_count = total.get("value", 0) if isinstance(total, dict) else total

        hits = _parse_hits(data, keyword, category, universe)
        all_results.extend(hits)

        # EFTS returns 10 hits per page by default
        fetched = len(data.get("hits", {}).get("hits", []))
        offset += fetched

        if offset >= total_count or fetched == 0:
            break

    return all_results


def search_theme(theme: str, universe: set[str],
                 lookback_days: int = FILING_LOOKBACK_DAYS) -> list[dict]:
    """Search all keywords in a theme category."""
    keywords = FILING_KEYWORDS.get(theme)
    if not keywords:
        logger.error("Unknown theme: %s. Available: %s", theme,
                     list(FILING_KEYWORDS.keys()))
        return []

    logger.info("Scanning theme '%s' (%d keywords)", theme, len(keywords))
    all_results = []

    for keyword in keywords:
        results = search_keyword(keyword, theme, universe, lookback_days)
        all_results.extend(results)
        logger.info("  '%s': %d universe hits", keyword, len(results))

    return all_results


def search_all_themes(universe: set[str],
                      lookback_days: int = FILING_LOOKBACK_DAYS) -> list[dict]:
    """Search all keyword themes."""
    all_results = []
    for theme in FILING_KEYWORDS:
        results = search_theme(theme, universe, lookback_days)
        all_results.extend(results)
    return all_results


# ── Storage ──────────────────────────────────────────────────────────────────


def store_results(results: list[dict]) -> int:
    """Store filing signal results. Returns count of new rows inserted."""
    if not results:
        return 0

    inserted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        for r in results:
            try:
                conn.execute(
                    f"""INSERT OR IGNORE INTO {TABLE_FILING_SIGNALS}
                        (ticker, filing_date, filing_type, keyword_matched,
                         keyword_category, context_snippet, filing_url,
                         relevance_score, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        r["ticker"],
                        r["filing_date"],
                        r["filing_type"],
                        r["keyword_matched"],
                        r["keyword_category"],
                        r["context_snippet"],
                        r["filing_url"],
                        r["relevance_score"],
                        now,
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    inserted += 1
            except sqlite3.Error:
                logger.exception("Failed to store filing signal for %s", r.get("ticker"))
        conn.commit()

    logger.info("Stored %d new filing signals (of %d total)", inserted, len(results))
    return inserted


# ── Query helpers ────────────────────────────────────────────────────────────


def get_recent_signals(days: int = 90, theme: str | None = None) -> list[dict]:
    """Retrieve recent filing signals from the database."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = f"""SELECT ticker, filing_date, filing_type, keyword_matched,
                       keyword_category, context_snippet, filing_url,
                       relevance_score
                FROM {TABLE_FILING_SIGNALS}
                WHERE filing_date >= ?"""
    params: list = [cutoff]

    if theme:
        query += " AND keyword_category = ?"
        params.append(theme)

    query += " ORDER BY filing_date DESC, relevance_score DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()

    return [dict(r) for r in rows]


def get_ticker_signals(ticker: str) -> list[dict]:
    """Get all filing signals for a specific ticker."""
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT filing_date, filing_type, keyword_matched,
                       keyword_category, context_snippet, relevance_score
                FROM {TABLE_FILING_SIGNALS}
                WHERE ticker = ?
                ORDER BY filing_date DESC""",
            (ticker,),
        ).fetchall()
    return [dict(r) for r in rows]


def score_filing_signals(ticker: str) -> dict:
    """Score a ticker's filing signals for checklist integration.

    Higher scores for: own filings, 10-K/10-Q, multiple themes, recent dates.
    Returns dict with score (0.0-1.0) and metadata.
    """
    cutoff = (datetime.now() - timedelta(days=FILING_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT keyword_category, relevance_score, filing_date, filing_type
                FROM {TABLE_FILING_SIGNALS}
                WHERE ticker = ? AND filing_date >= ?""",
            (ticker, cutoff),
        ).fetchall()

    if not rows:
        return {"ticker": ticker, "score": 0.0, "reason": "No filing signals",
                "hit_count": 0, "themes": []}

    # Score components:
    # 1. Best relevance score from any single hit
    best_relevance = max(r["relevance_score"] for r in rows)

    # 2. Theme diversity bonus (multiple themes = stronger signal)
    themes = list({r["keyword_category"] for r in rows})
    diversity_bonus = min(len(themes) * 0.1, 0.3)  # Cap at +0.3

    # 3. Recency bonus (hits in last 90 days score higher)
    recent_cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    recent_hits = [r for r in rows if r["filing_date"] >= recent_cutoff]
    recency_bonus = 0.1 if recent_hits else 0.0

    score = min(best_relevance + diversity_bonus + recency_bonus, 1.0)

    return {
        "ticker": ticker,
        "score": round(score, 2),
        "reason": f"{len(rows)} hits across {len(themes)} themes: {', '.join(themes)}",
        "hit_count": len(rows),
        "themes": themes,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="SEC EDGAR filing scanner — search for catalytic keywords"
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="Full scan of all keyword themes (last 12 months)"
    )
    parser.add_argument(
        "--theme", type=str,
        help=f"Scan a single theme. Choices: {', '.join(FILING_KEYWORDS.keys())}"
    )
    parser.add_argument(
        "--results", action="store_true",
        help="Print recent filing signal hits"
    )
    parser.add_argument(
        "--days", type=int, default=FILING_LOOKBACK_DAYS,
        help=f"Lookback period in days (default: {FILING_LOOKBACK_DAYS})"
    )

    args = parser.parse_args()

    if not any([args.refresh, args.theme, args.results]):
        parser.print_help()
        sys.exit(1)

    from .db import run_migration
    run_migration()

    _load_cik_maps()
    universe = set(get_active_tickers())
    logger.info("Universe: %d active tickers", len(universe))

    if args.results:
        signals = get_recent_signals(days=args.days, theme=args.theme)
        if not signals:
            print("No filing signals found")
            return

        print(f"\nFiling Signals (last {args.days} days):")
        print(f"{'Ticker':<8} {'Date':<12} {'Type':<6} {'Theme':<22} "
              f"{'Keyword':<35} {'Rel':>4}")
        print("-" * 92)
        for s in signals:
            print(
                f"{s['ticker']:<8} {s['filing_date']:<12} "
                f"{s['filing_type']:<6} {s['keyword_category']:<22} "
                f"{s['keyword_matched'][:34]:<35} {s['relevance_score']:>4.1f}"
            )
        print(f"\nTotal: {len(signals)} signals")
        return

    if args.theme:
        results = search_theme(args.theme, universe, lookback_days=args.days)
    else:
        results = search_all_themes(universe, lookback_days=args.days)

    if results:
        inserted = store_results(results)
        print("\nScan complete:")
        print(f"  Universe hits: {len(results)}")
        print(f"  New signals stored: {inserted}")
        print(f"  Unique tickers: {len({r['ticker'] for r in results})}")

        # Summary by theme
        from collections import Counter
        by_theme = Counter(r["keyword_category"] for r in results)
        print("\n  By theme:")
        for theme, count in by_theme.most_common():
            print(f"    {theme}: {count}")
    else:
        print("No matching filings found for universe tickers")


if __name__ == "__main__":
    main()
