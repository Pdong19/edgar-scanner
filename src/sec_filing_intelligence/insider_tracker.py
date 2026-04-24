"""SEC Form 4 insider transaction tracking for the SEC Filing Intelligence system.

Approach: SEC EDGAR submissions API (data.sec.gov/submissions/CIK{cik}.json) to find
Form 4 filings, then fetch each filing's XML index to parse insider transaction details.

Why this approach over alternatives:
- Submissions API: One call per company returns ALL recent filings (efficient).
- RSS feed / full-text search: Requires per-ticker searches, harder to parse, less structured.
- XBRL API: Form 4 is XML, not XBRL -- XBRL API doesn't cover ownership filings.

Rate limiting: SEC requires User-Agent with contact email, max 10 req/sec.
"""

import argparse
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests

from .config import (
    EDGAR_RATE_LIMIT_RPS,
    EDGAR_REQUEST_TIMEOUT,
    EDGAR_SUBMISSIONS_URL,
    EDGAR_USER_AGENT,
    INSIDER_CLUSTER_WINDOW_DAYS,
    INSIDER_LOOKBACK_DAYS,
    INSIDER_MIN_TRANSACTION_VALUE,
    TABLE_INSIDER_TRANSACTIONS,
)
from .db import get_active_tickers, get_connection
from .form4_parser import (
    cik_to_ticker,
    extract_accession_from_sec_url,
    is_amendment,
    parse_form4_xml,
    ticker_to_cik,
)
from .utils import get_logger, rate_limiter

logger = get_logger("insider_tracker", "screener_insiders.log")

_headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# SEC EDGAR archives base URL for fetching filing documents
_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

# C-suite titles (case-insensitive partial match)
C_SUITE_PATTERNS = [
    "chief executive",
    "chief financial",
    "chief operating",
    "chief technology",
    "president",
    "chairman",
]

# Exact abbreviation matches (checked with word boundaries)
C_SUITE_ABBREVIATIONS = {"ceo", "cfo", "coo", "cto"}

DIRECTOR_PATTERNS = ["director"]


def _ensure_schema():
    """Add is_open_market column if missing."""
    with get_connection() as conn:
        cols = {
            r[1] for r in conn.execute(
                f"PRAGMA table_info({TABLE_INSIDER_TRANSACTIONS})"
            ).fetchall()
        }
        if "is_open_market" not in cols:
            conn.execute(
                f"ALTER TABLE {TABLE_INSIDER_TRANSACTIONS} "
                "ADD COLUMN is_open_market INTEGER DEFAULT 0"
            )
            conn.commit()
            logger.info("Added is_open_market column to %s", TABLE_INSIDER_TRANSACTIONS)


@rate_limiter(EDGAR_RATE_LIMIT_RPS)
def _sec_get(url: str, accept_xml: bool = False) -> requests.Response | None:
    """Rate-limited GET to SEC EDGAR with exponential backoff on 429."""
    headers = dict(_headers)
    if accept_xml:
        headers["Accept"] = "application/xml, text/xml, */*"

    for attempt in range(4):
        try:
            resp = requests.get(url, headers=headers, timeout=EDGAR_REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("SEC 429 rate limit, backing off %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < 3:
                logger.warning("SEC request attempt %d failed: %s", attempt + 1, e)
                time.sleep(1)
            else:
                logger.exception("SEC request failed after 4 attempts for %s", url)
    return None


def _sec_json(url: str) -> dict | None:
    """Fetch JSON from SEC EDGAR."""
    resp = _sec_get(url)
    if resp is None:
        return None
    try:
        return resp.json()
    except ValueError:
        logger.exception("Failed to parse JSON from %s", url)
        return None


def _sec_xml(url: str) -> ET.Element | None:
    """Fetch and parse XML from SEC EDGAR."""
    resp = _sec_get(url, accept_xml=True)
    if resp is None:
        return None
    try:
        return ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.exception("Failed to parse XML from %s", url)
        return None


# ── CIK Lookup + XML Parsing ────────────────────────────────────────────────
#
# `parse_form4_xml`, `ticker_to_cik`, `cik_to_ticker`, plus the private XML
# helpers all live in `form4_parser` now (imported at module top). This module
# delegates to them so there is one canonical Form 4 parser used by both the
# per-ticker submissions-API path (below) and the new RSS atom poller.


def _is_c_suite(title: str | None) -> bool:
    """Check if a title represents a C-suite executive."""
    if not title:
        return False
    lower = title.lower()
    if any(p in lower for p in C_SUITE_PATTERNS):
        return True
    return any(re.search(rf'\b{abbr}\b', lower) for abbr in C_SUITE_ABBREVIATIONS)


def _is_director(title: str | None) -> bool:
    """Check if a title represents a board director."""
    if not title:
        return False
    lower = title.lower()
    return any(p in lower for p in DIRECTOR_PATTERNS)


# ── Filing Fetcher ───────────────────────────────────────────────────────────


def _get_form4_xml_url(cik: str, accession: str) -> str | None:
    """Get the URL of the actual Form 4 XML file from the filing index.

    Fetches the filing directory listing and finds the primary XML document.
    """

    acc_clean = accession.replace("-", "")
    dir_url = f"{_ARCHIVES_BASE}/{cik}/{acc_clean}/"
    resp = _sec_get(dir_url)
    if resp is None:
        return None

    # hrefs are absolute paths like /Archives/edgar/data/{cik}/{acc}/{file}.xml
    xml_files = re.findall(r'href="([^"]+\.xml)"', resp.text)
    for href in xml_files:
        basename = href.rsplit("/", 1)[-1] if "/" in href else href
        if basename.startswith("R") or basename == "FilingSummary.xml":
            continue
        # href is absolute path — prepend domain only
        if href.startswith("/"):
            return f"https://www.sec.gov{href}"
        return f"{dir_url}{href}"

    return None


def fetch_insider_transactions(ticker: str, lookback_days: int | None = None
                               ) -> list[dict]:
    """Fetch recent insider transactions for a ticker from SEC EDGAR.

    1. Look up CIK from ticker
    2. Fetch company submissions to find Form 4 filings
    3. For each Form 4, fetch and parse the XML

    Returns:
        List of insider transaction dicts.
    """
    cik = ticker_to_cik(ticker)
    if not cik:
        logger.warning("No CIK found for %s, skipping", ticker)
        return []

    days = lookback_days or INSIDER_LOOKBACK_DAYS
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Step 1: Get filing list from submissions API
    cik_padded = cik.zfill(10)
    url = f"{EDGAR_SUBMISSIONS_URL}/CIK{cik_padded}.json"
    data = _sec_json(url)
    if not data:
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])

    # Collect Form 4 filings within lookback
    form4_filings = []
    for i, form in enumerate(forms):
        if form not in ("4", "4/A"):
            continue
        if i >= len(dates) or dates[i] < cutoff:
            continue
        if i >= len(accessions):
            continue
        form4_filings.append((dates[i], accessions[i]))

    logger.info(
        "Found %d Form 4 filings for %s (CIK %s) since %s",
        len(form4_filings), ticker, cik, cutoff
    )

    # Step 2: Parse each Form 4 XML
    all_transactions = []
    for filing_date, accession in form4_filings:
        xml_url = _get_form4_xml_url(cik, accession)
        if not xml_url:
            logger.warning("Could not find XML URL for %s accession %s", ticker, accession)
            continue

        root = _sec_xml(xml_url)
        if root is None:
            continue

        txns = parse_form4_xml(root, filing_date=filing_date, accession=accession)
        # Override the XML-extracted ticker with the authoritative universe
        # ticker (the one we used to resolve the CIK), and reconstruct sec_url
        # with the correct CIK-included archives path (the shared parser does
        # not emit sec_url; the prior inline construction omitted CIK — bug).
        for t in txns:
            t["ticker"] = ticker
            t["sec_url"] = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{t['accession_number'].replace('-', '')}/"
                f"{t['accession_number']}-index.htm"
            )
        all_transactions.extend(txns)

    logger.info("Parsed %d transactions for %s", len(all_transactions), ticker)
    return all_transactions


# ── Storage ──────────────────────────────────────────────────────────────────


def store_transactions(transactions: list[dict]) -> int:
    """Store insider transactions in scr_insider_transactions. Returns rows inserted."""
    if not transactions:
        return 0

    inserted = 0
    with get_connection() as conn:
        for txn in transactions:
            try:
                conn.execute(
                    f"""INSERT OR IGNORE INTO {TABLE_INSIDER_TRANSACTIONS}
                        (ticker, filing_date, transaction_date, insider_name, insider_title,
                         transaction_type, shares, price_per_share, total_value,
                         shares_owned_after, is_open_market, sec_url, accession_number)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        txn["ticker"],
                        txn["filing_date"],
                        txn.get("transaction_date"),
                        txn.get("insider_name"),
                        txn.get("insider_title"),
                        txn.get("transaction_type", "other"),
                        txn.get("shares"),
                        txn.get("price_per_share"),
                        txn.get("total_value"),
                        txn.get("shares_owned_after"),
                        txn.get("is_open_market", 0),
                        txn.get("sec_url"),
                        txn.get("accession_number"),
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    inserted += 1
            except sqlite3.Error:
                logger.exception("Failed to insert transaction for %s", txn.get("ticker"))
        conn.commit()

    logger.info("Stored %d new transactions (of %d total)", inserted, len(transactions))
    return inserted


# ── Scoring ──────────────────────────────────────────────────────────────────


def _get_price_context(ticker: str) -> tuple[float | None, float | None]:
    """Get 52-week low and current price in a single query (read-only)."""
    with get_connection() as conn:
        row = conn.execute(
            """SELECT
                   MIN(low) as week_52_low,
                   (SELECT close FROM price_history
                    WHERE ticker = ? ORDER BY date DESC LIMIT 1) as current_price
               FROM price_history
               WHERE ticker = ?
                 AND date >= date('now', '-252 days')""",
            (ticker, ticker),
        ).fetchone()
    if not row:
        return None, None
    return row["week_52_low"], row["current_price"]


def score_insider_activity(ticker: str) -> dict:
    """Score insider activity for checklist criterion #9.

    Scoring:
        CEO/CFO open-market purchase within 20% of 52-week low = 1.0
        Any C-suite open-market purchase = 0.8
        Director open-market purchase = 0.6
        No insider selling when stock is at lows = 0.4
        Net insider selling = 0.0
        Multiple insiders buying in same quarter = bonus +0.2 (cap at 1.0)
    """
    cutoff = (
        datetime.now() - timedelta(days=INSIDER_CLUSTER_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")

    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT insider_name, insider_title, transaction_type,
                       shares, total_value, is_open_market, filing_date
                FROM {TABLE_INSIDER_TRANSACTIONS}
                WHERE ticker = ? AND filing_date >= ?
                  AND total_value >= ?""",
            (ticker, cutoff, INSIDER_MIN_TRANSACTION_VALUE),
        ).fetchall()

    if not rows:
        return {
            "ticker": ticker,
            "score": 0.0,
            "reason": "No insider transactions found",
            "purchases": 0,
            "sales": 0,
        }

    purchases = [r for r in rows if r["transaction_type"] == "purchase"]
    sales = [r for r in rows if r["transaction_type"] == "sale"]
    open_market_buys = [r for r in purchases if r["is_open_market"]]

    # Net selling → 0.0
    if len(sales) > 0 and len(purchases) == 0:
        return {
            "ticker": ticker,
            "score": 0.0,
            "reason": "Net insider selling",
            "purchases": len(purchases),
            "sales": len(sales),
        }

    score = 0.0
    reason_parts = []

    # Check proximity to 52-week low for purchase scoring
    low_52, current_price = _get_price_context(ticker)
    near_low = False
    if low_52 and current_price and low_52 > 0:
        pct_above_low = (current_price - low_52) / low_52
        near_low = pct_above_low <= 0.20

    for buy in open_market_buys:
        title = buy["insider_title"] or ""
        is_csuite = _is_c_suite(title)
        is_dir = _is_director(title)

        if is_csuite and near_low:
            candidate_score = 1.0
            reason_parts.append(
                f"C-suite ({buy['insider_name']}) open-market buy near 52w low"
            )
        elif is_csuite:
            candidate_score = 0.8
            reason_parts.append(
                f"C-suite ({buy['insider_name']}) open-market buy"
            )
        elif is_dir:
            candidate_score = 0.6
            reason_parts.append(
                f"Director ({buy['insider_name']}) open-market buy"
            )
        else:
            candidate_score = 0.4
            reason_parts.append(f"Insider ({buy['insider_name']}) open-market buy")

        score = max(score, candidate_score)

    # No open-market buys but also no selling at lows
    if not open_market_buys and len(sales) == 0 and near_low:
        score = 0.4
        reason_parts.append("No insider selling near 52-week low")

    # Cluster bonus: multiple distinct buyers in the quarter
    unique_buyers = {r["insider_name"] for r in open_market_buys if r["insider_name"]}
    if len(unique_buyers) >= 2:
        score = min(score + 0.2, 1.0)
        reason_parts.append(f"Cluster: {len(unique_buyers)} distinct buyers")

    return {
        "ticker": ticker,
        "score": round(score, 2),
        "reason": "; ".join(reason_parts) if reason_parts else "No significant activity",
        "purchases": len(purchases),
        "sales": len(sales),
        "open_market_buys": len(open_market_buys),
        "unique_buyers": len(unique_buyers),
        "near_52w_low": near_low,
    }


# ── Cluster Detection (used by checklist_scorer) ────────────────────────────


def detect_cluster_buying(ticker: str) -> dict:
    """Detect cluster insider buying for a ticker.

    Cluster = 3+ distinct insiders buying within INSIDER_CLUSTER_WINDOW_DAYS.
    """
    cutoff = (
        datetime.now() - timedelta(days=INSIDER_CLUSTER_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")

    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT DISTINCT insider_name, filing_date, total_value
                FROM {TABLE_INSIDER_TRANSACTIONS}
                WHERE ticker = ?
                  AND transaction_type = 'purchase'
                  AND filing_date >= ?
                  AND total_value >= ?""",
            (ticker, cutoff, INSIDER_MIN_TRANSACTION_VALUE),
        ).fetchall()

    unique_insiders = {r["insider_name"] for r in rows if r["insider_name"]}
    total_value = sum(r["total_value"] for r in rows if r["total_value"])

    return {
        "ticker": ticker,
        "cluster_detected": len(unique_insiders) >= 3,
        "unique_buyers": len(unique_insiders),
        "total_buy_value": total_value,
        "window_days": INSIDER_CLUSTER_WINDOW_DAYS,
    }


# ── Refresh / CLI Entrypoints ────────────────────────────────────────────────


def refresh_all(lookback_days: int = 90) -> dict:
    """Refresh insider transactions for all active screener universe tickers.

    Returns summary stats.
    """
    _ensure_schema()

    tickers = get_active_tickers()

    if not tickers:
        logger.warning("No active tickers in screener universe")
        return {"tickers": 0, "transactions": 0}

    logger.info("Refreshing insider data for %d tickers (last %d days)",
                len(tickers), lookback_days)

    total_txns = 0
    total_inserted = 0
    errors = 0

    for i, ticker in enumerate(tickers):
        try:
            txns = fetch_insider_transactions(ticker, lookback_days)
            inserted = store_transactions(txns)
            total_txns += len(txns)
            total_inserted += inserted
            if (i + 1) % 25 == 0:
                logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
        except Exception as e:
            logger.exception("Error processing %s", ticker)
            errors += 1

    summary = {
        "tickers": len(tickers),
        "transactions_found": total_txns,
        "transactions_inserted": total_inserted,
        "errors": errors,
    }
    logger.info("Refresh complete: %s", summary)
    return summary


def refresh_ticker(ticker: str, lookback_days: int = 90) -> list[dict]:
    """Refresh insider data for a single ticker. Returns transactions."""
    _ensure_schema()
    txns = fetch_insider_transactions(ticker, lookback_days)
    store_transactions(txns)
    return txns


def refresh_from_list(tickers: list[str], lookback_days: int = 90) -> dict:
    """Refresh insider data for an arbitrary ticker list (used by Phase 1/2 backfill driver).

    Rate-limited via the existing per-request _sec_get logic. Progress logged every
    25 tickers. Returns a summary dict the driver can record in scr_form4_poll_log.
    """
    from .db import run_migration
    run_migration()

    total_txns = 0
    total_inserted = 0
    errors = 0
    started_at = datetime.now()

    for i, ticker in enumerate(tickers):
        try:
            txns = fetch_insider_transactions(ticker, lookback_days)
            inserted = store_transactions(txns)
            total_txns += len(txns)
            total_inserted += inserted
            if (i + 1) % 25 == 0:
                logger.info(
                    "refresh_from_list progress: %d/%d tickers processed, "
                    "%d transactions found, %d new inserts",
                    i + 1, len(tickers), total_txns, total_inserted,
                )
        except Exception:
            logger.exception("refresh_from_list error on %s", ticker)
            errors += 1

    duration_s = int((datetime.now() - started_at).total_seconds())
    return {
        "tickers_processed": len(tickers),
        "transactions_found": total_txns,
        "transactions_inserted": total_inserted,
        "errors": errors,
        "duration_s": duration_s,
    }


def list_recent_buyers() -> list[dict]:
    """List all recent open-market insider purchases."""
    cutoff = (
        datetime.now() - timedelta(days=INSIDER_CLUSTER_WINDOW_DAYS)
    ).strftime("%Y-%m-%d")

    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT ticker, insider_name, insider_title, filing_date,
                       shares, price_per_share, total_value
                FROM {TABLE_INSIDER_TRANSACTIONS}
                WHERE is_open_market = 1
                  AND transaction_type = 'purchase'
                  AND filing_date >= ?
                  AND total_value >= ?
                ORDER BY filing_date DESC, total_value DESC""",
            (cutoff, INSIDER_MIN_TRANSACTION_VALUE),
        ).fetchall()

    return [dict(r) for r in rows]


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="SEC Form 4 insider transaction tracker"
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="Refresh insider data for all active universe tickers (last 90 days)"
    )
    parser.add_argument(
        "--ticker", type=str,
        help="Refresh and display insider data for a single ticker"
    )
    parser.add_argument(
        "--buyers", action="store_true",
        help="List all recent open-market insider purchases"
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="Lookback period in days (default: 90)"
    )
    parser.add_argument(
        "--score", type=str,
        help="Show insider activity score for a ticker"
    )
    parser.add_argument(
        "--historical-backfill", type=str, metavar="TICKERS_FILE",
        help="Path to newline-separated ticker file. Refreshes insider history "
             "for each ticker using the per-ticker submissions API (Phase 1/2 backfill).",
    )

    args = parser.parse_args()

    if not any([args.refresh, args.ticker, args.buyers, args.score, args.historical_backfill]):
        parser.print_help()
        sys.exit(1)

    from .db import run_migration
    run_migration()

    if args.refresh:
        result = refresh_all(lookback_days=args.days)
        print("\nRefresh complete:")
        print(f"  Tickers processed: {result['tickers']}")
        print(f"  Transactions found: {result['transactions_found']}")
        print(f"  New transactions stored: {result['transactions_inserted']}")
        print(f"  Errors: {result['errors']}")

    elif args.ticker:
        ticker = args.ticker.upper()
        print(f"\nFetching insider data for {ticker} (last {args.days} days)...")
        txns = refresh_ticker(ticker, lookback_days=args.days)

        if not txns:
            print(f"No Form 4 filings found for {ticker}")
        else:
            print(f"\n{'Date':<12} {'Insider':<25} {'Title':<20} {'Type':<12} "
                  f"{'Shares':>10} {'Price':>10} {'Value':>14} {'OM':>4}")
            print("-" * 110)
            for t in txns:
                om = "Yes" if t.get("is_open_market") else ""
                shares = f"{t['shares']:,}" if t.get("shares") else "N/A"
                price = f"${t['price_per_share']:.2f}" if t.get("price_per_share") else "N/A"
                value = f"${t['total_value']:,.0f}" if t.get("total_value") else "N/A"
                print(f"{t['filing_date']:<12} {(t.get('insider_name') or 'Unknown'):<25} "
                      f"{(t.get('insider_title') or ''):<20} {t['transaction_type']:<12} "
                      f"{shares:>10} {price:>10} {value:>14} {om:>4}")

        # Also show score
        score_result = score_insider_activity(ticker)
        print(f"\nInsider Score: {score_result['score']:.2f}")
        print(f"Reason: {score_result['reason']}")

    elif args.buyers:
        buyers = list_recent_buyers()
        if not buyers:
            print("No recent open-market insider purchases found")
        else:
            print(f"\nRecent Open-Market Insider Purchases (last {INSIDER_CLUSTER_WINDOW_DAYS} days):")
            print(f"{'Ticker':<8} {'Date':<12} {'Insider':<25} {'Title':<20} "
                  f"{'Shares':>10} {'Value':>14}")
            print("-" * 95)
            for b in buyers:
                shares = f"{b['shares']:,}" if b.get("shares") else "N/A"
                value = f"${b['total_value']:,.0f}" if b.get("total_value") else "N/A"
                print(f"{b['ticker']:<8} {b['filing_date']:<12} "
                      f"{(b.get('insider_name') or 'Unknown'):<25} "
                      f"{(b.get('insider_title') or ''):<20} "
                      f"{shares:>10} {value:>14}")

    elif args.score:
        ticker = args.score.upper()
        result = score_insider_activity(ticker)
        print(f"\nInsider Activity Score for {ticker}:")
        print(f"  Score: {result['score']:.2f}")
        print(f"  Reason: {result['reason']}")
        print(f"  Purchases: {result['purchases']}")
        print(f"  Sales: {result['sales']}")
        if "open_market_buys" in result:
            print(f"  Open-market buys: {result['open_market_buys']}")
        if "near_52w_low" in result:
            print(f"  Near 52-week low: {result['near_52w_low']}")

    elif args.historical_backfill:
        from pathlib import Path
        tickers_file = Path(args.historical_backfill)
        if not tickers_file.exists():
            print(f"Ticker file not found: {tickers_file}")
            sys.exit(1)
        tickers = [
            line.strip() for line in tickers_file.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        print(f"Backfilling {len(tickers)} tickers from {tickers_file}...")
        result = refresh_from_list(tickers, lookback_days=args.days)
        print(f"\nBackfill complete:")
        for k, v in result.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
