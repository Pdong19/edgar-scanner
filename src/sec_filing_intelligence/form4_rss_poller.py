"""EDGAR Form 4 atom-feed poller for the SEC Filing Intelligence system.

Hourly cron job: fetches the 100 most-recent Form 4 filings from EDGAR's
atom feed, resolves each filer's CIK to a universe ticker, parses the
filing's XML, and upserts transactions with inline amendment reconciliation.

Daily 06:00 UTC cron: completeness-guarantee sweep via submissions API
across the full universe -- catches filings the hourly atom poll missed
during high-volume hours.
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from .config import EDGAR_RATE_LIMIT_RPS, EDGAR_REQUEST_TIMEOUT, EDGAR_USER_AGENT
from .form4_parser import (
    cik_to_ticker,
    is_amendment,
    parse_form4_xml,
)

logger = logging.getLogger("form4_rss_poller")

ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=4&dateb=&owner=only&count=100&output=atom"
)

# Accession ID lives in <id>urn:tag:sec.gov,2008:accession-number=NNNNNNNNNN-NN-NNNNNN</id>
_ACC_ID_RE = re.compile(r"accession-number=(\d{10}-\d{2}-\d{6})")
# EDGAR atom feed 2025+ format: <link href="https://.../Archives/edgar/data/{CIK}/.../...-index.htm"/>
# Legacy format (still supported for robustness): <link href="...?CIK={CIK}&..."/>
_CIK_LINK_RE = re.compile(r"/data/(\d+)/|CIK=(\d+)")


def _sec_get(url: str, *, accept_xml: bool = False):
    """Rate-limited + 429-retry GET helper. Shared code with insider_tracker._sec_get."""
    import requests

    headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    if accept_xml:
        headers["Accept"] = "application/xml, text/xml, */*"

    for attempt in range(4):
        resp = requests.get(url, headers=headers, timeout=EDGAR_REQUEST_TIMEOUT)
        if resp.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        # Rate limit: max EDGAR_RATE_LIMIT_RPS rps
        time.sleep(1.0 / EDGAR_RATE_LIMIT_RPS)
        return resp
    raise RuntimeError(f"SEC 429 persisted across 4 attempts for {url}")


def fetch_latest_form4_atom() -> list[dict]:
    """Fetch + parse the atom feed. Returns list of filings in feed order.

    Each filing dict: {cik, accession_number, is_amendment, updated, title}.
    """
    resp = _sec_get(ATOM_URL, accept_xml=True)
    root = ET.fromstring(resp.content)
    ns = {"a": "http://www.w3.org/2005/Atom"}

    # Detect non-atom responses (e.g., HTML error page). Real atom feed root is
    # <feed xmlns="http://www.w3.org/2005/Atom">, so tag will be "{...}feed".
    # Anything else (html, empty body) means EDGAR returned something unexpected.
    if not root.tag.endswith("feed"):
        raise ValueError(
            f"atom response is not a feed: got root tag {root.tag!r} "
            f"(likely html error page)"
        )

    filings = []
    for entry in root.findall("a:entry", ns):
        title_el = entry.find("a:title", ns)
        link_el = entry.find("a:link", ns)
        id_el = entry.find("a:id", ns)
        updated_el = entry.find("a:updated", ns)
        category_el = entry.find("a:category", ns)

        if link_el is None or id_el is None:
            continue

        acc_match = _ACC_ID_RE.search(id_el.text or "")
        cik_match = _CIK_LINK_RE.search(link_el.get("href", ""))
        if not (acc_match and cik_match):
            continue

        # Regex has two capture groups: path form (/data/{cik}/) or legacy (?CIK={cik})
        raw_cik = cik_match.group(1) or cik_match.group(2)

        filings.append({
            "cik": raw_cik.zfill(10),
            "accession_number": acc_match.group(1),
            "is_amendment": (category_el is not None and category_el.get("term") == "4/A"),
            "updated": (updated_el.text if updated_el is not None else ""),
            "title": (title_el.text if title_el is not None else ""),
        })

    return filings


_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"


def _find_form4_xml_url(cik: str, accession: str) -> str | None:
    """Fetch the filing directory listing and return the first non-index Form 4 XML."""
    acc_clean = accession.replace("-", "")
    dir_url = f"{_ARCHIVES_BASE}/{int(cik)}/{acc_clean}/"
    try:
        resp = _sec_get(dir_url)
        xml_hrefs = re.findall(r'href="([^"]+\.xml)"', resp.text)
    except Exception as exc:
        logger.warning("Failed to list dir for %s/%s: %s", cik, accession, exc)
        return None

    for href in xml_hrefs:
        base = href.rsplit("/", 1)[-1] if "/" in href else href
        if base.startswith("R") or base == "FilingSummary.xml":
            continue
        if href.startswith("/"):
            return f"https://www.sec.gov{href}"
        return f"{dir_url}{href}"
    return None


def _universe_cik_set(conn) -> set[str]:
    """Return the set of CIKs for active, non-killed universe tickers."""
    rows = conn.execute(
        "SELECT cik FROM scr_universe WHERE is_active = 1 AND is_killed = 0 AND cik IS NOT NULL"
    ).fetchall()
    return {str(r["cik"]).zfill(10) for r in rows if r["cik"]}


def _resolve_insider_schema(conn) -> tuple[str, str]:
    """Return (url_col, owned_after_col) — the actual column names in scr_insider_transactions.

    Production has (sec_url, shares_owned_after); fresh DDL has (source_url, ownership_after).
    """
    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(scr_insider_transactions)").fetchall()
    }
    url_col = "sec_url" if "sec_url" in cols else "source_url"
    owned_col = "shares_owned_after" if "shares_owned_after" in cols else "ownership_after"
    return url_col, owned_col


def _upsert_with_reconciliation(conn, txn: dict) -> tuple[int, int]:
    """INSERT a transaction + run inline amendment reconciliation.

    Returns (inserted_count, amendments_resolved_count).
    """
    url_col, owned_col = _resolve_insider_schema(conn)
    sec_url = (
        f"{_ARCHIVES_BASE}/{int(txn['cik'])}/"
        f"{txn['accession_number'].replace('-','')}/"
        f"{txn['accession_number']}-index.htm"
    )
    try:
        conn.execute(
            f"""INSERT INTO scr_insider_transactions
                 (ticker, filing_date, transaction_date, insider_name, insider_title,
                  transaction_type, shares, price_per_share, total_value,
                  {owned_col}, is_open_market, accession_number, {url_col}, is_amended)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            (
                txn["ticker"], txn["filing_date"], txn["transaction_date"],
                txn["insider_name"], txn["insider_title"], txn["transaction_type"],
                txn["shares"], txn["price_per_share"], txn["total_value"],
                txn["shares_owned_after"], txn["is_open_market"],
                txn["accession_number"], sec_url,
            ),
        )
    except sqlite3.IntegrityError as exc:
        msg = str(exc)
        # Swallow ONLY the intended dedup collision on idx_insider_dedup
        # (ticker, accession_number, insider_name, transaction_date).
        # Any other UNIQUE violation (e.g., the legacy inline constraint
        # on ticker+filing_date+insider_name+transaction_type+shares)
        # is a real bug and must propagate.
        if "UNIQUE constraint failed" in msg and "accession_number" in msg:
            return (0, 0)
        raise
    inserted = 1

    # Inline reconciliation: keep latest (filing_date, accession) as is_amended=0.
    # Use `id` not `rowid`: scr_insider_transactions has `id INTEGER PRIMARY KEY
    # AUTOINCREMENT`, which makes `rowid` an alias that sqlite3.Row doesn't expose
    # as a distinct key in result sets (IndexError on r["rowid"]).
    siblings = conn.execute(
        """SELECT id, accession_number, filing_date
             FROM scr_insider_transactions
            WHERE ticker = ? AND insider_name = ? AND transaction_date = ?
              AND is_amended = 0
            ORDER BY filing_date DESC, accession_number DESC""",
        (txn["ticker"], txn["insider_name"], txn["transaction_date"]),
    ).fetchall()

    resolved = 0
    if len(siblings) >= 2:
        canonical = siblings[0]
        older_ids = [s["id"] for s in siblings[1:]]
        conn.executemany(
            """UPDATE scr_insider_transactions
                  SET is_amended = 1, amendment_accession = ?
                WHERE id = ?""",
            [(canonical["accession_number"], rid) for rid in older_ids],
        )
        resolved = len(older_ids)

    return (inserted, resolved)


def run_poll() -> dict:
    """One hourly atom poll: fetch → universe filter → parse → upsert → log."""
    from .db import get_connection, run_migration
    run_migration()

    started_at = datetime.now(timezone.utc)
    source = "atom-poll"
    filings_fetched = 0
    filings_in_universe = 0
    filings_skipped_no_cik = 0
    new_transactions_inserted = 0
    amendments_resolved = 0
    errors_n = 0
    notes = "ok"

    try:
        filings = fetch_latest_form4_atom()
    except Exception as exc:
        errors_n = 1
        notes = f"atom fetch failed: {str(exc)[:200]}"
        filings = []

    filings_fetched = len(filings)

    with get_connection() as conn:
        universe_ciks = _universe_cik_set(conn)

        for f in filings:
            # Universe filter by CIK: skip any filing whose CIK isn't in our active,
            # non-killed universe. Atom-resolvable but off-universe CIKs (LP vehicles,
            # delisted names, private funds) are counted as skipped_no_cik.
            if f["cik"] not in universe_ciks:
                filings_skipped_no_cik += 1
                continue

            ticker = cik_to_ticker(f["cik"])
            if not ticker:
                filings_skipped_no_cik += 1
                continue

            filings_in_universe += 1

            xml_url = _find_form4_xml_url(f["cik"], f["accession_number"])
            if not xml_url:
                errors_n += 1
                continue

            try:
                xml_resp = _sec_get(xml_url, accept_xml=True)
                root = ET.fromstring(xml_resp.content)
            except Exception as exc:
                logger.warning("Parse failed for %s: %s", f["accession_number"], exc)
                errors_n += 1
                continue

            # Filing date from atom updated field (ISO format) → date string
            filing_date = (f["updated"] or "")[:10] or started_at.date().isoformat()

            txns = parse_form4_xml(root, filing_date=filing_date, accession=f["accession_number"])
            for t in txns:
                # Ensure cik is populated for the sec_url construction
                t.setdefault("cik", f["cik"])
                try:
                    ins, resolved = _upsert_with_reconciliation(conn, t)
                    new_transactions_inserted += ins
                    amendments_resolved += resolved
                except Exception as exc:
                    logger.exception(
                        "Upsert failed for %s/%s/%s: %s",
                        t.get("ticker"), t.get("insider_name"),
                        t.get("accession_number"), exc,
                    )
                    errors_n += 1

        duration_s = int((datetime.now(timezone.utc) - started_at).total_seconds())

        conn.execute(
            """INSERT INTO scr_form4_poll_log
                 (poll_ts, source, filings_fetched, filings_in_universe,
                  filings_skipped_no_cik, new_transactions_inserted,
                  amendments_resolved, errors_n, duration_s, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                started_at.isoformat(), source, filings_fetched,
                filings_in_universe, filings_skipped_no_cik,
                new_transactions_inserted, amendments_resolved,
                errors_n, duration_s, notes,
            ),
        )
        conn.commit()

    return {
        "filings_fetched": filings_fetched,
        "filings_in_universe": filings_in_universe,
        "filings_skipped_no_cik": filings_skipped_no_cik,
        "new_transactions_inserted": new_transactions_inserted,
        "amendments_resolved": amendments_resolved,
        "errors_n": errors_n,
    }


def check_stalled_feed() -> bool:
    """Return True if the last 3 market-hour polls all had 0 filings_fetched.

    Intended to be called from a separate monitoring job or from run_poll itself.
    """
    from .db import get_connection

    with get_connection() as conn:
        rows = conn.execute(
            """SELECT poll_ts, filings_fetched
                 FROM scr_form4_poll_log
                WHERE source = 'atom-poll'
                ORDER BY poll_ts DESC
                LIMIT 3"""
        ).fetchall()

    if len(rows) < 3:
        return False

    # Check all 3 had 0 filings and all within US market hours (13:00-21:00 UTC roughly)
    for r in rows:
        if r["filings_fetched"] > 0:
            return False
        ts = datetime.fromisoformat(r["poll_ts"])
        hour_utc = ts.astimezone(timezone.utc).hour
        if not (13 <= hour_utc <= 21):
            return False
    return True


def run_daily_backfill() -> dict:
    """Completeness sweep via submissions API for the full scr_universe.

    Rationale: the hourly atom poll buffers 100 filings. On quarter-end
    or peak earnings days, Form 4 volume can exceed that. This daily
    sweep iterates every universe ticker, fetches the last 24 hours of
    Form 4 filings via the submissions API, and routes them through the
    same upsert-with-reconciliation helper. Unique index silently swallows
    anything the hourly poll already caught.
    """
    from .db import get_connection, run_migration
    from .insider_tracker import refresh_from_list
    run_migration()

    started_at = datetime.now(timezone.utc)

    with get_connection() as conn:
        tickers = [
            r["ticker"] for r in conn.execute(
                "SELECT ticker FROM scr_universe WHERE is_active = 1 AND is_killed = 0"
            ).fetchall()
        ]

    result = refresh_from_list(tickers, lookback_days=1)

    duration_s = int((datetime.now(timezone.utc) - started_at).total_seconds())
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO scr_form4_poll_log
                 (poll_ts, source, filings_fetched, filings_in_universe,
                  filings_skipped_no_cik, new_transactions_inserted,
                  amendments_resolved, errors_n, duration_s, notes)
               VALUES (?, 'atom-daily-backfill', ?, ?, 0, ?, 0, ?, ?, 'ok')""",
            (
                started_at.isoformat(),
                result["transactions_found"], result["tickers_processed"],
                result["transactions_inserted"], result["errors"], duration_s,
            ),
        )
        conn.commit()

    return result


def main():
    parser = argparse.ArgumentParser(description="EDGAR Form 4 atom poller + daily backfill")
    parser.add_argument("--poll", action="store_true",
                        help="Run one hourly atom poll (writes to scr_insider_transactions)")
    parser.add_argument("--backfill-daily", action="store_true",
                        help="Run the daily completeness sweep via submissions API")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse atom feed and report counts without writing")
    args = parser.parse_args()

    if args.dry_run:
        filings = fetch_latest_form4_atom()
        print(f"atom feed returned {len(filings)} filings")
        for f in filings[:10]:
            print(f"  {f['cik']} {f['accession_number']} amend={f['is_amendment']}")
        return

    if args.poll:
        result = run_poll()
        print("Poll complete:")
        for k, v in result.items():
            print(f"  {k}: {v}")
        if check_stalled_feed():
            print("WARNING: last 3 market-hour polls returned 0 filings. Feed may be stalled.")
        return

    if args.backfill_daily:
        result = run_daily_backfill()
        print("Daily backfill complete:")
        for k, v in result.items():
            print(f"  {k}: {v}")
        return

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
