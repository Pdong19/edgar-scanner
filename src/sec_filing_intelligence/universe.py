"""Universe management — seed and inspect the scr_universe ticker table.

Every scanner module reads its working set from scr_universe, so a fresh
database must be seeded before anything else will produce output:

    python -m sec_filing_intelligence.universe --seed examples/sample_universe.txt
    python -m sec_filing_intelligence.universe --add RCAT,AEHR,BKSY
    python -m sec_filing_intelligence.universe --list

Seeding resolves each ticker's CIK from SEC's public company_tickers.json so
the Form 4 poller's CIK-based universe filter works out of the box. If that
lookup is unavailable (offline), tickers are still seeded and the CIKs can be
filled in by re-running --seed later.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import requests

from .config import (
    EDGAR_REQUEST_TIMEOUT,
    EDGAR_USER_AGENT,
    TABLE_UNIVERSE,
    warn_if_placeholder_identity,
)
from .db import get_connection, run_migration
from .utils import get_logger

logger = get_logger("universe")

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def load_ticker_file(path: str | Path) -> list[str]:
    """Read tickers from a text file: one per line, '#' comments and blanks ignored."""
    tickers: list[str] = []
    seen: set[str] = set()
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.split("#", 1)[0].strip().upper()
        if line and line not in seen:
            seen.add(line)
            tickers.append(line)
    return tickers


def fetch_cik_map() -> dict[str, dict]:
    """Fetch SEC's ticker→CIK mapping. Returns {} on any failure (offline-safe)."""
    try:
        resp = requests.get(
            COMPANY_TICKERS_URL,
            headers={"User-Agent": EDGAR_USER_AGENT},
            timeout=EDGAR_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001 — degrade to CIK-less seeding on any failure
        logger.warning("Could not fetch SEC ticker→CIK map (%s); seeding without CIKs", exc)
        return {}
    return {
        entry["ticker"].upper(): {
            "cik": str(entry["cik_str"]).zfill(10),
            "company_name": entry.get("title"),
        }
        for entry in data.values()
        if entry.get("ticker")
    }


def seed_universe(tickers: list[str], cik_map: dict[str, dict] | None = None) -> dict:
    """Upsert tickers into scr_universe as active. Returns a summary dict.

    Existing rows are reactivated and enriched (CIK/company name filled in when
    known) without clobbering columns other modules maintain.
    """
    run_migration()
    if cik_map is None:
        cik_map = fetch_cik_map()
    today = date.today().isoformat()
    added = updated = no_cik = 0
    with get_connection() as conn:
        for ticker in tickers:
            info = cik_map.get(ticker, {})
            if not info.get("cik"):
                no_cik += 1
            exists = conn.execute(
                f"SELECT 1 FROM {TABLE_UNIVERSE} WHERE ticker = ?", (ticker,)
            ).fetchone()
            conn.execute(
                f"""INSERT INTO {TABLE_UNIVERSE} (ticker, company_name, cik, is_active, last_updated)
                    VALUES (?, ?, ?, 1, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        is_active = 1,
                        cik = COALESCE(excluded.cik, {TABLE_UNIVERSE}.cik),
                        company_name = COALESCE(excluded.company_name, {TABLE_UNIVERSE}.company_name),
                        last_updated = excluded.last_updated""",
                (ticker, info.get("company_name"), info.get("cik"), today),
            )
            if exists:
                updated += 1
            else:
                added += 1
        conn.commit()
    summary = {"added": added, "updated": updated, "no_cik": no_cik, "total": len(tickers)}
    logger.info(
        "Universe seed: %d added, %d updated, %d without CIK (of %d)",
        added, updated, no_cik, len(tickers),
    )
    return summary


def list_universe() -> list[dict]:
    """Return active universe rows (ticker, company_name, cik)."""
    run_migration()
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT ticker, company_name, cik FROM {TABLE_UNIVERSE}
                WHERE is_active = 1 AND is_killed = 0 ORDER BY ticker"""
        ).fetchall()
    return [dict(r) for r in rows]


def main(argv: list[str] | None = None) -> int:
    warn_if_placeholder_identity()
    parser = argparse.ArgumentParser(
        prog="python -m sec_filing_intelligence.universe",
        description="Seed and inspect the scr_universe ticker table.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--seed", metavar="FILE", help="Seed tickers from a text file (one per line)")
    group.add_argument("--add", metavar="TICKERS", help="Seed a comma-separated list of tickers")
    group.add_argument("--list", action="store_true", help="List active universe tickers")
    args = parser.parse_args(argv)

    if args.list:
        rows = list_universe()
        if not rows:
            print("Universe is empty. Seed it with --seed or --add.")
            return 1
        for r in rows:
            print(f"{r['ticker']:<8} {r['cik'] or '(no CIK)':<12} {r['company_name'] or ''}")
        print(f"\n{len(rows)} active tickers")
        return 0

    if args.seed:
        tickers = load_ticker_file(args.seed)
    else:
        tickers = [t.strip().upper() for t in args.add.split(",") if t.strip()]
    if not tickers:
        print("No tickers to seed.", file=sys.stderr)
        return 1
    summary = seed_universe(tickers)
    print(
        f"Seeded {summary['total']} tickers: {summary['added']} new, "
        f"{summary['updated']} updated, {summary['no_cik']} without CIK."
    )
    print("Next: python -m sec_filing_intelligence.fundamentals  (then run a scanner)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
