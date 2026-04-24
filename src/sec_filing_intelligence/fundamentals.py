"""Bulk financial data collection for the SEC Filing Intelligence universe.

Pulls quarterly financial data via yfinance, calculates derived fields
(YoY/QoQ growth, burn rate, cash runway), and stores in scr_fundamentals.

For tickers that exist in the shared `fundamentals` table (261 main watchlist),
reads that data instead of re-fetching -- avoids redundant API calls.

SAFETY: Read-only access to shared `fundamentals` table. Never writes to it.

CLI:
    python -m sec_filing_intelligence.fundamentals --refresh
    python -m sec_filing_intelligence.fundamentals --force
    python -m sec_filing_intelligence.fundamentals --ticker AAPL
"""

import argparse
import json
import sys
import time
from datetime import date, timedelta

import yfinance as yf

from .config import (
    FUNDAMENTALS_BATCH_DELAY,
    FUNDAMENTALS_BATCH_SIZE,
    FUNDAMENTALS_STALE_DAYS,
    SHARED_TABLE_FUNDAMENTALS,
    TABLE_FUNDAMENTALS,
    TABLE_UNIVERSE,
)
from .db import get_connection, run_migration
from .utils import chunk_list, get_logger

logger = get_logger("fundamentals")


def _populate_analyst_coverage(ticker: str, info: dict, db_path: str | None = None) -> None:
    """Insert today's analyst count for ticker into scr_analyst_coverage.

    Called once per ticker during the fundamentals refresh.
    """
    import sqlite3
    from datetime import date as _date
    from .config import DB_PATH as _DEFAULT_DB

    path = db_path or _DEFAULT_DB
    raw = info.get("numberOfAnalystOpinions")
    try:
        count = int(raw) if raw is not None else 0
        if count < 0:
            count = 0  # sanity (shouldn't happen but defense in depth)
    except (TypeError, ValueError):
        count = 0
    with sqlite3.connect(path) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO scr_analyst_coverage (ticker, date, analyst_count)
            VALUES (?, ?, ?)
            """,
            (ticker, _date.today().isoformat(), count),
        )


# ── Shared table reader ──────────────────────────────────────────────────────

def _read_shared_fundamentals(ticker: str) -> dict | None:
    """Read from the pipeline's fundamentals table (SELECT only).

    Maps shared column names to screener field names.
    Returns None if ticker not found.
    """
    with get_connection() as conn:
        row = conn.execute(
            f"""SELECT * FROM {SHARED_TABLE_FUNDAMENTALS}
                WHERE ticker = ? ORDER BY date DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
    if not row:
        return None

    return {
        "ticker": ticker,
        "snapshot_date": row["date"],  # Preserve source date for staleness checks
        "revenue_ttm": None,  # Not available in shared table
        "revenue_quarterly": None,
        "revenue_growth_yoy": row["revenue_growth"],
        "revenue_growth_qoq": None,
        "gross_margin": row["gross_margin"],
        "operating_margin": row["operating_margin"],
        "net_margin": row["profit_margin"],
        "free_cash_flow": None,  # Not in shared table
        "fcf_growth_yoy": None,
        "market_cap": row["market_cap"],
        "institutional_ownership": None,  # Not in shared table
        "shares_outstanding": None,
        "cash_and_equivalents": None,
        "total_debt": None,
        "debt_to_equity": row["debt_to_equity"],
        "current_ratio": row["current_ratio"],
        "roe": row["return_on_equity"],
        "quarterly_burn_rate": None,
        "cash_runway_quarters": None,
        "data_source": "shared_fundamentals",
        "raw_json": None,
    }


def _shared_tickers() -> set[str]:
    """Return set of tickers present in the shared fundamentals table."""
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT ticker FROM {SHARED_TABLE_FUNDAMENTALS}"
        ).fetchall()
    return {r["ticker"] for r in rows}


# ── yfinance fetcher ─────────────────────────────────────────────────────────

def _fetch_yfinance(ticker: str) -> dict | None:
    """Fetch fundamental data for a single ticker from yfinance.

    Extracts quarterly financials to compute derived growth and burn metrics.
    Returns a flat dict ready for storage, or None on failure.
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}
        if not info.get("marketCap"):
            logger.warning("%s: no yfinance data (missing marketCap)", ticker)
            return None

        try:
            _populate_analyst_coverage(ticker, info)
        except Exception as exc:
            logger.warning("%s: analyst coverage write failed: %s", ticker, exc)

        # ── Quarterly financials for revenue metrics ──────────────────────
        qf = stock.quarterly_financials
        revenue_ttm = None
        revenue_quarterly = None
        revenue_growth_yoy = None
        revenue_growth_qoq = None

        if qf is not None and not qf.empty and "Total Revenue" in qf.index:
            rev_series = qf.loc["Total Revenue"].dropna()
            if len(rev_series) >= 1:
                revenue_quarterly = float(rev_series.iloc[0])
            if len(rev_series) >= 4:
                revenue_ttm = float(rev_series.iloc[:4].sum())
            if len(rev_series) >= 5:
                prev_ttm = float(rev_series.iloc[1:5].sum())
                if prev_ttm > 0:
                    revenue_growth_yoy = (revenue_ttm - prev_ttm) / prev_ttm
            if len(rev_series) >= 2:
                prev_q = float(rev_series.iloc[1])
                if prev_q > 0:
                    revenue_growth_qoq = (revenue_quarterly - prev_q) / prev_q

        # ── Balance sheet for cash / debt / burn rate ─────────────────────
        bs = stock.quarterly_balance_sheet
        cash = None
        prev_cash = None
        total_debt = None

        if bs is not None and not bs.empty:
            for label in ("Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"):
                if label in bs.index:
                    cash_series = bs.loc[label].dropna()
                    if len(cash_series) >= 1:
                        cash = float(cash_series.iloc[0])
                    if len(cash_series) >= 2:
                        prev_cash = float(cash_series.iloc[1])
                    break

            for label in ("Total Debt", "Long Term Debt"):
                if label in bs.index:
                    debt_series = bs.loc[label].dropna()
                    if len(debt_series) >= 1:
                        total_debt = float(debt_series.iloc[0])
                    break

        # Burn rate = previous_cash - current_cash (positive = burning)
        quarterly_burn_rate = None
        cash_runway_quarters = None
        if cash is not None and prev_cash is not None:
            quarterly_burn_rate = prev_cash - cash
            if quarterly_burn_rate > 0 and cash > 0:
                cash_runway_quarters = cash / quarterly_burn_rate

        # ── Build result dict ─────────────────────────────────────────────
        data = {
            "ticker": ticker,
            "revenue_ttm": revenue_ttm,
            "revenue_quarterly": revenue_quarterly,
            "revenue_growth_yoy": revenue_growth_yoy,
            "revenue_growth_qoq": revenue_growth_qoq,
            "gross_margin": info.get("grossMargins"),
            "operating_margin": info.get("operatingMargins"),
            "net_margin": info.get("profitMargins"),
            "free_cash_flow": info.get("freeCashflow"),
            "fcf_growth_yoy": None,  # Needs historical scr_fundamentals comparison
            "market_cap": info.get("marketCap"),
            "institutional_ownership": info.get("heldPercentInstitutions"),
            "shares_outstanding": info.get("sharesOutstanding"),
            "cash_and_equivalents": cash,
            "total_debt": total_debt,
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "roe": info.get("returnOnEquity"),
            "quarterly_burn_rate": quarterly_burn_rate,
            "cash_runway_quarters": cash_runway_quarters,
            "data_source": "yfinance",
            "raw_json": json.dumps(
                {k: v for k, v in info.items()
                 if isinstance(v, (int, float, str, bool, type(None)))},
                default=str,
            ),
        }
        return data

    except Exception as e:
        logger.error("%s: yfinance fetch failed: %s", ticker, e)
        return None


# ── Storage ───────────────────────────────────────────────────────────────────

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_FUNDAMENTALS}
        (ticker, date, revenue_ttm, revenue_quarterly, revenue_growth_yoy,
         revenue_growth_qoq, gross_margin, operating_margin, net_margin,
         free_cash_flow, fcf_growth_yoy, market_cap, institutional_ownership,
         shares_outstanding, cash_and_equivalents, total_debt, debt_to_equity,
         current_ratio, roe, quarterly_burn_rate, cash_runway_quarters,
         data_source, raw_json)
    VALUES (?, COALESCE(?, date('now')), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ticker, date) DO UPDATE SET
        revenue_ttm=excluded.revenue_ttm,
        revenue_quarterly=excluded.revenue_quarterly,
        revenue_growth_yoy=excluded.revenue_growth_yoy,
        revenue_growth_qoq=excluded.revenue_growth_qoq,
        gross_margin=excluded.gross_margin,
        operating_margin=excluded.operating_margin,
        net_margin=excluded.net_margin,
        free_cash_flow=excluded.free_cash_flow,
        fcf_growth_yoy=excluded.fcf_growth_yoy,
        market_cap=excluded.market_cap,
        institutional_ownership=excluded.institutional_ownership,
        shares_outstanding=excluded.shares_outstanding,
        cash_and_equivalents=excluded.cash_and_equivalents,
        total_debt=excluded.total_debt,
        debt_to_equity=excluded.debt_to_equity,
        current_ratio=excluded.current_ratio,
        roe=excluded.roe,
        quarterly_burn_rate=excluded.quarterly_burn_rate,
        cash_runway_quarters=excluded.cash_runway_quarters,
        data_source=excluded.data_source,
        raw_json=excluded.raw_json
"""


def _data_to_tuple(data: dict) -> tuple:
    """Convert a fundamentals dict to a parameter tuple matching _UPSERT_SQL."""
    return (
        data["ticker"],
        data.get("snapshot_date"),  # None → COALESCE falls back to date('now')
        data.get("revenue_ttm"),
        data.get("revenue_quarterly"),
        data.get("revenue_growth_yoy"),
        data.get("revenue_growth_qoq"),
        data.get("gross_margin"),
        data.get("operating_margin"),
        data.get("net_margin"),
        data.get("free_cash_flow"),
        data.get("fcf_growth_yoy"),
        data.get("market_cap"),
        data.get("institutional_ownership"),
        data.get("shares_outstanding"),
        data.get("cash_and_equivalents"),
        data.get("total_debt"),
        data.get("debt_to_equity"),
        data.get("current_ratio"),
        data.get("roe"),
        data.get("quarterly_burn_rate"),
        data.get("cash_runway_quarters"),
        data.get("data_source"),
        data.get("raw_json"),
    )


def _store(data: dict):
    """Upsert a single row into scr_fundamentals."""
    with get_connection() as conn:
        conn.execute(_UPSERT_SQL, _data_to_tuple(data))
        conn.commit()


def _store_batch(data_list: list[dict]):
    """Upsert multiple rows into scr_fundamentals in a single transaction."""
    if not data_list:
        return
    with get_connection() as conn:
        conn.executemany(_UPSERT_SQL, [_data_to_tuple(d) for d in data_list])
        conn.commit()


# ── Ticker selection ──────────────────────────────────────────────────────────

def _get_active_tickers() -> list[str]:
    """Return all active tickers from scr_universe."""
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT ticker FROM {TABLE_UNIVERSE} WHERE is_active = 1 AND is_killed = 0 ORDER BY ticker"
        ).fetchall()
    return [r["ticker"] for r in rows]


def _get_stale_tickers(tickers: list[str]) -> list[str]:
    """Filter to tickers not updated within FUNDAMENTALS_STALE_DAYS.

    Single query: find tickers with a recent row, then subtract from input list.
    """
    cutoff = (date.today() - timedelta(days=FUNDAMENTALS_STALE_DAYS)).isoformat()
    with get_connection() as conn:
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"""SELECT DISTINCT ticker FROM {TABLE_FUNDAMENTALS}
                WHERE ticker IN ({placeholders}) AND date >= ?""",
            (*tickers, cutoff),
        ).fetchall()
    fresh = {r["ticker"] for r in rows}
    return [t for t in tickers if t not in fresh]


# ── Main collection logic ────────────────────────────────────────────────────

def _collect_data(ticker: str, shared_set: set[str]) -> dict | None:
    """Collect fundamentals for a single ticker without storing.

    Tries shared fundamentals table first, then yfinance.
    """
    data = None
    if ticker in shared_set:
        data = _read_shared_fundamentals(ticker)
        if data:
            logger.debug("%s: read from shared fundamentals table", ticker)

    if not data:
        data = _fetch_yfinance(ticker)

    return data


def collect_single(ticker: str, shared_set: set[str] | None = None) -> dict | None:
    """Collect fundamentals for a single ticker and store.

    Used by CLI --ticker mode. For batch use, prefer collect_batch().
    """
    if shared_set is None:
        shared_set = _shared_tickers()

    data = _collect_data(ticker, shared_set)
    if data:
        _store(data)
    return data


def collect_batch(tickers: list[str], force: bool = False) -> dict:
    """Collect fundamentals for a list of tickers in batches.

    Args:
        tickers: List of ticker symbols.
        force: If False, skip tickers updated within FUNDAMENTALS_STALE_DAYS.

    Returns:
        Summary dict with counts.
    """
    if not force:
        original_count = len(tickers)
        tickers = _get_stale_tickers(tickers)
        skipped = original_count - len(tickers)
        if skipped:
            logger.info("Skipping %d fresh tickers (updated within %d days)",
                        skipped, FUNDAMENTALS_STALE_DAYS)

    empty = {"total": 0, "success": 0, "failed": 0, "from_shared": 0, "failed_tickers": []}
    if not tickers:
        logger.info("No tickers need updating")
        return empty

    shared_set = _shared_tickers()
    batches = chunk_list(tickers, FUNDAMENTALS_BATCH_SIZE)

    success = 0
    failed = 0
    from_shared = 0
    failed_tickers = []

    logger.info("Processing %d tickers in %d batches of %d",
                len(tickers), len(batches), FUNDAMENTALS_BATCH_SIZE)

    for batch_idx, batch in enumerate(batches):
        if batch_idx > 0:
            time.sleep(FUNDAMENTALS_BATCH_DELAY)

        batch_results = []
        for ticker in batch:
            data = _collect_data(ticker, shared_set)
            if data:
                batch_results.append(data)
                success += 1
                if data.get("data_source") == "shared_fundamentals":
                    from_shared += 1
            else:
                failed += 1
                failed_tickers.append(ticker)

        _store_batch(batch_results)
        logger.info("Batch %d/%d complete (%d/%d tickers done)",
                    batch_idx + 1, len(batches), success + failed, len(tickers))

    summary = {
        "total": len(tickers),
        "success": success,
        "failed": failed,
        "from_shared": from_shared,
        "failed_tickers": failed_tickers,
    }

    logger.info(
        "Collection complete: %d total, %d success (%d from shared table), %d failed",
        summary["total"], summary["success"], summary["from_shared"], summary["failed"],
    )
    if failed_tickers:
        logger.warning("Failed tickers: %s", ", ".join(failed_tickers[:50]))

    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Screener fundamentals collection"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--refresh", action="store_true",
                       help="Incremental update (skip fresh tickers)")
    group.add_argument("--force", action="store_true",
                       help="Full refresh of all active tickers")
    group.add_argument("--ticker", type=str,
                       help="Single ticker debug mode")
    args = parser.parse_args()

    run_migration()

    if args.ticker:
        logger.info("Single ticker mode: %s", args.ticker)
        data = collect_single(args.ticker.upper())
        if data:
            # Print key fields for debug
            for k, v in data.items():
                if k != "raw_json":
                    print(f"  {k}: {v}")
        else:
            print(f"  No data returned for {args.ticker.upper()}")
            sys.exit(1)
    else:
        tickers = _get_active_tickers()
        if not tickers:
            logger.error("No active tickers in scr_universe. Run universe.sync_from_watchlist() first.")
            sys.exit(1)
        summary = collect_batch(tickers, force=args.force)
        if summary["failed"] > 0:
            sys.exit(2)  # Partial failure


if __name__ == "__main__":
    main()
