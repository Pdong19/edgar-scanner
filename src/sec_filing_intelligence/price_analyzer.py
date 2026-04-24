"""Price-derived screening metrics for the SEC Filing Intelligence system.

Computes ATH, 52-week range, volume, and washout detection signals.
Reads from shared price_history table (READ-ONLY). For tickers not in
price_history, fetches from yfinance. Writes to scr_price_metrics (upsert on ticker).
"""

import argparse
import time

import yfinance as yf

from .config import (
    PRICE_BATCH_DELAY,
    PRICE_LOG_FILE,
    SHARED_TABLE_PRICE_HISTORY,
    TABLE_PRICE_METRICS,
    WASHOUT_ABOVE_52W_LOW_MULT,
    WASHOUT_ATH_DROP_PCT,
    WASHOUT_MIN_AVG_VOLUME,
)
from .db import get_active_tickers, get_connection, run_migration
from .utils import get_logger

logger = get_logger("price_analyzer", log_file=PRICE_LOG_FILE)


def _get_price_data_from_db(ticker: str) -> list[dict] | None:
    """Read price data from shared price_history (READ-ONLY).

    Returns list of dicts ordered newest first, or None if ticker not found.
    """
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT date, open, high, low, close, volume
                FROM {SHARED_TABLE_PRICE_HISTORY}
                WHERE ticker = ?
                ORDER BY date DESC""",
            (ticker,),
        ).fetchall()
    if not rows:
        return None
    return [dict(r) for r in rows]


def _get_price_data_from_yfinance(ticker: str) -> list[dict] | None:
    """Fetch full price history from yfinance for ATH + recent metrics.

    Returns list of dicts ordered newest first.
    """
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(period="max", auto_adjust=False)
        if df.empty:
            logger.warning("No yfinance data for %s", ticker)
            return None
        df = df.reset_index()
        records = []
        for col in ("Date", "Open", "High", "Low", "Close", "Volume"):
            if col not in df.columns:
                logger.warning("Missing column %s in yfinance data for %s", col, ticker)
                return None
        records = [
            {
                "date": row.Date.strftime("%Y-%m-%d"),
                "open": float(row.Open),
                "high": float(row.High),
                "low": float(row.Low),
                "close": float(row.Close),
                "volume": int(row.Volume),
            }
            for row in df.itertuples(index=False)
        ]
        records.reverse()  # newest first
        return records
    except Exception:
        logger.exception("yfinance fetch failed for %s", ticker)
        return None


def _get_yfinance_info(ticker: str) -> dict:
    """Fetch float_shares and short_interest_pct from yfinance .info."""
    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}
        return {
            "float_shares": info.get("floatShares"),
            "short_interest_pct": info.get("shortPercentOfFloat"),
        }
    except Exception:
        logger.warning("Could not fetch yfinance info for %s", ticker)
        return {"float_shares": None, "short_interest_pct": None}


def analyze_ticker(ticker: str) -> dict | None:
    """Compute all price metrics for a single ticker.

    Uses price_history DB if available, otherwise yfinance.
    """
    data = _get_price_data_from_db(ticker)
    source = "price_history"
    if data is None:
        data = _get_price_data_from_yfinance(ticker)
        source = "yfinance"

    if not data or len(data) < 30:
        logger.warning("Insufficient price data for %s (%s)", ticker, source)
        return None

    logger.info("Analyzing %s from %s (%d rows)", ticker, source, len(data))

    current_price = data[0]["close"]

    # All-time high (max close across all history)
    ath_row = max(data, key=lambda r: r["close"])
    all_time_high = ath_row["close"]
    ath_date = ath_row["date"]

    # 52-week high/low (~252 trading days)
    recent_252 = data[:min(252, len(data))]
    high_52w = max(r["high"] for r in recent_252)
    low_52w = min(r["low"] for r in recent_252)

    pct_from_ath = ((current_price - all_time_high) / all_time_high * 100
                    if all_time_high > 0 else None)
    pct_from_52w_high = ((current_price - high_52w) / high_52w * 100
                         if high_52w > 0 else None)

    # Volume averages
    vol_30 = [r["volume"] for r in data[:30] if r["volume"] and r["volume"] > 0]
    vol_10 = [r["volume"] for r in data[:10] if r["volume"] and r["volume"] > 0]
    avg_volume_30d = sum(vol_30) / len(vol_30) if vol_30 else 0
    avg_volume_10d = sum(vol_10) / len(vol_10) if vol_10 else 0
    volume_ratio = avg_volume_10d / avg_volume_30d if avg_volume_30d > 0 else 0

    yf_info = _get_yfinance_info(ticker)

    return {
        "ticker": ticker,
        "current_price": current_price,
        "all_time_high": all_time_high,
        "ath_date": ath_date,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "pct_from_ath": pct_from_ath,
        "pct_from_52w_high": pct_from_52w_high,
        "avg_volume_30d": avg_volume_30d,
        "avg_volume_10d": avg_volume_10d,
        "volume_ratio": volume_ratio,
        "float_shares": yf_info["float_shares"],
        "short_interest_pct": yf_info["short_interest_pct"],
    }


_UPSERT_COLS = [
    "ticker", "current_price", "all_time_high", "ath_date",
    "high_52w", "low_52w", "pct_from_ath", "pct_from_52w_high",
    "avg_volume_30d", "avg_volume_10d", "volume_ratio",
    "float_shares", "short_interest_pct",
]


def store_metrics(metrics: dict):
    """Upsert price metrics into scr_price_metrics."""
    col_list = ", ".join(_UPSERT_COLS) + ", last_updated"
    placeholders = ", ".join(["?"] * len(_UPSERT_COLS)) + ", datetime('now')"
    updates = ", ".join(
        f"{c} = excluded.{c}" for c in _UPSERT_COLS[1:]
    ) + ", last_updated = datetime('now')"
    with get_connection() as conn:
        conn.execute(
            f"""INSERT INTO {TABLE_PRICE_METRICS} ({col_list})
                VALUES ({placeholders})
                ON CONFLICT(ticker) DO UPDATE SET {updates}""",
            tuple(metrics[c] for c in _UPSERT_COLS),
        )
        conn.commit()


def refresh_all():
    """Analyze and store price metrics for all active tickers in scr_universe."""
    run_migration()
    tickers = get_active_tickers()
    if not tickers:
        logger.warning("No active tickers in scr_universe. Nothing to analyze.")
        return

    logger.info("Refreshing price metrics for %d tickers", len(tickers))
    success, fail = 0, 0
    for i, ticker in enumerate(tickers, 1):
        try:
            metrics = analyze_ticker(ticker)
            if metrics:
                store_metrics(metrics)
                success += 1
            else:
                fail += 1
        except Exception:
            logger.exception("Failed to analyze %s", ticker)
            fail += 1
        if i < len(tickers):
            time.sleep(PRICE_BATCH_DELAY)
        if i % 20 == 0:
            logger.info("Progress: %d/%d (ok=%d, fail=%d)", i, len(tickers), success, fail)

    logger.info("Done: %d success, %d failed out of %d tickers", success, fail, len(tickers))


def get_washout_candidates() -> list[dict]:
    """Return tickers matching the washout recovery pattern.

    Criteria:
    - pct_from_ath <= WASHOUT_ATH_DROP_PCT (-70%): crashed hard from ATH
    - current_price > low_52w * WASHOUT_ABOVE_52W_LOW_MULT (1.1): not making new lows
    - avg_volume_30d > WASHOUT_MIN_AVG_VOLUME (50K): not illiquid
    """
    with get_connection() as conn:
        rows = conn.execute(
            f"""SELECT ticker, current_price, all_time_high, ath_date,
                       pct_from_ath, high_52w, low_52w, pct_from_52w_high,
                       avg_volume_30d, avg_volume_10d, volume_ratio,
                       float_shares, short_interest_pct
                FROM {TABLE_PRICE_METRICS}
                WHERE pct_from_ath <= ?
                  AND current_price > low_52w * ?
                  AND avg_volume_30d > ?
                ORDER BY pct_from_ath ASC""",
            (WASHOUT_ATH_DROP_PCT, WASHOUT_ABOVE_52W_LOW_MULT, WASHOUT_MIN_AVG_VOLUME),
        ).fetchall()
    return [dict(r) for r in rows]


def get_ticker_metrics(ticker: str) -> dict | None:
    """Return stored metrics for a single ticker."""
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT * FROM {TABLE_PRICE_METRICS} WHERE ticker = ?",
            (ticker,),
        ).fetchone()
    return dict(row) if row else None


def _print_ticker(ticker: str):
    """Print detailed metrics for a single ticker."""
    run_migration()
    metrics = get_ticker_metrics(ticker)
    if not metrics:
        logger.info("No stored metrics for %s, computing...", ticker)
        metrics = analyze_ticker(ticker)
        if metrics:
            store_metrics(metrics)
        else:
            print(f"No data available for {ticker}")
            return

    print(f"\n{'─' * 50}")
    print(f"  {ticker} — Price Analysis")
    print(f"{'─' * 50}")
    print(f"  Current Price:      ${metrics['current_price']:.2f}")
    print(f"  All-Time High:      ${metrics['all_time_high']:.2f}  ({metrics['ath_date']})")
    pct_ath = metrics.get('pct_from_ath')
    print(f"  % from ATH:         {pct_ath:.1f}%" if pct_ath is not None else "  % from ATH:         N/A")
    print(f"  52-Week High:       ${metrics['high_52w']:.2f}")
    print(f"  52-Week Low:        ${metrics['low_52w']:.2f}")
    pct_52 = metrics.get('pct_from_52w_high')
    print(f"  % from 52W High:    {pct_52:.1f}%" if pct_52 is not None else "  % from 52W High:    N/A")
    print(f"  Avg Volume (30d):   {metrics['avg_volume_30d']:,.0f}")
    print(f"  Avg Volume (10d):   {metrics['avg_volume_10d']:,.0f}")
    vr = metrics.get('volume_ratio', 0)
    spike = " ** VOLUME SPIKE **" if vr and vr > 1.5 else ""
    print(f"  Volume Ratio:       {vr:.2f}{spike}")
    fs = metrics.get('float_shares')
    print(f"  Float Shares:       {fs:,.0f}" if fs else "  Float Shares:       N/A")
    si = metrics.get('short_interest_pct')
    print(f"  Short Interest:     {si:.1%}" if si else "  Short Interest:     N/A")
    print(f"{'─' * 50}\n")


def _print_washouts():
    """Print washout candidates."""
    run_migration()
    candidates = get_washout_candidates()
    if not candidates:
        print("No washout candidates found. Run --refresh first to populate data.")
        return
    print(f"\n{'─' * 80}")
    print(f"  Washout Recovery Candidates ({len(candidates)} found)")
    print(f"  Criteria: ATH drop >= {abs(WASHOUT_ATH_DROP_PCT)}%, "
          f"above 52W low by {(WASHOUT_ABOVE_52W_LOW_MULT - 1) * 100:.0f}%, "
          f"avg vol > {WASHOUT_MIN_AVG_VOLUME:,}")
    print(f"{'─' * 80}")
    print(f"  {'Ticker':<8} {'Price':>8} {'ATH':>8} {'%ATH':>7} "
          f"{'52W Lo':>8} {'%52WH':>7} {'Vol30d':>12} {'VRatio':>7}")
    print(f"  {'─' * 72}")
    for c in candidates:
        print(f"  {c['ticker']:<8} ${c['current_price']:>7.2f} ${c['all_time_high']:>7.2f} "
              f"{c['pct_from_ath']:>6.1f}% ${c['low_52w']:>7.2f} "
              f"{c['pct_from_52w_high']:>6.1f}% {c['avg_volume_30d']:>11,.0f} "
              f"{c['volume_ratio']:>6.2f}")
    print(f"{'─' * 80}\n")


def main():
    parser = argparse.ArgumentParser(description="Screener price metrics analyzer")
    parser.add_argument("--refresh", action="store_true",
                        help="Refresh price metrics for all active tickers")
    parser.add_argument("--washouts", action="store_true",
                        help="Print washout recovery candidates")
    parser.add_argument("--ticker", type=str,
                        help="Show metrics for a specific ticker")
    args = parser.parse_args()

    if not any([args.refresh, args.washouts, args.ticker]):
        parser.print_help()
        return

    if args.refresh:
        refresh_all()
    if args.washouts:
        _print_washouts()
    if args.ticker:
        _print_ticker(args.ticker.upper())


if __name__ == "__main__":
    main()
