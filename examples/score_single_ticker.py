#!/usr/bin/env python3
"""Score a single ticker with the AMPX 11-dimension screener.

Usage:
    python examples/score_single_ticker.py ASTS

This bypasses the full universe scan and scores one ticker on demand,
showing the breakdown across all 11 dimensions.
"""

import sys

from sec_filing_intelligence.ampx_rules import rescore_one
from sec_filing_intelligence.db import run_migration

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python examples/score_single_ticker.py <TICKER>")
        print("Example: python examples/score_single_ticker.py ASTS")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    run_migration()

    print(f"Scoring {ticker} across 11 dimensions...")
    print("(Fetches live data from yfinance + SEC EDGAR)\n")

    result = rescore_one(ticker)
    if result is None:
        print(f"Could not score {ticker} — check ticker symbol or data availability.")
        sys.exit(1)

    print(f"{'Dimension':<25} {'Score':>6}  {'Max':>5}")
    print("-" * 40)

    dims = [
        ("CRASH", "dim1_crash", 2.0),
        ("REVGROWTH", "dim2_revgrowth", 2.0),
        ("DEBT", "dim3_debt", 1.0),
        ("RUNWAY", "dim4_runway", 1.5),
        ("FLOAT", "dim5_float", 1.0),
        ("INSTITUTIONAL", "dim6_institutional", 1.0),
        ("ANALYST", "dim7_analyst", 1.0),
        ("PRIORITY SECTOR", "dim8_priority_industry", 1.0),
        ("SHORT INTEREST", "dim9_short", 0.5),
        ("LEAPS OPTIONS", "dim10_leaps", 0.5),
        ("INSIDER BUYING", "dim11_insider", 1.0),
    ]

    total = 0
    for label, key, max_score in dims:
        score = result.get(key, 0) or 0
        total += score
        bar = "#" * int(score / max_score * 10) if max_score > 0 else ""
        print(f"  {label:<23} {score:>6.1f}  /{max_score:<4.1f}  {bar}")

    print("-" * 40)
    print(f"  {'TOTAL':<23} {total:>6.1f}  /12.5")

    if result.get("has_going_concern"):
        print("\n  WARNING: Going-concern language detected in latest 10-K")
