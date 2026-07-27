#!/usr/bin/env python3
"""Score a single ticker with the AMPX threshold screener.

Usage:
    python examples/score_single_ticker.py ASTS

Prints the per-dimension breakdown (max 12.5). Scores are computed from data
already in the local database, so a fresh clone needs three steps first:

    python -m sec_filing_intelligence.universe --seed examples/sample_universe.txt
    python -m sec_filing_intelligence.fundamentals
    python -m sec_filing_intelligence.price_analyzer
"""

import sys

from sec_filing_intelligence.ampx_rules import rescore_one
from sec_filing_intelligence.config import warn_if_placeholder_identity
from sec_filing_intelligence.db import get_active_tickers, run_migration
from sec_filing_intelligence.universe import seed_universe


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python examples/score_single_ticker.py <TICKER>")
        print("Example: python examples/score_single_ticker.py ASTS")
        return 1

    ticker = sys.argv[1].upper()
    warn_if_placeholder_identity()
    run_migration()

    if ticker not in get_active_tickers():
        print(f"{ticker} is not in the universe yet — adding it.")
        seed_universe([ticker])
        print(
            "Note: dimensions score 0 until data exists for it. Populate with:\n"
            "  python -m sec_filing_intelligence.fundamentals\n"
            "  python -m sec_filing_intelligence.price_analyzer\n"
        )

    print(f"Scoring {ticker} (LEAPS/insider checks fetch live data)...\n")
    # rescore_one prints the score and per-dimension breakdown, returns an exit code.
    return rescore_one(ticker)


if __name__ == "__main__":
    sys.exit(main())
