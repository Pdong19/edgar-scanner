#!/usr/bin/env python3
"""Search SEC EDGAR full-text search for moat keywords.

Usage:
    python examples/search_sec_filings.py "sole source"
    python examples/search_sec_filings.py "proprietary technology" --form 10-K

Demonstrates the EFTS (EDGAR Full-Text Search) API integration
that powers the discovery pipeline.
"""

import argparse
import json

from sec_filing_intelligence.filing_scanner import efts_search


def main():
    parser = argparse.ArgumentParser(
        description="Search SEC EDGAR filings for keywords"
    )
    parser.add_argument("keyword", help="Keyword to search for in SEC filings")
    parser.add_argument("--form", default="10-K", help="Filing type (default: 10-K)")
    parser.add_argument("--limit", type=int, default=10, help="Max results (default: 10)")
    args = parser.parse_args()

    print(f"Searching SEC EDGAR for '{args.keyword}' in {args.form} filings...\n")

    results = efts_search(
        keyword=args.keyword,
        form_type=args.form,
        limit=args.limit,
    )

    if not results:
        print("No results found.")
        return

    print(f"Found {len(results)} filings:\n")
    print(f"{'Company':<30} {'Ticker':<8} {'Filed':<12} {'CIK':<12}")
    print("-" * 65)

    for hit in results:
        company = (hit.get("entity_name") or "Unknown")[:29]
        ticker = hit.get("ticker") or "N/A"
        filed = hit.get("file_date") or "N/A"
        cik = hit.get("entity_id") or "N/A"
        print(f"  {company:<28} {ticker:<8} {filed:<12} {cik:<12}")

    print(f"\nThese companies mention '{args.keyword}' in their {args.form} filings.")
    print("Run the full discovery pipeline for scoring and validation:")
    print("  python -m sec_filing_intelligence.discovery --run")


if __name__ == "__main__":
    main()
