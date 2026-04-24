"""EDGAR XBRL fundamentals extractor — parallel data layer to scr_fundamentals.

Replaces the yfinance-sourced fundamentals (which produced known-bad values like
ZENA's 412-quarter runway, SOHU's 437% short interest, ~1,100 null tickers) with
SEC EDGAR XBRL via the `edgartools` library. The XBRL data is the issuer's own
machine-readable filing — the primary source.

Part of the SEC Filing Intelligence system.

Three CLI subcommands:
  --refresh                Pull XBRL for every active ticker in scr_universe.
  --ticker TICK            Pull XBRL for a single ticker (debugging).
  --validate               Generate a validation report comparing scr_fundamentals
                           and scr_fundamentals_xbrl; writes to output/ampx_rules/.

Failure-mode policy:
  Tickers without XBRL coverage MUST fall back to yfinance via COALESCE in the
  scorer join. Don't reintroduce the silent-drop failure mode this migration is
  solving by replacing it with an XBRL-shaped version of the same bug.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date as _date_class
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# edgartools requires EDGAR_IDENTITY before import (silently errors otherwise).
# Set it before any edgar.* import so test-time and CLI both work.
os.environ.setdefault("EDGAR_IDENTITY", "SECFilingIntelligence research@example.com")

from .config import (
    AMPX_OUTPUT_DIR,
    DB_PATH as _DEFAULT_DB_PATH,
    XBRL_BATCH_BACKOFF_SEC,
    XBRL_BATCH_ERROR_THRESHOLD,
    XBRL_BATCH_SIZE,
    XBRL_RATE_LIMIT_SLEEP_SEC,
    XBRL_RUNWAY_CAP_QUARTERS,
)
from .utils import get_logger

logger = get_logger("xbrl_fundamentals", log_file="screener_xbrl.log")


# Concept names we pull from edgartools' normalized layer (full list of 59
# returned by facts.list_supported_concepts()). We keep this list explicit
# rather than iterating get_all_facts() so we can reason about what's missing
# per-ticker.
_CONCEPTS = (
    "cash_and_equivalents",
    "long_term_debt",
    "short_term_debt",
    "stockholders_equity",
    "total_assets",
    "total_liabilities",
    "common_shares_outstanding",
    "operating_cash_flow",
    "capex",
)


# ── Internal helpers ─────────────────────────────────────────────────────────

def _get_company(ticker: str):
    """Indirection so tests can monkeypatch edgartools without touching network."""
    from edgar import Company  # provided by edgartools

    return Company(ticker)


def _safe_get_concept(facts, name: str) -> float | None:
    """Return facts.get_concept(name) silently as a float, or None on any failure."""
    try:
        v = facts.get_concept(name)
    except Exception:
        return None
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _shares_history(facts) -> list[tuple[str, float]]:
    """Return list of (period_end_iso, shares) sorted by period_end DESC.

    Prefers us-gaap:CommonStockSharesOutstanding (the FY-end balance value).
    Falls back to dei:EntityCommonStockSharesOutstanding (filing-date snapshot
    measured weeks after FY-end) when the us-gaap concept is missing or stale,
    which is the case for many REITs and partnerships.

    Returns empty list on any failure.
    """
    candidates: list[tuple[str, float]] = []
    for concept in (
        "us-gaap:CommonStockSharesOutstanding",
        "dei:EntityCommonStockSharesOutstanding",
        "ifrs-full:NumberOfSharesOutstanding",
    ):
        try:
            df = (
                facts.query()
                .by_concept(concept)
                .by_fiscal_period("FY")
                .to_dataframe()
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df_sorted = df.sort_values(
            ["period_end", "filing_date"], ascending=[False, False]
        ).drop_duplicates("period_end", keep="first")
        rows: list[tuple[str, float]] = []
        for _, r in df_sorted.iterrows():
            try:
                pe = str(r["period_end"])
                v = float(r["numeric_value"])
            except (TypeError, ValueError, KeyError):
                continue
            rows.append((pe, v))
        if not rows:
            continue
        # If the latest us-gaap point is more than 18 months old, the company
        # likely stopped tagging that concept (REIT case). Drop it and try the
        # next concept.
        try:
            latest_pe = datetime.fromisoformat(rows[0][0]).date()
            today = datetime.now(timezone.utc).date()
            if (today - latest_pe).days > 540:
                # Stale us-gaap series — keep but try dei first
                if not candidates:
                    candidates = rows
                continue
        except Exception:
            pass
        return rows
    return candidates


_REVENUE_CONCEPTS = (
    "us-gaap:Revenues",
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
    "us-gaap:SalesRevenueNet",
    "us-gaap:SalesRevenueGoodsNet",
    "ifrs-full:Revenue",
    "ifrs-full:RevenueFromContractsWithCustomers",
)

# Bank/financial-sector revenue concepts. Banks don't tag SalesRevenueNet — their
# operating income is interest + noninterest. Added 2026-04-15 (Phase 1d) to
# rescue ~56 regional banks that were showing near-zero XBRL revenue while
# yfinance reported real interest+noninterest totals.
#
# These are probed AFTER _REVENUE_CONCEPTS via _bank_annual_revenue fallback:
# for non-banks with an incidental interest income fact (e.g. tech co earning
# interest on cash), the primary revenue concept wins. For banks with no
# primary revenue fact, interest + noninterest are summed for the latest FY.
_BANK_INTEREST_CONCEPTS = (
    "us-gaap:InterestAndDividendIncomeOperating",
    "us-gaap:InterestIncomeOperating",
)
_BANK_NONINTEREST_CONCEPTS = (
    "us-gaap:NoninterestIncome",
)


def _collect_fy_rows(facts, concepts: tuple[str, ...]) -> list[dict[str, Any]]:
    """Walk concepts, collect FY-tagged rows into normalized dicts.

    Each dict has: period_end (str), value (float), filing_date (str),
    fiscal_year (int|None), concept (str).
    """
    out: list[dict[str, Any]] = []
    for concept in concepts:
        try:
            df = (
                facts.query()
                .by_concept(concept)
                .by_fiscal_period("FY")
                .to_dataframe()
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        has_filing = "filing_date" in df.columns
        has_fy = "fiscal_year" in df.columns
        for _, r in df.iterrows():
            try:
                pe = str(r["period_end"])
                v = float(r["numeric_value"])
            except (TypeError, ValueError, KeyError):
                continue
            fd = ""
            if has_filing:
                try:
                    fd = str(r["filing_date"])
                except (KeyError, IndexError):
                    fd = ""
            fy: int | None = None
            if has_fy:
                try:
                    raw = r["fiscal_year"]
                    fy = int(raw) if raw is not None else None
                except (KeyError, IndexError, TypeError, ValueError):
                    fy = None
            out.append({
                "period_end": pe,
                "value": v,
                "filing_date": fd,
                "fiscal_year": fy,
                "concept": concept,
            })
    return out


def _bank_annual_revenue(facts) -> tuple[float | None, int | None, str | None]:
    """Bank-concept fallback for `_annual_revenue`: sum interest + noninterest
    income for the latest FY where interest income is present.

    Regional banks don't tag SalesRevenueNet — their operating income is
    reported as separate interest and noninterest concepts. Under the primary
    `_REVENUE_CONCEPTS` path, these banks return near-zero revenue (e.g. a
    single fee-income line) and end up in the XBRL-vs-yf divergence pool.

    Logic:
      1. Collect FY-tagged interest-income rows across `_BANK_INTEREST_CONCEPTS`.
      2. Pick the latest period with interest > 0.
      3. Find noninterest-income rows for that exact same period.
      4. Return (interest + noninterest, fiscal_year, period_end).
      5. If no interest income is tagged at all, return None triple.

    Non-banks with an incidental interest-income fact (e.g. a tech company
    earning a few thousand on its cash pile) are harmless here because
    `_annual_revenue` only calls this fallback AFTER the primary concepts
    return nothing — and tech companies tag SalesRevenueNet/Revenues.
    """
    interest_rows = _collect_fy_rows(facts, _BANK_INTEREST_CONCEPTS)
    interest_pos = [r for r in interest_rows if r["value"] > 0]
    if not interest_pos:
        return None, None, None

    interest_pos.sort(
        key=lambda r: (r["period_end"], r["filing_date"], r["value"]),
        reverse=True,
    )
    best_interest = interest_pos[0]
    period = best_interest["period_end"]

    noninterest_rows = _collect_fy_rows(facts, _BANK_NONINTEREST_CONCEPTS)
    noninterest_match = [
        r for r in noninterest_rows if r["period_end"] == period and r["value"] > 0
    ]
    noninterest_match.sort(key=lambda r: (r["filing_date"], r["value"]), reverse=True)
    noninterest_value = noninterest_match[0]["value"] if noninterest_match else 0.0

    total = best_interest["value"] + noninterest_value
    try:
        fy = int(period[:4])
    except (ValueError, IndexError):
        fy = best_interest["fiscal_year"]
    return total, fy, period


def _annual_revenue(facts) -> tuple[float | None, int | None, str | None]:
    """Return (revenue, fiscal_year, period_end_date) for the latest FY.

    Primary path picks the row with max (period_end, filing_date, value)
    across every concept in _REVENUE_CONCEPTS. This fixes the ASC 606
    stale-tag bug: companies like INO that stopped tagging us-gaap:Revenues
    in 2018 (after switching to us-gaap:RevenueFromContract...) left a stale
    FY2017 fact under the legacy concept. The old "first non-None hit wins"
    logic picked that stale value; max-period-across-concepts skips it.

    Bank fallback (added 2026-04-15, Phase 1d) picks up interest + noninterest
    for regional banks that don't tag standard revenue concepts.

    Fallback to facts.get_annual_fact / facts.get_revenue preserves behavior
    for test stubs and any edge case where the query path returns empty.
    """
    rows = _collect_fy_rows(facts, _REVENUE_CONCEPTS)
    nonzero = [r for r in rows if r["value"] > 0]
    if nonzero:
        nonzero.sort(
            key=lambda r: (r["period_end"], r["filing_date"], r["value"]),
            reverse=True,
        )
        best = nonzero[0]
        pe = best["period_end"]
        try:
            fy = int(pe[:4])
        except (ValueError, IndexError):
            fy = best["fiscal_year"]
        return best["value"], fy, pe

    # Bank-concept fallback: interest + noninterest income sum for banks that
    # don't tag standard revenue concepts.
    bank_rev, bank_fy, bank_pe = _bank_annual_revenue(facts)
    if bank_rev is not None:
        return bank_rev, bank_fy, bank_pe

    # Fallback: get_annual_fact per concept (legacy test path)
    for concept in _REVENUE_CONCEPTS:
        try:
            af = facts.get_annual_fact(concept)
        except Exception:
            af = None
        if af is None:
            continue
        try:
            v = float(af.numeric_value) if af.numeric_value is not None else float(af.value)
            fy = int(af.fiscal_year) if af.fiscal_year is not None else None
            pe = str(af.period_end) if af.period_end is not None else None
            if v > 0 and fy is not None and pe:
                return v, fy, pe
        except (TypeError, ValueError):
            continue

    # Last resort: scalar get_revenue (no fiscal_year metadata, caller skips row)
    try:
        v = facts.get_revenue()
    except Exception:
        v = None
    if v is None:
        return None, None, None
    return float(v), None, None


def _prior_revenue_yoy(facts, latest_fy: int | None) -> float | None:
    """Return prior-year (latest_fy - 1) revenue from latest filing across concepts.

    Falls back to interest + noninterest income sum for banks (Phase 1d
    2026-04-15) — same logic as `_bank_annual_revenue` but constrained to
    the target fiscal year.
    """
    if latest_fy is None:
        return None
    target_fy = latest_fy - 1

    # Primary: standard revenue concepts
    rows = _collect_fy_rows(facts, _REVENUE_CONCEPTS)
    matches = [
        r for r in rows
        if r["period_end"].startswith(f"{target_fy}-") and r["value"] > 0
    ]
    if matches:
        matches.sort(key=lambda r: (r["filing_date"], r["value"]), reverse=True)
        return matches[0]["value"]

    # Bank fallback: sum interest + noninterest for target_fy
    interest_rows = _collect_fy_rows(facts, _BANK_INTEREST_CONCEPTS)
    interest_match = [
        r for r in interest_rows
        if r["period_end"].startswith(f"{target_fy}-") and r["value"] > 0
    ]
    if not interest_match:
        return None
    interest_match.sort(key=lambda r: (r["filing_date"], r["value"]), reverse=True)
    target_period = interest_match[0]["period_end"]
    interest_val = interest_match[0]["value"]

    noninterest_rows = _collect_fy_rows(facts, _BANK_NONINTEREST_CONCEPTS)
    noninterest_match = [
        r for r in noninterest_rows
        if r["period_end"] == target_period and r["value"] > 0
    ]
    noninterest_match.sort(key=lambda r: (r["filing_date"], r["value"]), reverse=True)
    noninterest_val = noninterest_match[0]["value"] if noninterest_match else 0.0

    return interest_val + noninterest_val


def _sanitize_shares(raw: list[tuple[str, float]]) -> tuple[list[tuple[str, float]], bool]:
    """Drop issuer-filed scaling garbage from a period_end-DESC shares list.

    Returns (sane_list, was_sanitized).

    Rejects:
      - Any observation > 10 billion shares (no legitimate US small/mid-cap
        has this many; almost always a missing decimals or scale attribute
        in the issuer's iXBRL).
      - Any observation that's > 50× the next-older one (SVRE 2025: 30B vs
        prior year 415M = 72× jump suggests a filing error, not real dilution).
    """
    flagged = False

    # Pass 1: hard ceiling
    capped: list[tuple[str, float]] = []
    for pe, v in raw:
        if v > 10_000_000_000:
            flagged = True
            continue
        capped.append((pe, v))

    # Pass 2: walk latest-to-oldest, drop any value >50x its successor
    cleaned: list[tuple[str, float]] = []
    i = 0
    while i < len(capped):
        pe, v = capped[i]
        if i + 1 < len(capped):
            _, next_v = capped[i + 1]
            if next_v > 0 and v / next_v > 50:
                flagged = True
                i += 1
                continue
        cleaned.append((pe, v))
        i += 1

    return cleaned, flagged


# ── Public extractor ─────────────────────────────────────────────────────────

def extract_xbrl_fundamentals(ticker: str) -> dict[str, Any] | None:
    """Pull latest annual XBRL facts + 2-year shares history for one ticker.

    Returns a dict matching the scr_fundamentals_xbrl schema, or None on hard
    failure (no CIK, no facts, exception during top-level Company() lookup).

    Never raises — callers iterate over thousands of tickers and errors must
    isolate to a single row.

    data_quality_flags is a JSON-serializable list of any of:
      - "no_debt_reported"     — both long+short debt are None AND
                                  stockholders_equity is non-None
      - "no_cashflow_reported" — operating_cash_flow is None
      - "no_gross_profit_reported" — gross_profit is None (banks, REITs)
      - "incomplete_history"   — fewer than 3 annual share observations OR
                                  stockholders_equity is None (balance-sheet
                                  not parseable, can't confirm "no debt")
      - "positive_cashflow"    — op_cf > 0 → cash_runway_quarters is N/A
      - "bank_income_statement" — operating_income is None AND total_assets > $1T
      - "reit_structure"       — total_liabilities > 0.7 * total_assets and
                                   no gross_profit (rough heuristic)
    """
    flags: list[str] = []

    try:
        company = _get_company(ticker)
    except Exception as exc:
        logger.debug("Company(%s) failed: %s", ticker, exc)
        return None

    # CIK lookup failure
    cik = getattr(company, "cik", None)
    if cik is None:
        logger.debug("%s: no CIK", ticker)
        return None

    try:
        facts = company.get_facts()
    except Exception as exc:
        logger.debug("%s: get_facts() failed: %s", ticker, exc)
        return None

    if facts is None:
        return None

    # ── Revenue + period anchor ──────────────────────────────────────────────
    revenue, fiscal_year, period_end_date = _annual_revenue(facts)
    if revenue is None or fiscal_year is None or period_end_date is None:
        # No revenue or no fiscal year context — we can't anchor a row in the
        # PRIMARY KEY (ticker, fiscal_year). Bail.
        logger.debug("%s: no annual revenue / fiscal_year — skipping", ticker)
        return None

    revenue_prev_year = _prior_revenue_yoy(facts, fiscal_year)
    revenue_growth_yoy: float | None
    if revenue_prev_year and revenue_prev_year > 0:
        revenue_growth_yoy = (revenue - revenue_prev_year) / revenue_prev_year
    else:
        revenue_growth_yoy = None

    # ── Income statement extras (None for banks / REITs) ─────────────────────
    try:
        gross_profit = facts.get_gross_profit()
    except Exception:
        gross_profit = None
    try:
        operating_income = facts.get_operating_income()
    except Exception:
        operating_income = None
    try:
        net_income = facts.get_net_income()
    except Exception:
        net_income = None

    if gross_profit is None:
        flags.append("no_gross_profit_reported")

    # ── Balance sheet ────────────────────────────────────────────────────────
    cash_and_equivalents = _safe_get_concept(facts, "cash_and_equivalents")
    long_term_debt = _safe_get_concept(facts, "long_term_debt")
    short_term_debt = _safe_get_concept(facts, "short_term_debt")
    total_assets = _safe_get_concept(facts, "total_assets")
    total_liabilities = _safe_get_concept(facts, "total_liabilities")
    stockholders_equity = _safe_get_concept(facts, "stockholders_equity")

    # Per the plan: long_term_debt None → 0 ONLY when stockholders_equity is
    # non-None (confirms balance sheet actually filed). DNN edge case: equity
    # also None → leave debt as None and add both flags.
    if long_term_debt is None and short_term_debt is None:
        if stockholders_equity is not None:
            flags.append("no_debt_reported")
            # Treat as zero for total_debt math, but keep the individual fields
            # as None to preserve the "not reported" signal.
            total_debt: float | None = 0.0
        else:
            # Balance sheet not parseable → can't confirm "no debt"
            flags.append("no_debt_reported")
            flags.append("incomplete_history")
            total_debt = None
    else:
        total_debt = (long_term_debt or 0.0) + (short_term_debt or 0.0)

    if total_debt is not None and stockholders_equity and stockholders_equity > 0:
        debt_to_equity = total_debt / stockholders_equity
    else:
        debt_to_equity = None

    # ── Shares history ──────────────────────────────────────────────────────
    shares_hist_raw = _shares_history(facts)
    shares_hist, shares_scaling_detected = _sanitize_shares(shares_hist_raw)
    if shares_scaling_detected:
        flags.append("scaling_bug_suspected")
    shares_outstanding: float | None = None
    shares_1yr_ago: float | None = None
    shares_2yr_ago: float | None = None
    if shares_hist:
        shares_outstanding = shares_hist[0][1]
    if len(shares_hist) >= 2:
        shares_1yr_ago = shares_hist[1][1]
    if len(shares_hist) >= 3:
        shares_2yr_ago = shares_hist[2][1]
    else:
        # Fewer than 3 annual observations → flag incomplete history (unless
        # already added above for the no-balance-sheet case).
        if "incomplete_history" not in flags:
            flags.append("incomplete_history")

    if (
        shares_outstanding is not None
        and shares_1yr_ago is not None
        and shares_1yr_ago > 0
    ):
        shares_change_1yr_pct = (shares_outstanding / shares_1yr_ago - 1.0) * 100.0
    else:
        shares_change_1yr_pct = None

    if (
        shares_outstanding is not None
        and shares_2yr_ago is not None
        and shares_2yr_ago > 0
    ):
        shares_change_2yr_pct = (shares_outstanding / shares_2yr_ago - 1.0) * 100.0
    else:
        shares_change_2yr_pct = None

    # ── Cash flow ────────────────────────────────────────────────────────────
    operating_cash_flow = _safe_get_concept(facts, "operating_cash_flow")
    capex_raw = _safe_get_concept(facts, "capex")
    # XBRL capex (us-gaap:PaymentsToAcquirePropertyPlantAndEquipment) is positive
    # in the filing but conventionally negative in cash-flow models. We keep the
    # sign as filed (positive = outflow) so downstream math is explicit.
    capex = capex_raw

    if operating_cash_flow is None:
        flags.append("no_cashflow_reported")

    if operating_cash_flow is not None and capex is not None:
        # FCF = OCF - capex (capex stored positive as outflow)
        free_cash_flow = operating_cash_flow - capex
    else:
        free_cash_flow = None

    # Cash runway only when burn (negative OCF) is real
    if operating_cash_flow is None:
        quarterly_burn = None
        cash_runway_quarters = None
    elif operating_cash_flow >= 0:
        # Profitable on cash basis — runway not meaningful
        flags.append("positive_cashflow")
        quarterly_burn = None
        cash_runway_quarters = None
    else:
        quarterly_burn = operating_cash_flow / 4.0  # negative
        if cash_and_equivalents is not None and quarterly_burn != 0:
            cash_runway_quarters = min(
                cash_and_equivalents / abs(quarterly_burn),
                XBRL_RUNWAY_CAP_QUARTERS,
            )
        else:
            cash_runway_quarters = None

    # ── Sector heuristics for flagging ──────────────────────────────────────
    if (
        operating_income is None
        and total_assets is not None
        and total_assets > 1_000_000_000_000  # $1T → mega-cap bank
    ):
        flags.append("bank_income_statement")
    # REIT structure: NOT already flagged as bank, but balance sheet is mostly
    # liabilities and there's no cost-of-revenue (gross_profit = None).
    if (
        "bank_income_statement" not in flags
        and gross_profit is None
        and total_liabilities is not None
        and total_assets is not None
        and total_assets > 0
        and (total_liabilities / total_assets) > 0.7
        and total_assets > 5_000_000_000
    ):
        flags.append("reit_structure")

    return {
        "ticker": ticker,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fiscal_year": fiscal_year,
        "period_end_date": period_end_date,
        "revenue": revenue,
        "revenue_prev_year": revenue_prev_year,
        "revenue_growth_yoy": revenue_growth_yoy,
        "gross_profit": gross_profit,
        "operating_income": operating_income,
        "net_income": net_income,
        "cash_and_equivalents": cash_and_equivalents,
        "long_term_debt": long_term_debt,
        "short_term_debt": short_term_debt,
        "total_debt": total_debt,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "stockholders_equity": stockholders_equity,
        "debt_to_equity": debt_to_equity,
        "shares_outstanding": shares_outstanding,
        "shares_outstanding_1yr_ago": shares_1yr_ago,
        "shares_outstanding_2yr_ago": shares_2yr_ago,
        "shares_outstanding_change_1yr_pct": shares_change_1yr_pct,
        "shares_outstanding_change_2yr_pct": shares_change_2yr_pct,
        "operating_cash_flow": operating_cash_flow,
        "capex": capex,
        "free_cash_flow": free_cash_flow,
        "quarterly_burn": quarterly_burn,
        "cash_runway_quarters": cash_runway_quarters,
        "data_quality_flags": json.dumps(flags),
        "source": "edgar_xbrl",
    }


# ── DB writer ────────────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT OR REPLACE INTO scr_fundamentals_xbrl (
    ticker, fetched_at, fiscal_year, period_end_date,
    revenue, revenue_prev_year, revenue_growth_yoy,
    gross_profit, operating_income, net_income,
    cash_and_equivalents, long_term_debt, short_term_debt, total_debt,
    total_assets, total_liabilities, stockholders_equity, debt_to_equity,
    shares_outstanding, shares_outstanding_1yr_ago, shares_outstanding_2yr_ago,
    shares_outstanding_change_1yr_pct, shares_outstanding_change_2yr_pct,
    operating_cash_flow, capex, free_cash_flow,
    quarterly_burn, cash_runway_quarters,
    data_quality_flags, source
) VALUES (
    :ticker, :fetched_at, :fiscal_year, :period_end_date,
    :revenue, :revenue_prev_year, :revenue_growth_yoy,
    :gross_profit, :operating_income, :net_income,
    :cash_and_equivalents, :long_term_debt, :short_term_debt, :total_debt,
    :total_assets, :total_liabilities, :stockholders_equity, :debt_to_equity,
    :shares_outstanding, :shares_outstanding_1yr_ago, :shares_outstanding_2yr_ago,
    :shares_outstanding_change_1yr_pct, :shares_outstanding_change_2yr_pct,
    :operating_cash_flow, :capex, :free_cash_flow,
    :quarterly_burn, :cash_runway_quarters,
    :data_quality_flags, :source
)
"""


def _write_batch(rows: list[dict[str, Any]], db_path: str | None = None) -> None:
    if not rows:
        return
    path = db_path or _DEFAULT_DB_PATH
    with sqlite3.connect(str(path)) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.executemany(_INSERT_SQL, rows)


# ── Batch orchestrator ──────────────────────────────────────────────────────

def refresh_xbrl_fundamentals(
    tickers: list[str],
    *,
    batch_size: int = XBRL_BATCH_SIZE,
    db_path: str | None = None,
    sleep_per_call: float = XBRL_RATE_LIMIT_SLEEP_SEC,
) -> dict[str, int]:
    """Iterate tickers, extract XBRL, commit per batch.

    Returns counts dict: {ok, failed, batches}.

    Rate-limit defense:
      - sleep(0.1s) between every per-ticker call (leaves SEC headroom under 10 RPS).
      - if a batch errors on >60% of its tickers, halve the batch size & sleep 30s.
    """
    n = len(tickers)
    logger.info("xbrl refresh starting: tickers=%d batch_size=%d", n, batch_size)

    ok = 0
    failed = 0
    batch_n = 0
    i = 0
    cur_batch_size = batch_size

    while i < n:
        batch = tickers[i : i + cur_batch_size]
        batch_n += 1
        batch_rows: list[dict[str, Any]] = []
        batch_errs = 0

        for tkr in batch:
            try:
                row = extract_xbrl_fundamentals(tkr)
            except Exception as exc:
                logger.warning("extract crashed for %s: %s", tkr, exc)
                row = None
            if row is not None:
                batch_rows.append(row)
                ok += 1
            else:
                failed += 1
                batch_errs += 1
            time.sleep(sleep_per_call)

        try:
            _write_batch(batch_rows, db_path=db_path)
        except Exception as exc:
            logger.exception("batch %d write failed: %s", batch_n, exc)

        # Rate-limit detection: if too many errors in this batch, back off.
        err_rate = batch_errs / max(len(batch), 1)
        if err_rate >= XBRL_BATCH_ERROR_THRESHOLD and cur_batch_size > 1:
            new_size = max(cur_batch_size // 2, 1)
            logger.warning(
                "batch %d had %.0f%% error rate — sleeping %ds and halving batch %d→%d",
                batch_n,
                err_rate * 100,
                XBRL_BATCH_BACKOFF_SEC,
                cur_batch_size,
                new_size,
            )
            time.sleep(XBRL_BATCH_BACKOFF_SEC)
            cur_batch_size = new_size

        i += len(batch)
        if batch_n % 5 == 0:
            logger.info(
                "xbrl refresh progress: %d/%d (ok=%d failed=%d)", i, n, ok, failed
            )

    logger.info("xbrl refresh done: ok=%d failed=%d batches=%d", ok, failed, batch_n)
    return {"ok": ok, "failed": failed, "batches": batch_n}


def _all_active_tickers(db_path: str | None = None) -> list[str]:
    path = db_path or _DEFAULT_DB_PATH
    with sqlite3.connect(str(path)) as con:
        rows = con.execute(
            "SELECT ticker FROM scr_universe WHERE is_active = 1 AND is_killed = 0 ORDER BY ticker"
        ).fetchall()
    return [r[0] for r in rows]


# ── Validation report ────────────────────────────────────────────────────────

def _agreement_pct(yf_val: float | None, xbrl_val: float | None, tol: float) -> str:
    """Classify how well a yfinance value agrees with the XBRL value.

    Returns one of: 'within_5', 'within_10', 'divergent', 'yf_only', 'xbrl_only', 'both_null'
    """
    if yf_val is None and xbrl_val is None:
        return "both_null"
    if yf_val is None:
        return "xbrl_only"
    if xbrl_val is None:
        return "yf_only"
    if xbrl_val == 0:
        # avoid div-by-zero; treat as agreeing if yf also zero, else divergent
        return "within_5" if yf_val == 0 else "divergent"
    pct = abs(yf_val - xbrl_val) / abs(xbrl_val)
    if pct <= 0.05:
        return "within_5"
    if pct <= 0.10:
        return "within_10"
    return "divergent"


def generate_validation_report(
    db_path: str | None = None,
    out_dir: str = AMPX_OUTPUT_DIR,
) -> str:
    """Compare scr_fundamentals (yfinance) and scr_fundamentals_xbrl, write report.

    Returns path to the markdown file written.
    """
    path = db_path or _DEFAULT_DB_PATH
    today = _date_class.today().isoformat()

    with sqlite3.connect(str(path)) as con:
        con.row_factory = sqlite3.Row
        # Pair the latest yfinance row with the latest XBRL row per ticker.
        # Use universe so we can also count XBRL-only / yf-only.
        rows = con.execute(
            """
            WITH yf AS (
                SELECT f.ticker,
                       f.revenue_ttm AS yf_revenue,
                       f.cash_and_equivalents AS yf_cash,
                       f.shares_outstanding AS yf_shares,
                       f.total_debt AS yf_debt
                FROM scr_fundamentals f
                INNER JOIN (
                    SELECT ticker, MAX(date) AS d FROM scr_fundamentals GROUP BY ticker
                ) latest ON f.ticker = latest.ticker AND f.date = latest.d
            ),
            xbrl AS (
                SELECT x.ticker,
                       x.revenue AS xbrl_revenue,
                       x.cash_and_equivalents AS xbrl_cash,
                       x.shares_outstanding AS xbrl_shares,
                       x.total_debt AS xbrl_debt,
                       x.data_quality_flags AS flags,
                       x.fiscal_year AS xbrl_fy
                FROM scr_fundamentals_xbrl x
                INNER JOIN (
                    SELECT ticker, MAX(fiscal_year) AS fy
                    FROM scr_fundamentals_xbrl GROUP BY ticker
                ) latest ON x.ticker = latest.ticker AND x.fiscal_year = latest.fy
            )
            SELECT u.ticker,
                   yf.yf_revenue, yf.yf_cash, yf.yf_shares, yf.yf_debt,
                   xbrl.xbrl_revenue, xbrl.xbrl_cash, xbrl.xbrl_shares, xbrl.xbrl_debt,
                   xbrl.flags, xbrl.xbrl_fy
            FROM scr_universe u
            LEFT JOIN yf   ON u.ticker = yf.ticker
            LEFT JOIN xbrl ON u.ticker = xbrl.ticker
            WHERE u.is_active = 1 AND u.is_killed = 0
            ORDER BY u.ticker
            """
        ).fetchall()

    universe_n = len(rows)
    xbrl_pop = sum(1 for r in rows if r["xbrl_revenue"] is not None)
    yf_pop = sum(1 for r in rows if r["yf_revenue"] is not None)
    overlap = [r for r in rows if r["xbrl_revenue"] is not None and r["yf_revenue"] is not None]
    overlap_n = len(overlap)

    # Per-field tallies on the overlap
    fields = {
        "revenue": ("yf_revenue", "xbrl_revenue", 0.05),
        "cash": ("yf_cash", "xbrl_cash", 0.10),
        "shares": ("yf_shares", "xbrl_shares", 0.10),
        "debt": ("yf_debt", "xbrl_debt", 0.10),
    }
    tallies: dict[str, dict[str, int]] = {}
    for label, (yf_col, xbrl_col, tol) in fields.items():
        bucket = {"within_5": 0, "within_10": 0, "divergent": 0, "yf_only": 0, "xbrl_only": 0, "both_null": 0}
        for r in rows:
            cls = _agreement_pct(r[yf_col], r[xbrl_col], tol)
            bucket[cls] += 1
        tallies[label] = bucket

    # Top-20 divergence (revenue-based) on the overlap
    divergent_rows: list[tuple[str, float, float, float]] = []
    for r in overlap:
        yf_rev = r["yf_revenue"]
        xbrl_rev = r["xbrl_revenue"]
        if xbrl_rev is None or xbrl_rev == 0:
            continue
        pct = abs(yf_rev - xbrl_rev) / abs(xbrl_rev) * 100.0
        divergent_rows.append((r["ticker"], yf_rev, xbrl_rev, pct))
    divergent_rows.sort(key=lambda t: t[3], reverse=True)
    top_divergent = divergent_rows[:20]

    # Data-quality flag tally
    flag_counts: dict[str, int] = {}
    for r in rows:
        if not r["flags"]:
            continue
        try:
            for f in json.loads(r["flags"]):
                flag_counts[f] = flag_counts.get(f, 0) + 1
        except json.JSONDecodeError:
            continue

    # ── Build markdown ──────────────────────────────────────────────────────
    md: list[str] = []
    md.append(f"# XBRL Fundamentals Validation Report — {today}")
    md.append("")
    md.append("Compares `scr_fundamentals` (yfinance, latest date per ticker) against")
    md.append("`scr_fundamentals_xbrl` (EDGAR XBRL, latest fiscal_year per ticker).")
    md.append("")
    md.append("## Coverage")
    md.append("")
    md.append(f"- Active universe (`scr_universe`): **{universe_n}**")
    md.append(f"- yfinance-populated (`scr_fundamentals`): **{yf_pop}** ({yf_pop / max(universe_n,1) * 100:.1f}%)")
    md.append(f"- XBRL-populated (`scr_fundamentals_xbrl`): **{xbrl_pop}** ({xbrl_pop / max(universe_n,1) * 100:.1f}%)")
    md.append(f"- Overlap (both sources): **{overlap_n}**")
    md.append("")
    md.append("## Per-field agreement (over the full universe)")
    md.append("")
    md.append("| Field | Within 5% | Within 10% | Divergent >10% | yf-only | xbrl-only | both null |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    for label, b in tallies.items():
        md.append(
            f"| {label} | {b['within_5']} | {b['within_10']} | {b['divergent']} | "
            f"{b['yf_only']} | {b['xbrl_only']} | {b['both_null']} |"
        )
    md.append("")
    md.append("### Agreement on the overlap")
    md.append("")
    md.append("| Field | Within 5% | Within 10% | Divergent | Overlap denom |")
    md.append("|---|---:|---:|---:|---:|")
    for label, b in tallies.items():
        denom = b["within_5"] + b["within_10"] + b["divergent"]
        if denom == 0:
            md.append(f"| {label} | 0 | 0 | 0 | 0 |")
            continue
        pct5 = b["within_5"] / denom * 100
        pct10 = (b["within_5"] + b["within_10"]) / denom * 100
        pctd = b["divergent"] / denom * 100
        md.append(
            f"| {label} | {b['within_5']} ({pct5:.1f}%) | "
            f"{b['within_10']} ({pct10:.1f}%) | {b['divergent']} ({pctd:.1f}%) | {denom} |"
        )
    md.append("")
    md.append("## Top-20 divergence (by revenue, % gap on overlap)")
    md.append("")
    md.append("| Ticker | yfinance revenue | XBRL revenue | Gap % |")
    md.append("|---|---:|---:|---:|")
    for tkr, yf_rev, xbrl_rev, pct in top_divergent:
        md.append(f"| {tkr} | {yf_rev:,.0f} | {xbrl_rev:,.0f} | {pct:.1f}% |")
    md.append("")
    md.append("## Data-quality flag tally")
    md.append("")
    if flag_counts:
        md.append("| Flag | Count |")
        md.append("|---|---:|")
        for f, n in sorted(flag_counts.items(), key=lambda kv: kv[1], reverse=True):
            md.append(f"| `{f}` | {n} |")
    else:
        md.append("(no flags recorded — XBRL table is empty or all rows are clean)")
    md.append("")

    # ── Acceptance gate verdict ─────────────────────────────────────────────
    md.append("## Acceptance gate")
    md.append("")
    md.append("Per plan section _Acceptance Criteria_:")
    md.append("- Revenue agreement >= 85% within 5% (overlap)")
    md.append("- Cash agreement >= 85% within 10% (overlap)")
    md.append("- Coverage >= 80% of universe")
    md.append("")

    def _overlap_pct(label: str, key: str) -> float:
        b = tallies[label]
        denom = b["within_5"] + b["within_10"] + b["divergent"]
        if denom == 0:
            return 0.0
        if key == "5":
            return b["within_5"] / denom * 100
        return (b["within_5"] + b["within_10"]) / denom * 100

    rev_5 = _overlap_pct("revenue", "5")
    cash_10 = _overlap_pct("cash", "10")
    cov_pct = xbrl_pop / max(universe_n, 1) * 100

    pass_rev = rev_5 >= 85.0
    pass_cash = cash_10 >= 85.0
    pass_cov = cov_pct >= 80.0
    md.append(f"- Revenue within-5% on overlap: **{rev_5:.1f}%** {'PASS' if pass_rev else 'FAIL'}")
    md.append(f"- Cash within-10% on overlap: **{cash_10:.1f}%** {'PASS' if pass_cash else 'FAIL'}")
    md.append(f"- Coverage: **{cov_pct:.1f}%** {'PASS' if pass_cov else 'FAIL'}")
    md.append("")

    md.append("### Methodology caveat")
    md.append("")
    md.append(
        "The gate measures XBRL against yfinance. That's a circular test — "
        "yfinance is the thing we're migrating *away from* because it's known "
        "to be wrong on a meaningful subset of tickers (biotechs reported as "
        "$100B+ revenue, clinical-stage names as $183M when actual is $237K). "
        "A 100% agreement result would mean XBRL inherits every yfinance bug; "
        "the actual target is agreement with the **10-K filing**, not with "
        "yfinance. See follow-up below."
    )
    md.append("")

    if pass_rev and pass_cash and pass_cov:
        md.append(
            "**Verdict: GATE PASS — XBRL agrees with yfinance on the overlap. "
            "Flipping `FUNDAMENTALS_SOURCE = 'edgar_xbrl'` is safe on the "
            "agreement metric, but the 10-K spot-check in the follow-up below "
            "should still be run to confirm XBRL is right where yfinance is "
            "wrong.**"
        )
    elif pass_cov:
        md.append(
            "**Verdict: COVERAGE PASS, agreement below 85% — ship "
            "`FUNDAMENTALS_SOURCE = 'yfinance_xbrl_fallback'` (hybrid mode). "
            "yfinance stays primary for tickers where it already produces "
            "usable data; XBRL rescues the null-yfinance cases. Zero "
            "regression, delivers the migration's original goal, defers the "
            "full swap until the 10-K validation below is run.**"
        )
    else:
        md.append(
            "**Verdict: COVERAGE FAIL — keep `FUNDAMENTALS_SOURCE = 'yfinance'`. "
            "Investigate extractor failure-rate before any deployment.**"
        )
    md.append("")

    md.append("### Follow-up validation (required for full edgar_xbrl flip)")
    md.append("")
    md.append(
        "Pull 50 tickers where XBRL and yfinance disagree by >20% on revenue. "
        "Manually check 20 of them against the actual SEC 10-K filing. For each, "
        "score which source (yf or XBRL) matches the 10-K's reported figure. "
        "If XBRL wins ≥15/20, flip `FUNDAMENTALS_SOURCE = 'edgar_xbrl'` with "
        "confidence. If closer to 10/10, the extractor still has bugs worth "
        "fixing before a full swap."
    )
    md.append("")

    # ── Write markdown ──────────────────────────────────────────────────────
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f"xbrl_validation_{today}.md"
    out_path.write_text("\n".join(md))
    logger.info("validation report written: %s", out_path)
    return str(out_path)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sec_filing_intelligence.xbrl_fundamentals",
        description="EDGAR XBRL fundamentals extractor for the AMPX screener.",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Pull XBRL for every active universe ticker."
    )
    parser.add_argument(
        "--ticker", metavar="TICK", help="Pull XBRL for a single ticker and print the row."
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Generate validation report comparing yfinance and XBRL fundamentals.",
    )
    args = parser.parse_args(argv)

    if args.refresh:
        tickers = _all_active_tickers()
        if not tickers:
            print("No active tickers in scr_universe.")
            return 1
        counts = refresh_xbrl_fundamentals(tickers)
        print(
            f"refresh complete: ok={counts['ok']} failed={counts['failed']} batches={counts['batches']}"
        )
        return 0

    if args.ticker:
        row = extract_xbrl_fundamentals(args.ticker.upper())
        if row is None:
            print(f"{args.ticker.upper()}: no XBRL data")
            return 1
        # Pretty-print
        for k in sorted(row):
            v = row[k]
            if isinstance(v, float):
                print(f"  {k}: {v:,.2f}")
            else:
                print(f"  {k}: {v}")
        # Persist on success so single-ticker runs are useful
        _write_batch([row])
        return 0

    if args.validate:
        path = generate_validation_report()
        print(f"validation report: {path}")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
