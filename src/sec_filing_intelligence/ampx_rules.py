"""AMPX Rules Screener — threshold-based scoring over the existing universe.

Reads scr_universe + scr_fundamentals + scr_price_metrics + scr_insider_transactions
+ scr_kill_list, filters via cheap SQL gates, does on-demand LEAPS + going-concern
checks for survivors, computes an 11-dimension score, writes scr_ampx_rules_scores.

Part of the SEC Filing Intelligence system.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import resource
import sqlite3
import sys
import time
from datetime import date as _date_class
from typing import Any

from .utils import get_logger

logger = get_logger("ampx_rules")

from .config import (
    AMPX_DILUTION_TIER_HEAVY_PCT,
    AMPX_DILUTION_TIER_MILD_PCT,
    AMPX_DILUTION_TIER_MODERATE_PCT,
    AMPX_GOING_CONCERN_PATTERNS,
    AMPX_INSIDER_CLUSTER_MIN_COUNT, AMPX_INSIDER_CLUSTER_WINDOW_DAYS,
    AMPX_INSIDER_LOOKBACK_DAYS,
    AMPX_MAX_MARKET_CAP_M, AMPX_MIN_52W_DECLINE_PCT,
    AMPX_MIN_CRASH_52W_PCT, AMPX_MIN_CRASH_ATH_PCT,
    AMPX_MIN_LIQUIDITY_USD, AMPX_MIN_MARKET_CAP_M, AMPX_OUTPUT_DIR,
    AMPX_PRIORITY_INDUSTRIES, AMPX_TARGET_SECTORS,
    FUNDAMENTALS_SOURCE,
)
from .config import DB_PATH as _DEFAULT_DB_PATH


# yfinance-only candidate query (default mode). All financials from scr_fundamentals.
_CANDIDATES_SQL_YFINANCE = """
SELECT
    u.ticker, u.company_name, u.sector, u.industry, u.market_cap, u.avg_volume,
    f.revenue_growth_yoy, f.revenue_growth_qoq, f.revenue_ttm,
    f.debt_to_equity,
    f.cash_runway_quarters, f.shares_outstanding, f.institutional_ownership,
    ac.analyst_count,
    f.cash_and_equivalents, f.total_debt,
    pm.current_price, pm.pct_from_52w_high, pm.pct_from_ath,
    pm.float_shares, pm.short_interest_pct,
    NULL AS shares_outstanding_change_2yr_pct,
    NULL AS xbrl_data_quality_flags
FROM scr_universe u
INNER JOIN (
    SELECT ticker, market_cap, revenue_growth_yoy, revenue_growth_qoq, revenue_ttm,
           debt_to_equity, cash_runway_quarters, shares_outstanding,
           institutional_ownership, cash_and_equivalents, total_debt,
           MAX(date) AS _latest_date
    FROM scr_fundamentals
    GROUP BY ticker
) f ON u.ticker = f.ticker
INNER JOIN scr_price_metrics pm ON u.ticker = pm.ticker
LEFT JOIN scr_kill_list kl ON u.ticker = kl.ticker
LEFT JOIN (
    SELECT ticker, analyst_count, MAX(date) AS _latest_date
    FROM scr_analyst_coverage
    GROUP BY ticker
) ac ON u.ticker = ac.ticker
WHERE u.is_active = 1
  AND u.is_killed = 0
  AND kl.ticker IS NULL
  AND f.market_cap BETWEEN ? AND ?
  AND (pm.pct_from_52w_high <= ? OR pm.pct_from_ath <= ?)
  -- 52w-decline floor applied universally: an ATH-only qualifier sitting near
  -- its 52w high is a slow grinder, not a washout. Real AMPX candidates clear
  -- both gates comfortably.
  AND pm.pct_from_52w_high <= ?
  AND (u.avg_volume * pm.current_price) >= ?
  AND u.sector IN ({sectors})
  -- Revenue-growth gate: exclude terminal-decline names; must be positive on at least one axis.
  -- Tickers with both NULL are also excluded (no data = not a candidate).
  AND (
      f.revenue_growth_yoy > 0
      OR f.revenue_growth_qoq > 0
  )
ORDER BY u.ticker
"""


# XBRL-preferred candidate query. Uses COALESCE(xbrl_field, yf_field) for every
# fundamental — XBRL is the primary source, yfinance fills gaps where XBRL has
# no data (foreign issuers, recent IPOs, REITs with stale dei tags, etc.).
# This is the policy that prevents reintroducing the yfinance-null silent-drop
# bug under XBRL: a ticker missing XBRL still scores via yfinance fallback.
_CANDIDATES_SQL_XBRL = """
SELECT
    u.ticker, u.company_name, u.sector, u.industry, u.market_cap, u.avg_volume,
    -- XBRL is primary; yfinance is the fallback for every field
    COALESCE(fx.revenue_growth_yoy, fy.revenue_growth_yoy) AS revenue_growth_yoy,
    fy.revenue_growth_qoq,
    COALESCE(fx.revenue, fy.revenue_ttm) AS revenue_ttm,
    COALESCE(fx.debt_to_equity, fy.debt_to_equity) AS debt_to_equity,
    COALESCE(fx.cash_runway_quarters, fy.cash_runway_quarters) AS cash_runway_quarters,
    COALESCE(fx.shares_outstanding, fy.shares_outstanding) AS shares_outstanding,
    fy.institutional_ownership,
    ac.analyst_count,
    COALESCE(fx.cash_and_equivalents, fy.cash_and_equivalents) AS cash_and_equivalents,
    COALESCE(fx.total_debt, fy.total_debt) AS total_debt,
    pm.current_price, pm.pct_from_52w_high, pm.pct_from_ath,
    pm.float_shares, pm.short_interest_pct,
    -- XBRL-only fields (no yf equivalent)
    fx.shares_outstanding_change_2yr_pct,
    fx.data_quality_flags AS xbrl_data_quality_flags
FROM scr_universe u
INNER JOIN scr_price_metrics pm ON u.ticker = pm.ticker
LEFT JOIN (
    SELECT ticker, market_cap, revenue_growth_yoy, revenue_growth_qoq, revenue_ttm,
           debt_to_equity, cash_runway_quarters, shares_outstanding,
           institutional_ownership, cash_and_equivalents, total_debt,
           MAX(date) AS _latest_date
    FROM scr_fundamentals
    GROUP BY ticker
) fy ON u.ticker = fy.ticker
LEFT JOIN (
    SELECT ticker, revenue, revenue_growth_yoy, debt_to_equity, cash_runway_quarters,
           shares_outstanding, cash_and_equivalents, total_debt,
           shares_outstanding_change_2yr_pct, data_quality_flags,
           MAX(fiscal_year) AS _latest_fy
    FROM scr_fundamentals_xbrl
    GROUP BY ticker
) fx ON u.ticker = fx.ticker
LEFT JOIN scr_kill_list kl ON u.ticker = kl.ticker
LEFT JOIN (
    SELECT ticker, analyst_count, MAX(date) AS _latest_date
    FROM scr_analyst_coverage
    GROUP BY ticker
) ac ON u.ticker = ac.ticker
-- Market cap still comes from yfinance (XBRL doesn't carry shares-issued × price);
-- if yf row missing, fall back to whatever the XBRL row implies.
WHERE u.is_active = 1
  AND u.is_killed = 0
  AND kl.ticker IS NULL
  AND COALESCE(fy.market_cap, u.market_cap_m * 1000000) BETWEEN ? AND ?
  AND (pm.pct_from_52w_high <= ? OR pm.pct_from_ath <= ?)
  AND pm.pct_from_52w_high <= ?
  AND (u.avg_volume * pm.current_price) >= ?
  AND u.sector IN ({sectors})
  -- Revenue-growth gate: prefer XBRL YoY; fall back to yfinance YoY/QoQ.
  AND (
      COALESCE(fx.revenue_growth_yoy, fy.revenue_growth_yoy) > 0
      OR fy.revenue_growth_qoq > 0
  )
  -- Accept either source; require at least one revenue + cash anchor
  AND COALESCE(fx.revenue, fy.revenue_ttm) IS NOT NULL
  AND COALESCE(fx.cash_and_equivalents, fy.cash_and_equivalents) IS NOT NULL
ORDER BY u.ticker
"""


_CANDIDATES_SQL_HYBRID = """
SELECT
    u.ticker, u.company_name, u.sector, u.industry, u.market_cap, u.avg_volume,
    -- yfinance primary for most fields; XBRL rescues NULL-yf tickers (safe first deployment).
    -- EXCEPTION: revenue_growth_yoy prefers XBRL when available. yfinance's revenueGrowth
    -- is most-recent-quarter YoY (noisy for fiscal-year misalignment), while XBRL's value
    -- is FY-annual vs prior FY — the right signal for AMPX's inflection thesis.
    -- Verified via ABAT 10-K (2026-04-15): XBRL 1149% matched filing exactly; yf 88%.
    COALESCE(fx.revenue_growth_yoy, fy.revenue_growth_yoy) AS revenue_growth_yoy,
    fy.revenue_growth_qoq,
    COALESCE(fy.revenue_ttm, fx.revenue) AS revenue_ttm,
    COALESCE(fy.debt_to_equity, fx.debt_to_equity) AS debt_to_equity,
    COALESCE(fy.cash_runway_quarters, fx.cash_runway_quarters) AS cash_runway_quarters,
    COALESCE(fy.shares_outstanding, fx.shares_outstanding) AS shares_outstanding,
    fy.institutional_ownership,
    ac.analyst_count,
    COALESCE(fy.cash_and_equivalents, fx.cash_and_equivalents) AS cash_and_equivalents,
    COALESCE(fy.total_debt, fx.total_debt) AS total_debt,
    pm.current_price, pm.pct_from_52w_high, pm.pct_from_ath,
    pm.float_shares, pm.short_interest_pct,
    -- XBRL-only fields (no yf equivalent)
    fx.shares_outstanding_change_2yr_pct,
    fx.data_quality_flags AS xbrl_data_quality_flags
FROM scr_universe u
INNER JOIN scr_price_metrics pm ON u.ticker = pm.ticker
LEFT JOIN (
    SELECT ticker, market_cap, revenue_growth_yoy, revenue_growth_qoq, revenue_ttm,
           debt_to_equity, cash_runway_quarters, shares_outstanding,
           institutional_ownership, cash_and_equivalents, total_debt,
           MAX(date) AS _latest_date
    FROM scr_fundamentals
    GROUP BY ticker
) fy ON u.ticker = fy.ticker
LEFT JOIN (
    SELECT ticker, revenue, revenue_growth_yoy, debt_to_equity, cash_runway_quarters,
           shares_outstanding, cash_and_equivalents, total_debt,
           shares_outstanding_change_2yr_pct, data_quality_flags,
           MAX(fiscal_year) AS _latest_fy
    FROM scr_fundamentals_xbrl
    GROUP BY ticker
) fx ON u.ticker = fx.ticker
LEFT JOIN scr_kill_list kl ON u.ticker = kl.ticker
LEFT JOIN (
    SELECT ticker, analyst_count, MAX(date) AS _latest_date
    FROM scr_analyst_coverage
    GROUP BY ticker
) ac ON u.ticker = ac.ticker
WHERE u.is_active = 1
  AND u.is_killed = 0
  AND kl.ticker IS NULL
  -- Market cap: prefer yf.market_cap (most accurate when present), fall back to universe.market_cap
  AND COALESCE(fy.market_cap, u.market_cap) BETWEEN ? AND ?
  AND (pm.pct_from_52w_high <= ? OR pm.pct_from_ath <= ?)
  AND pm.pct_from_52w_high <= ?
  AND (u.avg_volume * pm.current_price) >= ?
  AND u.sector IN ({sectors})
  -- Revenue-growth gate: XBRL-primary (FY-vs-FY) with yf fallback; qoq from yf.
  AND (
      COALESCE(fx.revenue_growth_yoy, fy.revenue_growth_yoy) > 0
      OR fy.revenue_growth_qoq > 0
  )
ORDER BY u.ticker
"""


def _candidates_sql() -> str:
    """Pick the candidate SQL based on FUNDAMENTALS_SOURCE config flag.

    Three modes:
    - `yfinance` (legacy): yf only, no XBRL.
    - `yfinance_xbrl_fallback` (hybrid, current default): yf primary, XBRL
      rescues null-yf tickers. Zero-regression deployment that delivers the
      migration's original goal (populate the ~300 tickers yfinance missed)
      without depending on the XBRL extractor being right in the divergences
      against yfinance — because we haven't verified that against 10-K
      filings yet.
    - `edgar_xbrl` (future): XBRL primary, yf fallback. Gated on a spot-check
      study that pulls 50 divergent tickers and compares each source against
      the actual 10-K. Until that study shows XBRL wins in the majority of
      divergences, we don't flip here.
    """
    if FUNDAMENTALS_SOURCE == "edgar_xbrl":
        return _CANDIDATES_SQL_XBRL
    if FUNDAMENTALS_SOURCE == "yfinance_xbrl_fallback":
        return _CANDIDATES_SQL_HYBRID
    return _CANDIDATES_SQL_YFINANCE


# Back-compat: keep _CANDIDATES_SQL as an alias for the yfinance variant, since
# tests import it directly. New code should call _candidates_sql().
_CANDIDATES_SQL = _CANDIDATES_SQL_YFINANCE


def fetch_candidates(db_path: str | None = None) -> list[dict[str, Any]]:
    """Apply cheap SQL gates; return list of dict rows for survivors.

    Gates (in order):
      1. Active universe, not killed, not in scr_kill_list
      2. Market cap in [$30M, $500M]  -- NOTE: AMPX_MIN_MARKET_CAP_M / AMPX_MAX_MARKET_CAP_M are in millions
      3. Crashed: pct_from_52w_high <= -60 OR pct_from_ath <= -70 (both are NEGATIVE in scr_price_metrics)
      4. Liquidity: avg_volume * current_price >= $500K/day
      5. Target sector (strict -- 'Unknown' excluded)
    """
    path = db_path or _DEFAULT_DB_PATH
    sector_placeholders = ",".join("?" * len(AMPX_TARGET_SECTORS))
    sql = _candidates_sql().format(sectors=sector_placeholders)

    params: list[Any] = [
        AMPX_MIN_MARKET_CAP_M * 1_000_000,
        AMPX_MAX_MARKET_CAP_M * 1_000_000,
        AMPX_MIN_CRASH_52W_PCT,
        AMPX_MIN_CRASH_ATH_PCT,
        AMPX_MIN_52W_DECLINE_PCT,
        AMPX_MIN_LIQUIDITY_USD,
        *AMPX_TARGET_SECTORS,
    ]
    with sqlite3.connect(str(path)) as con:
        con.row_factory = sqlite3.Row
        rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    logger.info("fetch_candidates: %d survivors of SQL gates", len(rows))
    return rows


# ── Scoring primitives ────────────────────────────────────────────────────────

# Matches any priority keyword at a word boundary, with an optional trailing 's'
# so yfinance plural industry strings ('Semiconductors', 'Airlines') still match
# the singular-form keywords ('semiconductor'). Still safe against AI/AIRLINE —
# the right-side \b blocks matches inside longer words.
_PRIORITY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(kw) for kw in AMPX_PRIORITY_INDUSTRIES) + r")s?\b",
    re.IGNORECASE,
)


def score_priority_industry(company_name: str | None, industry: str | None) -> float:
    """Dimension 8: 1.0 if any priority keyword matches the company name or industry string
    at a word boundary, else 0.0. Word-boundary required so 'AI' does not match 'AIRLINE'.
    """
    text = " ".join(filter(None, [company_name or "", industry or ""]))
    return 1.0 if _PRIORITY_PATTERN.search(text) else 0.0


# ── Going-concern hard kill ───────────────────────────────────────────────────

_GOING_CONCERN_COMPILED = [
    re.compile(p, re.IGNORECASE | re.DOTALL) for p in AMPX_GOING_CONCERN_PATTERNS
]


def scan_going_concern_text(text: str | None) -> str | None:
    """Return the first matched phrase (up to 200 chars) or None."""
    if not text:
        return None
    for pat in _GOING_CONCERN_COMPILED:
        m = pat.search(text)
        if m:
            return m.group(0)[:200]  # cap logged phrase length
    return None


def check_going_concern(ticker: str) -> str | None:
    """Fetch the most recent 10-K via edgartools; return matched phrase or None.

    On any edgartools/network failure, log at debug level and return None
    (do NOT hard-kill on absent data).
    """
    import os
    # edgartools requires EDGAR_IDENTITY. Set a default matching EDGAR_USER_AGENT convention.
    os.environ.setdefault("EDGAR_IDENTITY", "SECFilingIntelligence research@example.com")
    try:
        from edgar import Company  # provided by edgartools package
        company = Company(ticker)
        filings = company.get_filings(form="10-K")
        if not filings:
            return None
        filing = filings.latest(1)
        if filing is None:
            return None
        # edgartools filing objects expose text via `.text()` or may need `.obj()` — try both
        if hasattr(filing, "text"):
            text = filing.text()
        elif hasattr(filing, "obj"):
            text = str(filing.obj())
        else:
            text = str(filing)
        return scan_going_concern_text(text)
    except Exception as exc:
        logger.debug("going-concern check failed for %s: %s", ticker, exc)
        return None


# ── LEAPS availability check ──────────────────────────────────────────────────

from datetime import date, datetime


def _yf_ticker(symbol: str):
    """Indirection for test mocking — avoids importing yfinance at module load time
    and lets tests substitute a fake Ticker class.
    """
    import yfinance as yf
    return yf.Ticker(symbol)


def check_leaps(ticker: str) -> str:
    """Return 'leaps' (>=365d out), 'options' (chain exists, nothing that far), or 'none'."""
    try:
        t = _yf_ticker(ticker)
        exps = list(t.options or [])
        if not exps:
            return "none"
        today = date.today()
        for exp_str in exps:
            try:
                d = datetime.strptime(exp_str, "%Y-%m-%d").date()
                if (d - today).days >= 365:
                    return "leaps"
            except ValueError:
                continue
        return "options"
    except Exception as exc:
        logger.debug("LEAPS check failed for %s: %s", ticker, exc)
        return "none"


def score_leaps(status: str) -> float:
    """Dimension 10: LEAPS=0.5, regular options=0.25, none=0.0 (max 0.5)."""
    return {"leaps": 0.5, "options": 0.25, "none": 0.0}.get(status, 0.0)


# ── Reverse-split hard kill ───────────────────────────────────────────────────


def check_reverse_splits(ticker: str, lookback_years: int = 3, threshold: int = 2) -> int:
    """Return count of reverse splits in the last N years.

    yfinance represents reverse splits as fractional ratios (<1.0) in Ticker.splits.
    Threshold: >= 2 reverse splits in 3 years = equity destruction pattern, caller
    should hard-kill the ticker.
    """
    from datetime import datetime, timedelta, timezone
    try:
        t = _yf_ticker(ticker)
        splits = t.splits
        if splits is None or len(splits) == 0:
            return 0
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=365 * lookback_years)
        count = 0
        for date_idx, ratio in splits.items():
            # date_idx is a pandas Timestamp (tz-aware from yfinance)
            try:
                d = date_idx.to_pydatetime() if hasattr(date_idx, 'to_pydatetime') else date_idx
                if d.tzinfo is None:
                    d = d.replace(tzinfo=timezone.utc)
                if d >= cutoff and ratio < 1.0:
                    count += 1
            except Exception:
                continue
        return count
    except Exception as exc:
        logger.debug("reverse-split check failed for %s: %s", ticker, exc)
        return 0


# ── Insider-buying aggregator (dimension 11) ──────────────────────────────────

from datetime import timedelta


def count_insider_buys(ticker: str, as_of: str, db_path: str | None = None) -> dict[str, int]:
    """Count Form-4 open-market buys (transaction_code='P') for ticker in the last
    AMPX_INSIDER_LOOKBACK_DAYS days, plus cluster count.

    A cluster event = AMPX_INSIDER_CLUSTER_MIN_COUNT or more distinct insiders
    within a rolling AMPX_INSIDER_CLUSTER_WINDOW_DAYS window.

    Returns dict with keys buy_count, cluster_count.
    """
    path = db_path or _DEFAULT_DB_PATH
    as_of_d = datetime.strptime(as_of, "%Y-%m-%d").date()
    lookback_start = (as_of_d - timedelta(days=AMPX_INSIDER_LOOKBACK_DAYS)).isoformat()

    with sqlite3.connect(str(path)) as con:
        buys = con.execute(
            """
            SELECT filing_date, insider_name
            FROM scr_insider_transactions
            WHERE ticker = ?
              AND is_open_market = 1
              AND transaction_type = 'purchase'
              AND is_amended = 0
              AND filing_date >= ?
              AND filing_date <= ?
            ORDER BY filing_date
            """,
            (ticker, lookback_start, as_of),
        ).fetchall()

    buy_count = len(buys)
    cluster_count = 0
    if buy_count >= AMPX_INSIDER_CLUSTER_MIN_COUNT:
        dates = [datetime.strptime(d, "%Y-%m-%d").date() for d, _ in buys]
        insiders = [n for _, n in buys]
        for i, anchor in enumerate(dates):
            window_end = anchor + timedelta(days=AMPX_INSIDER_CLUSTER_WINDOW_DAYS)
            window_insiders = {
                insiders[j] for j, d in enumerate(dates)
                if anchor <= d <= window_end
            }
            if len(window_insiders) >= AMPX_INSIDER_CLUSTER_MIN_COUNT:
                cluster_count += 1

    return {"buy_count": buy_count, "cluster_count": cluster_count}


def score_insider_buying(agg: dict[str, int]) -> float:
    """Dimension 11: cluster threshold → 1.0; any buys at all → 0.5; none → 0.0."""
    if agg["cluster_count"] >= AMPX_INSIDER_CLUSTER_MIN_COUNT:
        return 1.0
    if agg["buy_count"] >= 1:
        return 0.5
    return 0.0


# ── 11-dimension composer ─────────────────────────────────────────────────────


def _gc_penalty(
    has_going_concern: bool,
    runway_quarters: float | None,
    revenue_growth_yoy: float | None,
) -> float:
    """Tiered going-concern penalty. Zero when no GC flag.

    Runway tier (how close to running out of cash):
      >= 6 quarters       → -0.5 (boilerplate auditor conservatism)
      3 to 6 quarters     → -1.0 (real risk, needs financing event this year)
      < 3 OR unknown      → -1.5 (genuinely at risk of zero)

    Additional -0.5 if revenue_growth_yoy < 0 (shrinking + burning = the
    profile that actually goes to zero). Range -0.5 to -2.0.
    """
    if not has_going_concern:
        return 0.0
    if runway_quarters is None or runway_quarters < 3:
        base = -1.5
    elif runway_quarters < 6:
        base = -1.0
    else:
        base = -0.5
    shrink = -0.5 if (revenue_growth_yoy is not None and revenue_growth_yoy < 0) else 0.0
    return round(base + shrink, 2)


def score_row(
    row: dict[str, Any],
    leaps_status: str,
    insider_agg: dict[str, int],
    reverse_split_count: int = 0,
    has_going_concern: bool = False,
) -> dict[str, Any]:
    """Compute 12 dimension scores + total + optional GC penalty.

    Max = 12.5 (2x CRASH/REVGROWTH, 1.5x RUNWAY). Dim 12 is the dilution drag
    (-1.0 floor, never adds). Two paths fire the drag:
      a) heavy issuance: 2-yr share count growth > AMPX_DILUTION_DRAG_PCT
      b) apparent reduction + reverse-split history: negative 2-yr change AND
         reverse_split_count >= 1 means the "buyback" is a reverse split in
         disguise (equity destruction, not return of capital).

    Going-concern penalty (Round 6 #4): when has_going_concern=True, applies a
    tiered penalty (-0.5 to -2.0) based on runway + revenue trajectory. GC is
    information, not a verdict — the ticker still gets scored and surfaces in
    output, just with visible risk labeling.
    """
    # Dim 1: Post-hype crash (max 2.0). Values are NEGATIVE. min() gives deeper crash.
    pct_52w = row.get("pct_from_52w_high") or 0
    pct_ath = row.get("pct_from_ath") or 0
    decline = min(pct_52w, pct_ath)
    crash_source = "ATH" if pct_ath < pct_52w else "52W"
    if decline <= -80:
        d1 = 2.0
    elif decline <= -60:
        d1 = 1.0
    else:
        d1 = 0.0

    # Dim 2: Revenue growth (max 2.0). YoY preferred; annualized QoQ fallback with 0.75 haircut.
    # Floor: revenue_ttm must be >= $1M for percentage growth to be meaningful
    # (prevents ATOM-style noise on near-zero revenue bases).
    # XBRL guard: if scaling_bug_suspected is flagged upstream, the growth numbers
    # are derived from mis-scaled data — don't reward them. NXTT had 500%+ apparent
    # growth AND this flag; shipping the flag-consumption closes that hole.
    MIN_REVENUE_TTM = 1_000_000
    revenue_ttm = row.get("revenue_ttm") or 0

    xbrl_flags_raw = row.get("xbrl_data_quality_flags")
    xbrl_flags: list[str] = []
    if xbrl_flags_raw:
        import json as _json
        try:
            parsed = _json.loads(xbrl_flags_raw) if isinstance(xbrl_flags_raw, str) else xbrl_flags_raw
            if isinstance(parsed, list):
                xbrl_flags = [str(f) for f in parsed]
        except (ValueError, TypeError):
            pass

    yoy = row.get("revenue_growth_yoy")
    if yoy is None:
        qoq = row.get("revenue_growth_qoq")
        effective = (qoq * 4) if qoq is not None else None
        haircut = 0.75
    else:
        effective = yoy
        haircut = 1.0
    if "scaling_bug_suspected" in xbrl_flags:
        d2 = 0.0
    elif effective is None or revenue_ttm < MIN_REVENUE_TTM:
        d2 = 0.0
    elif effective >= 1.0:
        d2 = 2.0 * haircut
    elif effective >= 0.5:
        d2 = 1.0 * haircut
    elif effective >= 0.3:
        d2 = 0.5 * haircut
    else:
        d2 = 0.0

    # Dim 3: Low debt (1.0)
    de = row.get("debt_to_equity")
    if de is None:
        d3 = 0.0
    elif de <= 0.1:
        d3 = 1.0
    elif de <= 0.3:
        d3 = 0.5
    else:
        d3 = 0.0

    # Dim 4: Cash runway (max 1.5, was 1.0)
    runway_raw = row.get("cash_runway_quarters") or 0
    runway = min(runway_raw, 40)  # cap — yfinance returns astronomical values for tiny-burn companies
    d4 = 1.5 if runway >= 8 else (0.75 if runway >= 6 else 0.0)

    # Dim 5: Small float (1.0). Prefer float_shares; fall back to shares_outstanding.
    float_shares = row.get("float_shares") or row.get("shares_outstanding") or float("inf")
    d5 = 1.0 if float_shares < 100_000_000 else (0.5 if float_shares < 150_000_000 else 0.0)

    # Dim 6: Low institutional % (1.0)
    inst = row.get("institutional_ownership")
    if inst is None:
        d6 = 0.0
    elif inst < 0.2:
        d6 = 1.0
    elif inst < 0.4:
        d6 = 0.5
    else:
        d6 = 0.0

    # Dim 7: Low analyst coverage (1.0)
    acount = row.get("analyst_count")
    if acount is None:
        d7 = 0.0
    elif acount <= 1:
        d7 = 1.0
    elif acount <= 3:
        d7 = 0.5
    else:
        d7 = 0.0

    # Dim 8: Priority industry (1.0)
    d8 = score_priority_industry(row.get("company_name"), row.get("industry"))

    # Dim 9: Short interest > 15% of float (0.5). Treat raw > 1.0 (>100%) as a data error and zero out.
    si_raw = row.get("short_interest_pct") or 0
    if si_raw > 1.0:
        d9 = 0.0  # physically impossible reading — likely yfinance data error
    elif si_raw > 0.15:
        d9 = 0.5
    else:
        d9 = 0.0

    # Dim 10: LEAPS (0.5)
    d10 = score_leaps(leaps_status)

    # Dim 11: Insider buying (1.0)
    d11 = score_insider_buying(insider_agg)

    # Dim 12: Dilution drag (tiered penalty, XBRL-only, max -1.0 floor).
    # R7 refinement (2026-04-15): tiered thresholds replace R6's binary
    # >80% → -1.0. The binary cut created a cliff — 79% diluter scored the
    # same as 5% diluter. Tiered version catches VUZI (25%), ARBE (40%),
    # PDYN (78%), etc. that previously escaped penalty unscathed.
    #
    # Evaluation order (first match wins):
    #   a) apparent reduction masked by reverse split: negative change AND
    #      caller passed reverse_split_count >= 1 (FCEL/NXTT-style — a 1:33
    #      reverse split reports as -97% shares, which is NOT a buyback).
    #      Stays at -1.0 regardless of absolute magnitude.
    #   b) tiered heavy-issuance thresholds based on 2yr share change:
    #      < 10% = 0.0 (healthy or small secondary — no drag)
    #      10-50% = -0.25 (moderate dilution)
    #      50-100% = -0.5 (significant dilution)
    #      >= 100% = -1.0 (AMPG/ABAT territory — heavy dilution)
    # Silent zero when the XBRL field is absent — we don't punish yfinance-only
    # tickers for missing data.
    dilution_pct = row.get("shares_outstanding_change_2yr_pct")
    if dilution_pct is None:
        d12 = 0.0
    elif dilution_pct < 0 and reverse_split_count >= 1:
        d12 = -1.0  # RS-masked dilution, unchanged from R6
    elif dilution_pct < AMPX_DILUTION_TIER_MILD_PCT:
        d12 = 0.0
    elif dilution_pct < AMPX_DILUTION_TIER_MODERATE_PCT:
        d12 = -0.25
    elif dilution_pct < AMPX_DILUTION_TIER_HEAVY_PCT:
        d12 = -0.5
    else:
        d12 = -1.0  # >= 100%

    # Round 6 #4: tiered going-concern penalty. Runway-capped at 40Q for the
    # penalty computation to match the display cap used elsewhere.
    runway_for_gc = row.get("cash_runway_quarters")
    if runway_for_gc is not None:
        runway_for_gc = min(runway_for_gc, 40)
    gc_penalty = _gc_penalty(
        has_going_concern=has_going_concern,
        runway_quarters=runway_for_gc,
        revenue_growth_yoy=row.get("revenue_growth_yoy"),
    )

    total = round(
        d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 + d10 + d11 + d12 + gc_penalty, 2
    )
    return {
        "ticker": row["ticker"],
        "score": total,
        "dim1_crash": d1, "dim2_revgrowth": round(d2, 3), "dim3_debt": d3, "dim4_runway": d4,
        "dim5_float": d5, "dim6_institutional": d6, "dim7_analyst": d7,
        "dim8_priority_industry": d8, "dim9_short": d9, "dim10_leaps": d10,
        "dim11_insider": d11,
        "dim12_dilution_drag": d12,
        "has_going_concern": bool(has_going_concern),
        "gc_penalty": gc_penalty,
        "_crash_source": crash_source,  # "52W" or "ATH" — which triggered dim1
        "_dilution_pct": dilution_pct,  # for score_details rendering
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sec_filing_intelligence.ampx_rules",
        description="AMPX threshold-based screener over the existing universe.",
    )
    parser.add_argument("--run", action="store_true",
                        help="Run the full scorer end-to-end.")
    parser.add_argument("--no-digest", action="store_true",
                        help="Skip the scan digest log output.")
    parser.add_argument("--no-csv", action="store_true",
                        help="Skip writing the dated CSV.")
    parser.add_argument("--rescore", metavar="TICKER",
                        help="Rescore a single ticker (debugging).")
    parser.add_argument("--top", type=int, metavar="N",
                        help="Print top N from latest scan and exit.")
    parser.add_argument("--trend", metavar="TICKER",
                        help="Show week-over-week score history for TICKER.")
    args = parser.parse_args(argv)

    if args.run:
        return run_full_scan(send_digest=not args.no_digest, write_csv=not args.no_csv)
    if args.rescore:
        return rescore_one(args.rescore)
    if args.top:
        return print_top_n(args.top)
    if args.trend:
        return print_trend(args.trend)

    parser.print_help()
    return 0


def run_full_scan(*, send_digest: bool, write_csv: bool) -> int:
    """End-to-end: fetch candidates, enrich with LEAPS/going-concern/insider data,
    score, write scr_ampx_rules_scores + scr_ampx_red_flags, write CSV, send report.
    """
    # Apply any pending schema migrations first — idempotent and cheap. Without
    # this, adding a new dim column (like dim12_dilution_drag) in a PR-merged
    # DDL update wouldn't reach the live DB until someone manually ran
    # run_migration(), causing executemany to fail on the first automated scan.
    from .db import run_migration
    run_migration()

    t0 = time.time()
    scan_date = _date_class.today().isoformat()
    errors_n = 0
    red_flagged = 0

    candidates = fetch_candidates()
    logger.info("AMPX scan %s: %d candidates after SQL gates", scan_date, len(candidates))

    scored: list[dict[str, Any]] = []
    red_flags: list[tuple[str, str]] = []

    for row in candidates:
        ticker = row["ticker"]
        try:
            # Hard kill: reverse-split equity destruction (2+ RS in 3yr is
            # structurally terminal — no scenario where serial RS is bullish)
            rs_count = check_reverse_splits(ticker)
            if rs_count >= 2:
                red_flags.append((ticker, f"reverse_split_destruction:{rs_count}_splits_3yr"))
                red_flagged += 1
                continue

            # Going-concern: Round 6 #4 — no longer hard-kill. Scored with tiered
            # penalty instead. GC is information, not a verdict.
            gc_phrase = check_going_concern(ticker)
            has_gc = gc_phrase is not None

            leaps_status = check_leaps(ticker)
            insider_agg = count_insider_buys(ticker, scan_date)
            result = score_row(
                row, leaps_status, insider_agg,
                reverse_split_count=rs_count,
                has_going_concern=has_gc,
            )
            if has_gc:
                result["_gc_phrase"] = gc_phrase
            result.update({
                "company_name": row.get("company_name"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "market_cap": row.get("market_cap"),
                "current_price": row.get("current_price"),
                "pct_from_52w_high": row.get("pct_from_52w_high"),
                "pct_from_ath": row.get("pct_from_ath"),
                "revenue_growth_yoy": row.get("revenue_growth_yoy"),
                "cash_and_equivalents": row.get("cash_and_equivalents"),
                "total_debt": row.get("total_debt"),
                "debt_to_equity": row.get("debt_to_equity"),
                "cash_runway_quarters": (
                    min(row["cash_runway_quarters"], 40)
                    if row.get("cash_runway_quarters") is not None else None
                ),
                "float_shares": row.get("float_shares"),
                "institutional_ownership": row.get("institutional_ownership"),
                "analyst_count": row.get("analyst_count"),
                "short_interest_pct": row.get("short_interest_pct"),
                "insider_buys_90d": insider_agg["buy_count"],
                "has_leaps": leaps_status == "leaps",
                "shares_outstanding_change_2yr_pct": row.get("shares_outstanding_change_2yr_pct"),
                "xbrl_data_quality_flags": row.get("xbrl_data_quality_flags"),
                "score_details": _format_details(result),
            })
            scored.append(result)
        except Exception as exc:
            logger.exception("Scoring failed for %s: %s", ticker, exc)
            errors_n += 1

    _persist_scores(scored, scan_date)
    _persist_red_flags(red_flags, scan_date)

    if write_csv:
        _write_csv(scored, scan_date)

    if send_digest:
        _send_digest(scored, scan_date, total_candidates=len(candidates), red_flagged=red_flagged)

    duration = int(time.time() - t0)
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_mb = peak_rss_kb // 1024
    _log_run(scan_date, duration, len(candidates), len(scored), red_flagged, errors_n, peak_rss_mb)

    logger.info(
        "AMPX scan complete: scored=%d red_flagged=%d duration=%ds peak_rss=%dMB",
        len(scored), red_flagged, duration, peak_rss_mb,
    )
    return 0


def _format_details(score: dict[str, Any]) -> str:
    parts = []
    crash_source = score.get("_crash_source", "")
    crash_str = f"CRASH:{score['dim1_crash']}({crash_source})" if crash_source and score["dim1_crash"] > 0 else f"CRASH:{score['dim1_crash']}"
    parts.append(crash_str)
    remaining_labels = [
        ("REVGROWTH", "dim2_revgrowth"),
        ("DEBT", "dim3_debt"), ("RUNWAY", "dim4_runway"),
        ("FLOAT", "dim5_float"), ("INSTOWN", "dim6_institutional"),
        ("ANALYST", "dim7_analyst"), ("PRIORITY", "dim8_priority_industry"),
        ("SHORT", "dim9_short"), ("LEAPS", "dim10_leaps"),
        ("INSIDER", "dim11_insider"),
    ]
    for label, key in remaining_labels:
        parts.append(f"{label}:{score[key]}")
    # Dilution: include the % if known so the audit trail is readable. Use
    # "N/A" for yfinance-only tickers (XBRL data unavailable).
    dilution_pct = score.get("_dilution_pct")
    if dilution_pct is None:
        parts.append("DILUTION:N/A")
    else:
        parts.append(f"DILUTION:{score['dim12_dilution_drag']}({dilution_pct:.0f}%)")
    # Going-concern: only emit when flagged, so clean rows stay uncluttered.
    # Format: GOING_CONCERN:-1.5(2Q) — penalty + runway quarters (or "?Q" if unknown).
    if score.get("has_going_concern"):
        runway = score.get("cash_runway_quarters")
        runway_str = f"{runway:.0f}Q" if runway is not None else "?Q"
        parts.append(f"GOING_CONCERN:{score.get('gc_penalty', 0.0)}({runway_str})")
    return "|".join(parts)


def _persist_scores(scored: list[dict[str, Any]], scan_date: str) -> None:
    if not scored:
        return
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        # Drop any stale rows for this scan_date before writing — otherwise a
        # ticker that passed the gates last run but now gets killed or red-
        # flagged still shows up in scr_ampx_rules_scores (INSERT OR REPLACE
        # matches on PK, doesn't remove rows the new scan no longer produces).
        # Same-day re-scans must be idempotent.
        con.execute(
            "DELETE FROM scr_ampx_rules_scores WHERE scan_date = ?", (scan_date,)
        )
        con.executemany(
            """
            INSERT OR REPLACE INTO scr_ampx_rules_scores (
                ticker, scan_date, score, score_details,
                dim1_crash, dim2_revgrowth, dim3_debt, dim4_runway,
                dim5_float, dim6_institutional, dim7_analyst, dim8_priority_industry,
                dim9_short, dim10_leaps, dim11_insider, dim12_dilution_drag,
                has_going_concern
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (s["ticker"], scan_date, s["score"], s["score_details"],
                 s["dim1_crash"], s["dim2_revgrowth"], s["dim3_debt"], s["dim4_runway"],
                 s["dim5_float"], s["dim6_institutional"], s["dim7_analyst"], s["dim8_priority_industry"],
                 s["dim9_short"], s["dim10_leaps"], s["dim11_insider"],
                 s.get("dim12_dilution_drag", 0.0),
                 1 if s.get("has_going_concern") else 0)
                for s in scored
            ],
        )


def _persist_red_flags(red_flags: list[tuple[str, str]], scan_date: str) -> None:
    """red_flags items are (ticker, matched_phrase). If phrase starts with a flag_type tag
    like 'reverse_split_destruction:...', extract that tag; otherwise default to 'going_concern'.
    """
    if not red_flags:
        return
    rows = []
    for ticker, phrase in red_flags:
        if ':' in phrase and phrase.split(':', 1)[0] in ('reverse_split_destruction', 'going_concern'):
            flag_type, matched = phrase.split(':', 1)
        else:
            flag_type = 'going_concern'
            matched = phrase
        rows.append((ticker, scan_date, flag_type, matched))
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        # Same idempotency rule as _persist_scores — clear the day's rows first.
        con.execute(
            "DELETE FROM scr_ampx_red_flags WHERE scan_date = ?", (scan_date,)
        )
        con.executemany(
            """
            INSERT OR REPLACE INTO scr_ampx_red_flags (ticker, scan_date, flag_type, matched_phrase)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )


def _log_run(scan_date, duration, rows_in, rows_scored, red_flagged, errors_n, peak_rss_mb) -> None:
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO scr_ampx_run_log
              (run_date, duration_sec, rows_in, rows_scored, red_flagged_n, errors_n, peak_rss_mb)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (scan_date, duration, rows_in, rows_scored, red_flagged, errors_n, peak_rss_mb),
        )


def _write_csv(scored: list[dict[str, Any]], scan_date: str) -> str:
    os.makedirs(AMPX_OUTPUT_DIR, exist_ok=True)
    path = f"{AMPX_OUTPUT_DIR}/{scan_date}.csv"
    cols = [
        "score", "ticker", "company_name", "current_price", "market_cap",
        "pct_from_52w_high", "pct_from_ath",
        "revenue_growth_yoy", "cash_and_equivalents", "total_debt",
        "debt_to_equity", "cash_runway_quarters", "float_shares", "institutional_ownership",
        "analyst_count", "short_interest_pct", "insider_buys_90d", "has_leaps",
        "shares_outstanding_change_2yr_pct", "xbrl_data_quality_flags",
        "has_going_concern", "gc_penalty",
        "sector", "industry", "score_details",
    ]
    sorted_rows = sorted(scored, key=lambda r: r.get("score", 0), reverse=True)
    with open(path, "w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in sorted_rows:
            w.writerow(r)
    logger.info("CSV written: %s (%d rows)", path, len(sorted_rows))
    return path


def _send_digest(scored, scan_date, total_candidates, red_flagged):
    """Build the AMPX digest and log it.

    The DB + CSV persistence is the primary output; this produces a human-readable
    summary of the scan results for monitoring.
    """
    from .report import build_ampx_rules_section

    try:
        text = build_ampx_rules_section(scan_date=scan_date)
    except Exception as exc:
        logger.warning("AMPX digest build failed: %s", exc)
        return

    logger.info("AMPX scan digest:\n%s", text)


def rescore_one(ticker: str) -> int:
    """Rescore a single ticker (debugging). Bypasses kill list & SQL gates.

    Pulls both yfinance and XBRL fundamentals so dim12 dilution drag fires
    correctly when XBRL data is available.
    """
    ticker = ticker.upper()
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            """
            SELECT u.ticker, u.company_name, u.sector, u.industry, u.market_cap, u.avg_volume,
                   COALESCE(fx.revenue_growth_yoy, f.revenue_growth_yoy) AS revenue_growth_yoy,
                   f.revenue_growth_qoq,
                   COALESCE(fx.revenue, f.revenue_ttm) AS revenue_ttm,
                   COALESCE(fx.debt_to_equity, f.debt_to_equity) AS debt_to_equity,
                   COALESCE(fx.cash_runway_quarters, f.cash_runway_quarters) AS cash_runway_quarters,
                   COALESCE(fx.shares_outstanding, f.shares_outstanding) AS shares_outstanding,
                   f.institutional_ownership,
                   COALESCE(fx.cash_and_equivalents, f.cash_and_equivalents) AS cash_and_equivalents,
                   COALESCE(fx.total_debt, f.total_debt) AS total_debt,
                   pm.current_price, pm.pct_from_52w_high, pm.pct_from_ath,
                   pm.float_shares, pm.short_interest_pct,
                   fx.shares_outstanding_change_2yr_pct,
                   fx.data_quality_flags AS xbrl_data_quality_flags,
                   (SELECT ac.analyst_count FROM scr_analyst_coverage ac
                    WHERE ac.ticker = u.ticker ORDER BY ac.date DESC LIMIT 1) AS analyst_count
            FROM scr_universe u
            LEFT JOIN (
                SELECT ticker, market_cap, revenue_growth_yoy, revenue_growth_qoq, revenue_ttm,
                       debt_to_equity, cash_runway_quarters, shares_outstanding,
                       institutional_ownership, cash_and_equivalents, total_debt,
                       MAX(date) AS _latest_date
                FROM scr_fundamentals GROUP BY ticker
            ) f ON u.ticker = f.ticker
            LEFT JOIN (
                SELECT ticker, revenue, revenue_growth_yoy, debt_to_equity, cash_runway_quarters,
                       shares_outstanding, cash_and_equivalents, total_debt,
                       shares_outstanding_change_2yr_pct, data_quality_flags,
                       MAX(fiscal_year) AS _latest_fy
                FROM scr_fundamentals_xbrl GROUP BY ticker
            ) fx ON u.ticker = fx.ticker
            LEFT JOIN scr_price_metrics pm ON u.ticker = pm.ticker
            WHERE u.ticker = ?
            """,
            (ticker,),
        ).fetchone()
    if row is None:
        print(f"{ticker}: not in scr_universe")
        return 1
    row = dict(row)
    leaps = check_leaps(ticker)
    insiders = count_insider_buys(ticker, _date_class.today().isoformat())
    rs_count = check_reverse_splits(ticker)
    gc_phrase = check_going_concern(ticker)
    result = score_row(
        row, leaps, insiders,
        reverse_split_count=rs_count,
        has_going_concern=gc_phrase is not None,
    )
    print(f"{ticker}: score={result['score']}/12.5")
    for k in sorted(result):
        if k.startswith("dim"):
            print(f"  {k}: {result[k]}")
    if result.get("_dilution_pct") is not None:
        print(f"  (2yr dilution: {result['_dilution_pct']:.1f}%)")
    if result.get("has_going_concern"):
        print(f"  (GC flagged, penalty: {result.get('gc_penalty')})")
    if rs_count >= 2:
        print(f"  (reverse splits: {rs_count} in 3yr — would be hard-killed in full scan)")
    return 0


def print_top_n(n: int) -> int:
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        latest = con.execute(
            "SELECT MAX(scan_date) FROM scr_ampx_rules_scores"
        ).fetchone()[0]
        if latest is None:
            print("No scans in scr_ampx_rules_scores — run --run first.")
            return 1
        rows = con.execute(
            """
            SELECT s.ticker, s.score, u.company_name
            FROM scr_ampx_rules_scores s
            LEFT JOIN scr_universe u ON s.ticker = u.ticker
            WHERE s.scan_date = ?
            ORDER BY s.score DESC
            LIMIT ?
            """,
            (latest, n),
        ).fetchall()
    print(f"Top {n} AMPX candidates from {latest}:")
    for r in rows:
        print(f"  {r['ticker']:6}  {r['score']:5.1f}  {r['company_name']}")
    return 0


def print_trend(ticker: str) -> int:
    ticker = ticker.upper()
    with sqlite3.connect(str(_DEFAULT_DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT scan_date, score, score_details
            FROM scr_ampx_rules_scores
            WHERE ticker = ?
            ORDER BY scan_date DESC
            LIMIT 12
            """,
            (ticker,),
        ).fetchall()
    if not rows:
        print(f"{ticker}: no scan history")
        return 1
    print(f"{ticker} — last {len(rows)} weekly scans:")
    for r in rows:
        print(f"  {r['scan_date']}  {r['score']:5.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
