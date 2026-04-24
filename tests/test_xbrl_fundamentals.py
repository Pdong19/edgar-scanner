"""Tests for scripts/screener/xbrl_fundamentals.py — EDGAR XBRL extractor.

All tests mock `edgartools` responses; we never hit the live SEC API. Reference
fixtures captured during recon (2026-04-14) and pinned in the plan doc.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sec_filing_intelligence import db as screener_db
from sec_filing_intelligence import xbrl_fundamentals as xf


# ── Helpers ──────────────────────────────────────────────────────────────────


def _stub_query(shares_history: list[tuple[str, float, int, str]] | None = None,
                revenue_history: dict[str, list[tuple[str, float, int, str]]] | None = None):
    """Build a minimal fake `facts.query()` chain that returns canned dataframes.

    shares_history items: (period_end, value, fiscal_year, filing_date)
    revenue_history: dict mapping concept name → same row format

    Anything not matched returns an empty DataFrame.
    """
    shares_history = shares_history or []
    revenue_history = revenue_history or {}

    class _Query:
        def __init__(self):
            self._concept = None

        def by_concept(self, c):
            self._concept = c
            return self

        def by_fiscal_period(self, _p):
            return self

        def to_dataframe(self):
            if self._concept == "us-gaap:CommonStockSharesOutstanding":
                rows = [
                    {"period_end": pe, "numeric_value": v, "fiscal_year": fy, "filing_date": fd}
                    for pe, v, fy, fd in shares_history
                ]
                return pd.DataFrame(rows)
            for concept_key, hist in revenue_history.items():
                if self._concept == concept_key:
                    rows = [
                        {"period_end": pe, "numeric_value": v, "fiscal_year": fy, "filing_date": fd}
                        for pe, v, fy, fd in hist
                    ]
                    return pd.DataFrame(rows)
            return pd.DataFrame()

    def _factory():
        return _Query()

    return _factory


def _make_facts(*, concept_values: dict[str, Any] | None = None,
                annual_revenue: tuple[float, int, str] | None = None,
                shares_history: list[tuple[str, float, int, str]] | None = None,
                revenue_history: dict[str, list[tuple[str, float, int, str]]] | None = None,
                gross_profit: float | None = None,
                operating_income: float | None = None,
                net_income: float | None = None,
                revenue_scalar: float | None = None) -> MagicMock:
    """Build a fake EntityFacts MagicMock with the given concept values."""
    facts = MagicMock()
    facts.get_concept = MagicMock(side_effect=lambda name: (concept_values or {}).get(name))

    class _AnnualFact:
        def __init__(self, value, fiscal_year, period_end):
            self.value = value
            self.numeric_value = value
            self.fiscal_year = fiscal_year
            self.period_end = period_end

    if annual_revenue is not None:
        v, fy, pe = annual_revenue
        af = _AnnualFact(v, fy, pe)
        facts.get_annual_fact = MagicMock(return_value=af)
    else:
        facts.get_annual_fact = MagicMock(return_value=None)
    facts.get_revenue = MagicMock(return_value=revenue_scalar)
    facts.get_gross_profit = MagicMock(return_value=gross_profit)
    facts.get_operating_income = MagicMock(return_value=operating_income)
    facts.get_net_income = MagicMock(return_value=net_income)
    facts.query = _stub_query(shares_history=shares_history, revenue_history=revenue_history)
    return facts


def _patch_company(monkeypatch, ticker: str, facts: MagicMock | None, cik: int = 1234):
    """Replace _get_company so extractor uses our fake."""
    company = MagicMock()
    company.cik = cik
    if facts is None:
        company.get_facts = MagicMock(side_effect=Exception("fake no-facts"))
    else:
        company.get_facts = MagicMock(return_value=facts)
    monkeypatch.setattr(xf, "_get_company", lambda t: company)


# ── Tests: clean tech ticker (AMPX) ──────────────────────────────────────────


def test_extract_ampx_clean_tech_ticker(monkeypatch):
    """AMPX (battery tech micro-cap) — fixture from plan doc.

    Expected: revenue $71.9M, cash $90.5M, shares 134.5M, debt None with
    flag `no_debt_reported` (because stockholders_equity IS reported).
    """
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 90_465_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": 103_815_000.0,
            "total_assets": 156_891_000.0,
            "total_liabilities": 53_076_000.0,
            "operating_cash_flow": -31_134_000.0,
            "capex": 4_400_000.0,
        },
        annual_revenue=(71_911_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 134_536_592.0, 2025, "2026-03-06"),
            ("2024-12-31", 116_934_314.0, 2025, "2026-03-06"),
            ("2023-12-31", 88_869_463.0, 2024, "2025-03-20"),
        ],
        revenue_history={
            "Revenues": [
                ("2025-12-31", 73_011_000.0, 2025, "2026-03-06"),
                ("2024-12-31", 24_167_000.0, 2025, "2026-03-06"),
            ],
        },
        gross_profit=8_264_000.0,
        operating_income=-46_646_000.0,
        net_income=-44_024_000.0,
    )
    _patch_company(monkeypatch, "AMPX", facts)

    row = xf.extract_xbrl_fundamentals("AMPX")
    assert row is not None
    assert row["ticker"] == "AMPX"
    assert row["fiscal_year"] == 2025
    assert row["period_end_date"] == "2025-12-31"
    assert row["revenue"] == 71_911_000.0
    assert row["cash_and_equivalents"] == 90_465_000.0
    assert row["long_term_debt"] is None
    assert row["short_term_debt"] is None
    # debt None + equity reported → no_debt_reported, total_debt collapsed to 0
    assert row["total_debt"] == 0.0
    assert row["stockholders_equity"] == 103_815_000.0
    flags = json.loads(row["data_quality_flags"])
    assert "no_debt_reported" in flags
    assert "incomplete_history" not in flags  # 3 share observations is enough
    assert row["shares_outstanding"] == 134_536_592.0
    assert row["shares_outstanding_1yr_ago"] == 116_934_314.0
    assert row["shares_outstanding_2yr_ago"] == 88_869_463.0
    # 2-yr dilution: 134.5 / 88.9 - 1 ≈ 51.4%
    assert row["shares_outstanding_change_2yr_pct"] == pytest.approx(51.39, abs=0.5)
    # Negative OCF → cash runway is computed
    assert row["quarterly_burn"] is not None and row["quarterly_burn"] < 0
    # Burn is -7.78M/q; cash 90.5M → ~11.6 quarters
    assert row["cash_runway_quarters"] == pytest.approx(11.6, abs=0.5)


# ── Tests: no-debt with confirmed balance sheet ─────────────────────────────


def test_extract_no_debt_with_balance_sheet_confirmed(monkeypatch):
    """If long+short debt are None but stockholders_equity is non-None,
    set total_debt=0 and add `no_debt_reported` flag (NOT `incomplete_history`)."""
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 50_000_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": 80_000_000.0,
            "total_assets": 100_000_000.0,
            "total_liabilities": 20_000_000.0,
            "operating_cash_flow": 5_000_000.0,
            "capex": 1_000_000.0,
        },
        annual_revenue=(60_000_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 10_000_000.0, 2025, "2026-03-01"),
            ("2024-12-31", 9_500_000.0, 2024, "2025-03-01"),
            ("2023-12-31", 9_000_000.0, 2023, "2024-03-01"),
        ],
        gross_profit=20_000_000.0,
        operating_income=5_000_000.0,
        net_income=4_000_000.0,
    )
    _patch_company(monkeypatch, "CLEAN", facts)

    row = xf.extract_xbrl_fundamentals("CLEAN")
    assert row is not None
    flags = json.loads(row["data_quality_flags"])
    assert "no_debt_reported" in flags
    assert "incomplete_history" not in flags  # equity reported → balance sheet OK
    assert row["total_debt"] == 0.0
    assert row["debt_to_equity"] == 0.0


# ── Tests: no-debt with incomplete history (DNN edge case) ──────────────────


def test_extract_dnn_no_balance_sheet_no_debt_keeps_none(monkeypatch):
    """DNN: stockholders_equity is None too — we cannot confirm "no debt".
    Leave long/short/total debt as None and add BOTH flags."""
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 11_620_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": None,  # the edge case
            "total_assets": 1_106_074_000.0,
            "total_liabilities": 737_704_000.0,
            "operating_cash_flow": None,
            "capex": None,
        },
        annual_revenue=(4_918_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 901_610_950.0, 2025, "2026-03-15"),
            ("2024-12-31", 850_000_000.0, 2024, "2025-03-15"),
        ],  # only 2 observations → also triggers incomplete_history
        gross_profit=None,
        operating_income=None,
        net_income=-217_288_000.0,
    )
    _patch_company(monkeypatch, "DNN", facts)

    row = xf.extract_xbrl_fundamentals("DNN")
    assert row is not None
    flags = json.loads(row["data_quality_flags"])
    assert "no_debt_reported" in flags
    assert "incomplete_history" in flags
    assert "no_cashflow_reported" in flags
    assert "no_gross_profit_reported" in flags
    # debt unknown → keep None, debt_to_equity also None
    assert row["long_term_debt"] is None
    assert row["short_term_debt"] is None
    assert row["total_debt"] is None
    assert row["debt_to_equity"] is None
    # No OCF → no runway
    assert row["cash_runway_quarters"] is None


# ── Tests: bank income statement (JPM) ──────────────────────────────────────


def test_extract_jpm_bank_income_statement(monkeypatch):
    """JPM: gross_profit=None, operating_income=None, total_assets > $1T.
    Should emit `bank_income_statement` and `no_gross_profit_reported` flags."""
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 278_793_000_000.0,
            "long_term_debt": 269_929_000_000.0,
            "short_term_debt": 64_776_000_000.0,
            "stockholders_equity": 362_438_000_000.0,
            "total_assets": 4_424_900_000_000.0,
            "total_liabilities": 4_062_462_000_000.0,
            "operating_cash_flow": -147_782_000_000.0,
            "capex": None,
        },
        annual_revenue=(182_447_000_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 2_696_200_000.0, 2025, "2026-02-15"),
            ("2024-12-31", 2_750_000_000.0, 2024, "2025-02-15"),
            ("2023-12-31", 2_850_000_000.0, 2023, "2024-02-15"),
        ],
        gross_profit=None,
        operating_income=None,
        net_income=57_048_000_000.0,
    )
    _patch_company(monkeypatch, "JPM", facts)

    row = xf.extract_xbrl_fundamentals("JPM")
    assert row is not None
    flags = json.loads(row["data_quality_flags"])
    assert "no_gross_profit_reported" in flags
    assert "bank_income_statement" in flags
    # Even with -OCF, we have cash and capex unknown → FCF is None (capex None)
    assert row["free_cash_flow"] is None
    # Banks have actual debt (not no_debt)
    assert "no_debt_reported" not in flags
    assert row["total_debt"] == pytest.approx(269_929_000_000.0 + 64_776_000_000.0)


# ── Tests: bank revenue fallback (Phase 1d — 2026-04-15) ────────────────────
# Regional banks don't tag us-gaap:Revenues / SalesRevenueNet — their operating
# income is InterestAndDividendIncomeOperating + NoninterestIncome. Before
# Phase 1d these banks showed near-zero XBRL revenue (~56 tickers in the AMPX
# universe). The bank fallback sums the two concepts for the latest FY.


def test_bank_annual_revenue_sums_interest_and_noninterest():
    """Bank with both interest and noninterest income for the same FY → sum."""
    facts = _make_facts(
        annual_revenue=None,
        revenue_history={
            "us-gaap:InterestAndDividendIncomeOperating": [
                ("2025-12-31", 180_000_000.0, 2025, "2026-02-28"),
                ("2024-12-31", 150_000_000.0, 2024, "2025-02-28"),
            ],
            "us-gaap:NoninterestIncome": [
                ("2025-12-31", 40_000_000.0, 2025, "2026-02-28"),
                ("2024-12-31", 35_000_000.0, 2024, "2025-02-28"),
            ],
        },
    )
    rev, fy, pe = xf._bank_annual_revenue(facts)
    assert rev == pytest.approx(220_000_000.0), (
        f"Bank revenue must sum interest ($180M) + noninterest ($40M) = $220M; got {rev}"
    )
    assert fy == 2025
    assert pe == "2025-12-31"


def test_bank_annual_revenue_returns_none_when_no_interest_income():
    """Non-bank ticker with no interest concepts returns None, unchanged."""
    facts = _make_facts(annual_revenue=None, revenue_history={})
    rev, fy, pe = xf._bank_annual_revenue(facts)
    assert rev is None and fy is None and pe is None


def test_bank_annual_revenue_uses_only_interest_when_noninterest_missing():
    """Small community bank that only tags interest income (no noninterest)
    returns just the interest value — degrades gracefully rather than failing."""
    facts = _make_facts(
        annual_revenue=None,
        revenue_history={
            "us-gaap:InterestAndDividendIncomeOperating": [
                ("2025-12-31", 25_000_000.0, 2025, "2026-02-28"),
            ],
        },
    )
    rev, fy, pe = xf._bank_annual_revenue(facts)
    assert rev == pytest.approx(25_000_000.0)
    assert fy == 2025


def test_annual_revenue_falls_back_to_bank_concepts_when_primary_empty():
    """_annual_revenue's bank fallback path: standard revenue concepts return
    nothing → function falls through to _bank_annual_revenue and returns the
    interest+noninterest sum as revenue."""
    facts = _make_facts(
        annual_revenue=None,  # disables get_annual_fact fallback
        revenue_history={
            "us-gaap:InterestAndDividendIncomeOperating": [
                ("2025-12-31", 80_000_000.0, 2025, "2026-02-28"),
            ],
            "us-gaap:NoninterestIncome": [
                ("2025-12-31", 15_000_000.0, 2025, "2026-02-28"),
            ],
        },
        revenue_scalar=None,
    )
    rev, fy, pe = xf._annual_revenue(facts)
    assert rev == pytest.approx(95_000_000.0), (
        f"_annual_revenue must fall through to bank path; got {rev}"
    )
    assert fy == 2025


def test_annual_revenue_prefers_standard_revenue_over_bank_fallback():
    """Non-bank tech ticker tags SalesRevenueNet AND incidentally has a small
    interest income on its cash pile. The standard revenue concept wins —
    bank fallback is only for tickers with no primary revenue facts."""
    facts = _make_facts(
        annual_revenue=None,
        revenue_history={
            "us-gaap:Revenues": [
                ("2025-12-31", 500_000_000.0, 2025, "2026-02-28"),
            ],
            "us-gaap:InterestAndDividendIncomeOperating": [
                ("2025-12-31", 2_000_000.0, 2025, "2026-02-28"),  # interest on cash
            ],
        },
        revenue_scalar=None,
    )
    rev, fy, pe = xf._annual_revenue(facts)
    assert rev == pytest.approx(500_000_000.0), (
        f"Primary revenue concept must win over bank fallback for non-banks; got {rev}"
    )


def test_prior_revenue_yoy_bank_fallback_for_target_fy():
    """Prior-year growth calculation for banks: when the primary concepts have
    no entry for (latest_fy - 1), fall back to interest + noninterest sum for
    that same target FY."""
    facts = _make_facts(
        revenue_history={
            "us-gaap:InterestAndDividendIncomeOperating": [
                ("2025-12-31", 100_000_000.0, 2025, "2026-02-28"),
                ("2024-12-31", 80_000_000.0, 2024, "2025-02-28"),
            ],
            "us-gaap:NoninterestIncome": [
                ("2025-12-31", 25_000_000.0, 2025, "2026-02-28"),
                ("2024-12-31", 20_000_000.0, 2024, "2025-02-28"),
            ],
        },
    )
    prior = xf._prior_revenue_yoy(facts, latest_fy=2025)
    assert prior == pytest.approx(100_000_000.0), (
        f"Prior-year bank revenue must sum interest($80M) + noninterest($20M) = $100M; got {prior}"
    )


# ── Tests: hard failure paths ──────────────────────────────────────────────


def test_extract_returns_none_when_company_lookup_raises(monkeypatch):
    """Top-level Company() exception → None, no propagation."""
    def _raise(_):
        raise RuntimeError("fake no-such-ticker")

    monkeypatch.setattr(xf, "_get_company", _raise)
    assert xf.extract_xbrl_fundamentals("NONEXISTENT") is None


def test_extract_returns_none_when_get_facts_raises(monkeypatch):
    _patch_company(monkeypatch, "FAIL", None)
    assert xf.extract_xbrl_fundamentals("FAIL") is None


def test_extract_returns_none_when_no_revenue(monkeypatch):
    """No annual revenue → can't anchor a row, return None."""
    facts = _make_facts(
        concept_values={"cash_and_equivalents": 100.0},
        annual_revenue=None,
        shares_history=[],
        revenue_scalar=None,
    )
    _patch_company(monkeypatch, "NOREV", facts)
    assert xf.extract_xbrl_fundamentals("NOREV") is None


def test_extract_returns_none_when_no_cik(monkeypatch):
    company = MagicMock()
    company.cik = None
    monkeypatch.setattr(xf, "_get_company", lambda t: company)
    assert xf.extract_xbrl_fundamentals("NOCIK") is None


# ── Tests: positive cash flow ──────────────────────────────────────────────


def test_extract_positive_cashflow_skips_runway(monkeypatch):
    """Positive operating cash flow → cash_runway_quarters is None + flag."""
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 50_000_000.0,
            "long_term_debt": 1_000_000.0,
            "stockholders_equity": 80_000_000.0,
            "operating_cash_flow": 12_000_000.0,
            "capex": 3_000_000.0,
        },
        annual_revenue=(40_000_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 5_000_000.0, 2025, "2026-03-01"),
            ("2024-12-31", 4_900_000.0, 2024, "2025-03-01"),
            ("2023-12-31", 4_800_000.0, 2023, "2024-03-01"),
        ],
        gross_profit=10_000_000.0,
        operating_income=8_000_000.0,
        net_income=6_000_000.0,
    )
    _patch_company(monkeypatch, "PROFIT", facts)

    row = xf.extract_xbrl_fundamentals("PROFIT")
    assert row is not None
    flags = json.loads(row["data_quality_flags"])
    assert "positive_cashflow" in flags
    assert row["cash_runway_quarters"] is None
    assert row["quarterly_burn"] is None
    # FCF still computes
    assert row["free_cash_flow"] == 9_000_000.0


# ── Tests: Round 5 extractor bugs (INO/AVTX/SVRE) ──────────────────────────


def test_extract_picks_latest_period_across_concepts_when_legacy_tag_is_stale(monkeypatch):
    """INO/AVTX bug — ASC 606 stale-tag problem.

    Companies that adopted ASC 606 in 2018 switched from reporting revenue under
    us-gaap:Revenues to us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax.
    The legacy us-gaap:Revenues tag still has FY2017 data tied to it (the last
    year it was used). The extractor MUST pick the row with the latest period_end
    across ALL revenue concepts, not the first non-None hit on the priority list.

    Reference: INO FY2017 stored $42.2M, FY2025 actual $65K.
    """
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 5_000_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": 10_000_000.0,
            "operating_cash_flow": -8_000_000.0,
        },
        annual_revenue=None,  # Fix path must not rely on get_annual_fact
        shares_history=[
            ("2025-12-31", 200_000_000.0, 2025, "2026-03-12"),
            ("2024-12-31", 180_000_000.0, 2024, "2025-03-12"),
            ("2023-12-31", 150_000_000.0, 2023, "2024-03-12"),
        ],
        revenue_history={
            # Legacy concept last tagged in 2018 filing — stale data
            "us-gaap:Revenues": [
                ("2017-12-31", 42_220_086.0, 2017, "2018-03-14"),
                ("2016-12-31", 35_368_361.0, 2017, "2018-03-14"),
            ],
            # Current concept (post-ASC-606) — authoritative latest data
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": [
                ("2025-12-31", 65_343.0, 2025, "2026-03-12"),
                ("2024-12-31", 217_756.0, 2025, "2026-03-12"),
            ],
        },
        net_income=-30_000_000.0,
    )
    _patch_company(monkeypatch, "INO", facts)

    row = xf.extract_xbrl_fundamentals("INO")
    assert row is not None
    # Must pick FY2025 from the contract concept, NOT FY2017 from Revenues
    assert row["revenue"] == 65_343.0
    assert row["fiscal_year"] == 2025
    assert row["period_end_date"].startswith("2025-12-31")
    # Prior-year YoY must also cross concepts — 2024 data lives in the contract concept
    assert row["revenue_prev_year"] == 217_756.0


def test_extract_drops_implausible_shares_count(monkeypatch):
    """SVRE bug — issuer-filed iXBRL scaling garbage.

    SVRE's 2025 10-K filed dei:EntityCommonStockSharesOutstanding = 29,961,257,022
    (30 billion shares). That's literally what's in the iXBRL, not an extractor
    bug. The prior years filed plausibly (28M → 70M → 415M). The extractor MUST
    sanity-guard against:
      - shares > 50 billion (physically impossible for small-cap)
      - YoY jumps > 100× (likely scaling/decimals bug)

    When rejected, shift to the next plausible observation and add a
    `scaling_bug_suspected` flag.
    """
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 10_000_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": 5_000_000.0,
            "operating_cash_flow": -20_000_000.0,
        },
        annual_revenue=(1_016_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 29_961_257_022.0, 2025, "2026-03-27"),  # issuer typo
            ("2024-12-31", 415_103_076.0, 2024, "2025-03-21"),
            ("2023-12-31", 69_579_231.0, 2023, "2024-03-25"),
            ("2022-12-31", 27_780_896.0, 2022, "2023-04-27"),
        ],
        net_income=-40_000_000.0,
    )
    _patch_company(monkeypatch, "SVRE", facts)

    row = xf.extract_xbrl_fundamentals("SVRE")
    assert row is not None
    # Garbage 30B dropped; prior plausible year (415M) becomes shares_outstanding
    assert row["shares_outstanding"] == 415_103_076.0
    assert row["shares_outstanding_1yr_ago"] == 69_579_231.0
    assert row["shares_outstanding_2yr_ago"] == 27_780_896.0
    flags = json.loads(row["data_quality_flags"])
    assert "scaling_bug_suspected" in flags


# ── Tests: shares-history dilution math ─────────────────────────────────────


def test_extract_high_dilution_two_year(monkeypatch):
    """Synthetic: shares went from 50M → 150M over 2 years (200% increase)."""
    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 30_000_000.0,
            "long_term_debt": None,
            "short_term_debt": None,
            "stockholders_equity": 25_000_000.0,
            "operating_cash_flow": -10_000_000.0,
        },
        annual_revenue=(20_000_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 150_000_000.0, 2025, "2026-03-01"),
            ("2024-12-31", 90_000_000.0, 2024, "2025-03-01"),
            ("2023-12-31", 50_000_000.0, 2023, "2024-03-01"),
        ],
        net_income=-15_000_000.0,
    )
    _patch_company(monkeypatch, "DILUTE", facts)

    row = xf.extract_xbrl_fundamentals("DILUTE")
    assert row is not None
    # 150 / 50 - 1 = 200% over 2yr
    assert row["shares_outstanding_change_2yr_pct"] == pytest.approx(200.0, abs=0.5)
    # 150 / 90 - 1 ≈ 66.7% over 1yr
    assert row["shares_outstanding_change_1yr_pct"] == pytest.approx(66.67, abs=0.5)


# ── Tests: DB write ────────────────────────────────────────────────────────


def test_write_batch_persists_row(tmp_path, monkeypatch):
    """A successful extract should round-trip through the DB writer."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()

    monkeypatch.setattr(xf, "_DEFAULT_DB_PATH", str(db))

    facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 50_000_000.0,
            "long_term_debt": 5_000_000.0,
            "short_term_debt": 1_000_000.0,
            "stockholders_equity": 30_000_000.0,
            "operating_cash_flow": -8_000_000.0,
            "capex": 2_000_000.0,
        },
        annual_revenue=(40_000_000.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 20_000_000.0, 2025, "2026-03-01"),
            ("2024-12-31", 18_000_000.0, 2024, "2025-03-01"),
            ("2023-12-31", 16_000_000.0, 2023, "2024-03-01"),
        ],
        gross_profit=15_000_000.0,
        operating_income=-5_000_000.0,
        net_income=-6_000_000.0,
    )
    _patch_company(monkeypatch, "WRITE", facts)

    row = xf.extract_xbrl_fundamentals("WRITE")
    assert row is not None
    xf._write_batch([row], db_path=str(db))

    with sqlite3.connect(str(db)) as con:
        out = con.execute(
            "SELECT ticker, fiscal_year, revenue, total_debt, source FROM scr_fundamentals_xbrl WHERE ticker='WRITE'"
        ).fetchone()
    assert out is not None
    assert out[0] == "WRITE"
    assert out[1] == 2025
    assert out[2] == 40_000_000.0
    assert out[3] == 6_000_000.0
    assert out[4] == "edgar_xbrl"


# ── Tests: refresh batch orchestrator ──────────────────────────────────────


def test_refresh_orchestrator_handles_failures_per_ticker(tmp_path, monkeypatch):
    """A failing ticker in the middle of a batch should not block others."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    monkeypatch.setattr(xf, "_DEFAULT_DB_PATH", str(db))

    good_facts = _make_facts(
        concept_values={
            "cash_and_equivalents": 10.0, "stockholders_equity": 5.0,
            "operating_cash_flow": 1.0,
        },
        annual_revenue=(100.0, 2025, "2025-12-31"),
        shares_history=[
            ("2025-12-31", 1.0, 2025, "2026-03-01"),
            ("2024-12-31", 1.0, 2024, "2025-03-01"),
            ("2023-12-31", 1.0, 2023, "2024-03-01"),
        ],
        net_income=10.0,
    )

    counter = {"n": 0}

    def fake_get_company(tkr):
        counter["n"] += 1
        if tkr == "BAD":
            company = MagicMock()
            company.cik = 1
            company.get_facts = MagicMock(side_effect=Exception("boom"))
            return company
        company = MagicMock()
        company.cik = 1
        company.get_facts = MagicMock(return_value=good_facts)
        return company

    monkeypatch.setattr(xf, "_get_company", fake_get_company)

    counts = xf.refresh_xbrl_fundamentals(
        ["GOOD1", "BAD", "GOOD2"], batch_size=10, db_path=str(db),
        sleep_per_call=0.0,
    )
    # Two succeed, one fails — orchestrator does not raise
    assert counts["ok"] == 2
    assert counts["failed"] == 1


# ── Tests: dilution dim wiring (B2 verification) ───────────────────────────


def test_dilution_dim_triggers_drag_at_threshold():
    """Dim 12: shares_outstanding_change_2yr_pct > 80 → -1.0 (drag)."""
    from sec_filing_intelligence.ampx_rules import score_row

    # Synthetic AMPX-like row with >80% dilution
    row = {
        "ticker": "DILUTE",
        "company_name": "Dilution Co",
        "industry": "Battery Manufacturing",
        "pct_from_52w_high": -75.0,
        "pct_from_ath": -90.0,
        "revenue_growth_yoy": 1.5,
        "revenue_ttm": 50_000_000,
        "debt_to_equity": 0.05,
        "cash_runway_quarters": 10,
        "shares_outstanding": 200_000_000,
        "float_shares": 80_000_000,
        "institutional_ownership": 0.10,
        "analyst_count": 1,
        "short_interest_pct": 0.20,
        "shares_outstanding_change_2yr_pct": 154.0,
    }
    out = score_row(row, leaps_status="leaps", insider_agg={"buy_count": 0, "cluster_count": 0})
    assert out["dim12_dilution_drag"] == -1.0


def test_dilution_dim_no_drag_when_below_mild_threshold():
    """Dim 12 under R7 tiered thresholds: dilution below the 10% mild threshold
    must score 0.0 (no drag). Boundaries at 9/11/49/51/99/101 are covered in
    tests/test_ampx_rules.py::test_dim12_tier_boundary_*.
    """
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "OK",
        "company_name": "OK Co",
        "industry": "Semiconductor",
        "pct_from_52w_high": -75.0,
        "pct_from_ath": -90.0,
        "revenue_growth_yoy": 0.5,
        "revenue_ttm": 50_000_000,
        "debt_to_equity": 0.05,
        "cash_runway_quarters": 10,
        "shares_outstanding": 100_000_000,
        "float_shares": 80_000_000,
        "institutional_ownership": 0.10,
        "analyst_count": 1,
        "short_interest_pct": 0.20,
        "shares_outstanding_change_2yr_pct": 5.0,  # clearly below 10% mild boundary
    }
    out = score_row(row, leaps_status="leaps", insider_agg={"buy_count": 0, "cluster_count": 0})
    assert out["dim12_dilution_drag"] == 0.0


def test_dilution_dim_silent_zero_when_xbrl_unavailable():
    """Dim 12: if XBRL field absent (yfinance-only ticker), dim = 0.0 (no drag, no bonus)."""
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "YF",
        "company_name": "YF Only",
        "industry": "Defense",
        "pct_from_52w_high": -75.0,
        "pct_from_ath": -90.0,
        "revenue_growth_yoy": 0.5,
        "revenue_ttm": 50_000_000,
        "debt_to_equity": 0.05,
        "cash_runway_quarters": 10,
        "shares_outstanding": 100_000_000,
        "float_shares": 80_000_000,
        "institutional_ownership": 0.10,
        "analyst_count": 1,
        "short_interest_pct": 0.20,
        # shares_outstanding_change_2yr_pct intentionally missing
    }
    out = score_row(row, leaps_status="leaps", insider_agg={"buy_count": 0, "cluster_count": 0})
    assert out["dim12_dilution_drag"] == 0.0
