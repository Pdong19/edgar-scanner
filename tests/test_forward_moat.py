"""Tests for the Forward Moat Scanner."""

import sqlite3
import pytest
from sec_filing_intelligence.db import get_connection, run_migration


def test_forward_moat_tables_exist():
    """Verify scr_forward_moat_scores and scr_forward_moat_history tables are created."""
    run_migration()
    with get_connection() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'scr_forward%'"
        ).fetchall()}
    assert "scr_forward_moat_scores" in tables
    assert "scr_forward_moat_history" in tables


def test_forward_moat_scores_columns():
    """Verify scr_forward_moat_scores has all required columns."""
    run_migration()
    with get_connection() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_forward_moat_scores)").fetchall()}
    required = {
        "ticker", "scan_date", "sig_backlog", "sig_segment_crossover",
        "sig_partnership_mismatch", "sig_new_tam", "sig_capex_inflection",
        "sig_tech_milestone", "forward_score", "backlog_current", "backlog_prior",
        "backlog_growth_pct", "partnership_names", "partnership_verified",
        "new_tam_keywords", "capex_growth_pct", "rd_growth_pct",
        "milestone_keywords", "forward_verdict", "forward_verdict_reason",
    }
    assert required.issubset(cols), f"Missing columns: {required - cols}"


# ── Task 2: Backlog signal tests ────────────────────────────────────────────

from sec_filing_intelligence.forward_moat import score_backlog, _extract_backlog_amount


class TestBacklogExtraction:
    def test_extract_dollar_millions(self):
        text = "Our backlog was $504.3 million as of December 31, 2022"
        assert _extract_backlog_amount(text) == pytest.approx(504.3e6, rel=0.01)

    def test_extract_dollar_billions(self):
        text = "contract backlog of $1.2 billion"
        assert _extract_backlog_amount(text) == pytest.approx(1.2e9, rel=0.01)

    def test_extract_plain_number(self):
        text = "backlog increased to $503,600,000"
        assert _extract_backlog_amount(text) == pytest.approx(503.6e6, rel=0.01)

    def test_no_backlog(self):
        text = "We sell widgets to customers."
        assert _extract_backlog_amount(text) is None


class TestBacklogScoring:
    def test_doubled_backlog_max_score(self):
        score, meta = score_backlog(current_amount=504e6, prior_amount=241e6)
        assert score == 8

    def test_50pct_growth(self):
        score, meta = score_backlog(current_amount=150e6, prior_amount=100e6)
        assert score == 6

    def test_25pct_growth(self):
        score, meta = score_backlog(current_amount=130e6, prior_amount=100e6)
        assert score == 4

    def test_flat_backlog(self):
        score, meta = score_backlog(current_amount=100e6, prior_amount=100e6)
        assert score == 0

    def test_no_data(self):
        score, meta = score_backlog(current_amount=None, prior_amount=None)
        assert score == 0


# ── Task 3: Partnership detection and scoring tests ────────────────────────

from sec_filing_intelligence.forward_moat import (
    _detect_partnerships,
    score_partnership_mismatch,
)


class TestPartnershipDetection:
    def test_detect_strategic_investment(self):
        text = "The company entered into a strategic investment with Boeing to co-develop next-gen avionics."
        partners = _detect_partnerships(text)
        assert "BA" in partners

    def test_ignore_customer_mention(self):
        """Plain customer mention without partnership keyword should not match."""
        text = "Our largest customer is Boeing. We shipped 500 units to their Seattle facility last quarter."
        partners = _detect_partnerships(text)
        assert partners == []

    def test_detect_multiple_partners(self):
        text = (
            "We signed a joint development agreement with Lockheed Martin for defense sensors. "
            "Additionally, a strategic partnership with Nvidia enables AI-accelerated processing."
        )
        partners = _detect_partnerships(text)
        assert "LMT" in partners
        assert "NVDA" in partners
        assert len(partners) >= 2


class TestPartnershipScoring:
    def test_fortune100_max_score(self):
        """Fortune 100 + 2 partners + 100x ratio = 5."""
        score, meta = score_partnership_mismatch(
            partner_tickers=["AAPL", "MSFT"],
            ticker_market_cap=500e6,
            partner_market_caps={"AAPL": 3e12, "MSFT": 2.8e12},
        )
        assert score == 5

    def test_single_fortune500_small_cap(self):
        """1 Fortune 500 + ratio 10x+ + ticker < $2B = 3."""
        score, meta = score_partnership_mismatch(
            partner_tickers=["LMT"],
            ticker_market_cap=800e6,
            partner_market_caps={"LMT": 120e9},
        )
        assert score == 3

    def test_no_mismatch(self):
        """Fortune partner but low ratio and large cap = 1."""
        score, meta = score_partnership_mismatch(
            partner_tickers=["CSCO"],
            ticker_market_cap=50e9,
            partner_market_caps={"CSCO": 200e9},
        )
        assert score <= 1

    def test_no_partners(self):
        score, meta = score_partnership_mismatch(
            partner_tickers=[],
            ticker_market_cap=500e6,
            partner_market_caps={},
        )
        assert score == 0


# ── Task 4: Capex/R&D inflection tests ────────────────────────────────────

from sec_filing_intelligence.forward_moat import score_capex_inflection


class TestCapexInflection:
    def test_50pct_capex_deep_crash(self):
        """50% capex growth + 65% below ATH = 2."""
        score, meta = score_capex_inflection(
            capex_current=-150e6, capex_prior=-100e6,
            rd_current=None, rd_prior=None,
            pct_from_ath=-0.65,
        )
        assert score == 2

    def test_30pct_rd_moderate_crash(self):
        """30% R&D growth + 55% below ATH = 1."""
        score, meta = score_capex_inflection(
            capex_current=None, capex_prior=None,
            rd_current=130e6, rd_prior=100e6,
            pct_from_ath=-0.55,
        )
        assert score == 1

    def test_growth_but_no_crash(self):
        """Growth present but stock not beaten down = 0."""
        score, meta = score_capex_inflection(
            capex_current=-200e6, capex_prior=-100e6,
            rd_current=None, rd_prior=None,
            pct_from_ath=-0.20,
        )
        assert score == 0

    def test_no_data(self):
        score, meta = score_capex_inflection(
            capex_current=None, capex_prior=None,
            rd_current=None, rd_prior=None,
            pct_from_ath=-0.70,
        )
        assert score == 0


# ── Task 5: Segment crossover tests ───────────────────────────────────────

from sec_filing_intelligence.forward_moat import score_segment_crossover


class TestSegmentCrossover:
    def test_secondary_overtaking(self):
        """Secondary grew 60% and is 75% of primary = 5."""
        score, meta = score_segment_crossover(
            primary_revenue=100e6, primary_growth=0.05,
            secondary_revenue=75e6, secondary_growth=0.60,
        )
        assert score == 5

    def test_growing_but_small(self):
        """Secondary grew 55% but only 20% of primary = 1."""
        score, meta = score_segment_crossover(
            primary_revenue=500e6, primary_growth=0.10,
            secondary_revenue=100e6, secondary_growth=0.55,
        )
        assert score == 1

    def test_single_segment(self):
        score, meta = score_segment_crossover(
            primary_revenue=200e6, primary_growth=0.10,
            secondary_revenue=None, secondary_growth=None,
        )
        assert score == 0


# ── Task 5: New TAM language tests ─────────────────────────────────────────

from sec_filing_intelligence.forward_moat import score_new_tam_language


class TestNewTamLanguage:
    def test_multiple_new_keywords(self):
        """3+ new keywords = 3."""
        score, meta = score_new_tam_language(
            current_keywords={"eVTOL", "quantum computing", "fusion energy", "CHIPS Act"},
            prior_keywords={"CHIPS Act"},
        )
        assert score == 3

    def test_one_new_keyword(self):
        score, meta = score_new_tam_language(
            current_keywords={"eVTOL", "CHIPS Act"},
            prior_keywords={"CHIPS Act"},
        )
        assert score == 1

    def test_no_change(self):
        score, meta = score_new_tam_language(
            current_keywords={"CHIPS Act", "reshoring"},
            prior_keywords={"CHIPS Act", "reshoring"},
        )
        assert score == 0


# ── Task 5: Technology milestone tests ─────────────────────────────────────

from sec_filing_intelligence.forward_moat import score_tech_milestone


class TestTechMilestone:
    def test_multiple_milestones(self):
        """2+ distinct milestone keywords = 2."""
        texts = [
            "The company achieved the world's first commercial deployment of the technology.",
            "Additionally, the product was FAA certified for operation in US airspace.",
        ]
        score, meta = score_tech_milestone(texts)
        assert score == 2

    def test_single_milestone(self):
        texts = ["We successfully demonstrated the prototype to DoD officials."]
        score, meta = score_tech_milestone(texts)
        assert score == 1

    def test_no_milestones(self):
        texts = ["Revenue increased 15% year over year driven by strong demand."]
        score, meta = score_tech_milestone(texts)
        assert score == 0


# ── Task 6: Score composition + DB writer tests ─────────────────────────────

from sec_filing_intelligence.forward_moat import compute_forward_score, write_forward_scores


class TestComputeForwardScore:
    def test_max_score_capped_at_25(self):
        """Sum exceeding 25 should be capped."""
        assert compute_forward_score(8, 5, 5, 3, 2, 2) == 25

    def test_rklb_simulated(self):
        """RKLB simulated: 8+5+3+3+2+2 = 23."""
        assert compute_forward_score(8, 5, 3, 3, 2, 2) == 23

    def test_asts_simulated(self):
        """ASTS simulated: 0+0+5+3+2+2 = 12."""
        assert compute_forward_score(0, 0, 5, 3, 2, 2) == 12

    def test_zero_score(self):
        assert compute_forward_score(0, 0, 0, 0, 0, 0) == 0


class TestWriteForwardScores:
    def test_write_and_read(self):
        """Insert a row, query it back, verify forward_score matches."""
        run_migration()
        row = {
            "ticker": "TEST123",
            "scan_date": "2026-04-22",
            "sig_backlog": 6.0,
            "sig_segment_crossover": 0.0,
            "sig_partnership_mismatch": 3.0,
            "sig_new_tam": 2.0,
            "sig_capex_inflection": 1.0,
            "sig_tech_milestone": 1.0,
            "forward_score": 13.0,
            "backlog_current": "500000000",
            "backlog_prior": "300000000",
            "backlog_growth_pct": 0.6667,
            "partnership_names": "LMT,BA",
            "partnership_verified": 1,
            "new_tam_keywords": "eVTOL,CHIPS Act",
            "capex_growth_pct": 0.35,
            "rd_growth_pct": 0.20,
            "milestone_keywords": "first flight",
            "forward_verdict": None,
            "forward_verdict_reason": None,
        }
        count = write_forward_scores([row])
        assert count == 1

        with get_connection() as conn:
            result = conn.execute(
                "SELECT forward_score, sig_backlog, partnership_names FROM scr_forward_moat_scores WHERE ticker = 'TEST123'"
            ).fetchone()
        assert result is not None
        assert result["forward_score"] == 13.0
        assert result["sig_backlog"] == 6.0
        assert result["partnership_names"] == "LMT,BA"


# ── Task 7: Orchestrator integration test ────────────────────────────────────

from unittest.mock import patch
from sec_filing_intelligence.forward_moat import run_forward_scan


class TestOrchestrator:
    def test_orchestrator_returns_summary(self):
        """Mock batch EFTS + all 6 _fetch_*_signal functions, insert a fake discovery row, verify result."""
        run_migration()

        # Insert a fake discovery candidate
        with get_connection() as conn:
            # Ensure the table has the right columns by trying to insert
            conn.execute(
                """INSERT OR REPLACE INTO scr_discovery_flags
                   (ticker, scan_date, composite_score, company_name, flag_count, layer_2a, layer_2b, layer_2c,
                    moat_types, keywords_matched, moat_type_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("MOCK1", "2026-04-22", 35.0, "Mock Corp", 5, 3, 2, 0, "[]", "[]", 2),
            )
            conn.commit()

        with (
            patch("sec_filing_intelligence.forward_moat._load_cik_maps"),
            patch("sec_filing_intelligence.forward_moat._global_efts_search_mapped", return_value={}),
            patch("sec_filing_intelligence.forward_moat._fetch_backlog_signal", return_value=(4.0, {"backlog_current": 100e6, "backlog_prior": 70e6, "backlog_growth_pct": 0.43})),
            patch("sec_filing_intelligence.forward_moat._fetch_partnership_signal", return_value=(3.0, {"partner_tickers": "LMT", "partner_count": 1, "max_ratio": 150.0})),
            patch("sec_filing_intelligence.forward_moat._fetch_capex_signal", return_value=(1.0, {"capex_growth_pct": 0.35, "rd_growth_pct": None})),
            patch("sec_filing_intelligence.forward_moat._fetch_segment_signal", return_value=(0, {})),
            patch("sec_filing_intelligence.forward_moat._fetch_tam_signal", return_value=(2.0, {"new_tam_keywords": "eVTOL", "new_count": 1})),
            patch("sec_filing_intelligence.forward_moat._fetch_milestone_signal", return_value=(1.0, {"milestone_keywords": "first flight"})),
        ):
            result = run_forward_scan(dry_run=True, scan_date="2026-04-22")

        assert "total_scored" in result
        assert result["total_scored"] >= 1
        assert result["scan_date"] == "2026-04-22"


# ── Task 8: Combined merge tests ─────────────────────────────────────────────

import tempfile
from pathlib import Path
from sec_filing_intelligence.forward_moat import merge_combined_csv


class TestMergeCombinedCSV:
    def test_merge_produces_file(self):
        """Basic merge creates file with combined_rank column."""
        moat_rows = [
            {"ticker": "AAA", "composite_score": 30.0, "rank": 1},
            {"ticker": "BBB", "composite_score": 20.0, "rank": 2},
        ]
        forward_rows = [
            {"ticker": "AAA", "forward_score": 15.0, "rank": 1},
            {"ticker": "BBB", "forward_score": 10.0, "rank": 2},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "combined-test.csv"
            result = merge_combined_csv(moat_rows, forward_rows, output_path=out)
            assert result.exists()
            content = result.read_text()
            assert "combined_rank" in content
            assert "AAA" in content
            assert "BBB" in content

    def test_merge_ranks_convergence(self):
        """2 tickers, verify combined output has both with correct ranking."""
        moat_rows = [
            {"ticker": "TOP", "composite_score": 25.0, "rank": 1},
            {"ticker": "BOT", "composite_score": 10.0, "rank": 2},
        ]
        forward_rows = [
            {"ticker": "BOT", "forward_score": 20.0, "rank": 1},
            {"ticker": "TOP", "forward_score": 5.0, "rank": 2},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "combined-conv.csv"
            result = merge_combined_csv(moat_rows, forward_rows, output_path=out)
            import csv as csv_mod
            with open(result) as f:
                rows = list(csv_mod.DictReader(f))
            tickers = [r["ticker"] for r in rows]
            assert "TOP" in tickers
            assert "BOT" in tickers
            assert len(rows) == 2
            # Both should have combined_rank assigned
            ranks = {r["ticker"]: int(r["combined_rank"]) for r in rows}
            assert set(ranks.values()) == {1, 2}
