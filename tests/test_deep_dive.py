"""Tests for the Automated Deep-Dive Scoring module (Phase 6).

Every test uses an in-memory SQLite database via monkeypatching so the
production stocks.db is never touched.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sec_filing_intelligence import db as screener_db
from sec_filing_intelligence.config import (
    DEEP_DIVE_ANALOG_STRONG,
    DEEP_DIVE_ANALOG_WEAK,
    DEEP_DIVE_MOAT_GATE,
    DEEP_DIVE_MOAT_STRONG,
    TABLE_DEEP_DIVE_RESULTS,
    TABLE_DISCOVERY_FLAGS,
)
from sec_filing_intelligence.deep_dive import (
    _check_disqualifiers,
    _compute_verdict,
    _score_analogs,
    _score_balance_sheet,
    _score_insider,
    _score_moat,
    _score_washout,
    _store_deep_dive_results,
    get_latest_deep_dive_results,
    run_deep_dives,
    run_single_deep_dive,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _in_memory_db(monkeypatch):
    """Redirect screener DB to a shared in-memory connection.

    Patches get_connection in BOTH screener.db and screener.deep_dive
    since deep_dive imports get_connection via `from .db import get_connection`
    which binds to the original function reference.
    """
    import sec_filing_intelligence.deep_dive as dd_mod

    screener_db._migrated = False

    _shared = sqlite3.connect(":memory:", check_same_thread=False)
    _shared.execute("PRAGMA journal_mode=WAL")
    _shared.execute("PRAGMA foreign_keys=ON")
    _shared.row_factory = sqlite3.Row

    @contextmanager
    def _mock_conn():
        yield _shared

    monkeypatch.setattr(screener_db, "get_connection", _mock_conn)
    monkeypatch.setattr(dd_mod, "get_connection", _mock_conn)
    screener_db.run_migration()

    yield _shared
    _shared.close()


def _seed_discovery_flags(conn, ticker, score=10.0, company_name="TestCo",
                          moat_types="technology,regulatory", keywords="sole source,ITAR",
                          flag_count=4, moat_type_count=2, scan_date=None):
    """Helper to insert a discovery flag row for testing."""
    sd = scan_date or date.today().isoformat()
    conn.execute(
        f"""INSERT OR REPLACE INTO {TABLE_DISCOVERY_FLAGS}
            (ticker, scan_date, company_name, moat_types, keywords_matched,
             flag_count, moat_type_count, composite_score, rank)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
        (ticker, sd, company_name, moat_types, keywords, flag_count, moat_type_count, score),
    )
    conn.commit()


def _make_data(**overrides):
    """Build a test data dict with sensible defaults, overridable."""
    data = {
        "ticker": "TEST",
        "keywords_matched": "",
        "moat_types": "",
        "keyword_hits": [],
        "moat_signals": [],
        "patent_count": 0,
        "active_patents": 0,
        "gov_contracts": 0,
        "customer_mentions": 0,
        "moat_type_count": 0,
        "pct_from_ath": -50,
        "pct_from_52w_high": -40,
        "revenue_growth": 0,
        "debt_to_equity": 0.5,
        "analyst_count": 5,
        "cash_runway": 10,
        "cash": 100_000_000,
        "total_debt": 50_000_000,
        "float_shares": 50_000_000,
        "institutional_ownership": 0.3,
        "sector": "technology",
        "industry": "semiconductor",
        "market_cap": 200,
        "revenue": 50_000_000,
        "avg_volume_10d": 100_000,
        "avg_volume_30d": 80_000,
        "insider_transactions": [],
        "red_flags": [],
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Test 1: Defense ticker with sole_source + ITAR + gov contracts scores >= 12
# ---------------------------------------------------------------------------


class TestScoreMoat:

    def test_score_moat_defense_ticker(self):
        """Ticker with sole_source + ITAR + gov_contract scores >= 12/25."""
        data = _make_data(
            patent_count=20,
            gov_contracts=3,
            keyword_hits=[
                {"keyword": "sole source", "layer": "hard", "moat_type": "supply_chain"},
                {"keyword": "ITAR", "layer": "soft", "moat_type": "government"},
                {"keyword": "security clearance", "layer": "soft", "moat_type": "government"},
                {"keyword": "only manufacturer", "layer": "soft", "moat_type": "infrastructure"},
                {"keyword": "mission-critical", "layer": "soft", "moat_type": "supply_chain"},
            ],
            moat_types="technology,regulatory,government,supply_chain,infrastructure",
        )
        result = _score_moat(data)
        # 2A: 20 patents -> 3, 2B: mission-critical + gov -> 4, 2D: sole_source+gov -> 5,
        # 2E: only manufacturer -> 4.  Total >= 12
        assert result["moat_score"] >= 12, f"Expected >= 12, got {result['moat_score']}"
        assert result["gate_pass"] is True

    def test_score_moat_no_ip_fails_gate(self):
        """Ticker with zero patents and no moat keywords scores < 8 -> KILL."""
        data = _make_data(
            patent_count=0,
            keyword_hits=[],
            moat_types="",
            gov_contracts=0,
        )
        result = _score_moat(data)
        assert result["moat_score"] < DEEP_DIVE_MOAT_GATE
        assert result["gate_pass"] is False


# ---------------------------------------------------------------------------
# Test 3-4: Analog DNA patterns
# ---------------------------------------------------------------------------


class TestScoreAnalogs:

    def test_analog_ampx_pattern(self):
        """Ticker with 80%+ crash, growing revenue, zero debt matches AMPX DNA 5+/8."""
        data = _make_data(
            pct_from_ath=-85,
            customer_mentions=2,
            revenue_growth=150,
            moat_type_count=4,
            keyword_hits=[
                {"keyword": "production", "layer": "soft", "moat_type": "infrastructure"},
            ],
            debt_to_equity=0,
            analyst_count=1,
            cash_runway=8,
        )
        result = _score_analogs(data)
        assert result["analogs"]["AMPX"] >= 5, f"AMPX score {result['analogs']['AMPX']}"
        assert result["best_analog"] == "AMPX" or result["analogs"]["AMPX"] >= DEEP_DIVE_ANALOG_STRONG

    def test_analog_rklb_pattern(self):
        """Ticker with sole source + gov contracts + defense matches RKLB DNA 5+/8."""
        data = _make_data(
            keyword_hits=[
                {"keyword": "sole source", "layer": "hard", "moat_type": "supply_chain"},
                {"keyword": "proprietary technology", "layer": "soft", "moat_type": "technology"},
                {"keyword": "patent portfolio", "layer": "soft", "moat_type": "technology"},
                {"keyword": "vertically integrated", "layer": "soft", "moat_type": "infrastructure"},
            ],
            revenue=50_000_000,
            gov_contracts=5,
            moat_type_count=3,
            moat_types="technology,government,supply_chain",
            sector="industrials",
            industry="aerospace & defense",
            institutional_ownership=0.25,
        )
        result = _score_analogs(data)
        assert result["analogs"]["RKLB"] >= 5, f"RKLB score {result['analogs']['RKLB']}"


# ---------------------------------------------------------------------------
# Test 5: Clustered insider selling -> KILL
# ---------------------------------------------------------------------------


class TestDisqualifiers:

    def test_disqualifier_cluster_selling_kills(self):
        """3 C-suite insider sellers in 60 days -> KILL regardless of other scores."""
        today = date.today().isoformat()
        recent = (date.today() - timedelta(days=30)).isoformat()

        data = _make_data(
            insider_transactions=[
                {"insider_name": "CEO Bob", "insider_title": "CEO", "transaction_type": "sale",
                 "shares": 10000, "total_value": 100000, "is_open_market": 1, "filing_date": recent},
                {"insider_name": "CFO Alice", "insider_title": "CFO", "transaction_type": "sale",
                 "shares": 8000, "total_value": 80000, "is_open_market": 1, "filing_date": recent},
                {"insider_name": "COO Carol", "insider_title": "COO", "transaction_type": "sale",
                 "shares": 5000, "total_value": 50000, "is_open_market": 1, "filing_date": recent},
            ],
        )

        insider = _score_insider(data)
        assert insider["cluster_selling"] is True
        assert insider["pass"] is False

        # Even with strong moat, verdict is KILL
        moat = {"moat_score": 20, "gate_pass": True}
        analogs = {"best_analog": "AMPX", "best_analog_score": 7, "strong_match": True, "weak_match": True}
        balance = {"pass": True, "flags": []}
        washout = {"phase": 2, "phase_name": "Base Building", "entry_ok": True}
        disqualifiers = _check_disqualifiers(data, insider)

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "KILL"
        assert "selling" in verdict["reason"].lower() or "cluster" in verdict["reason"].lower()


# ---------------------------------------------------------------------------
# Test 6: Full PASS verdict
# ---------------------------------------------------------------------------


class TestVerdict:

    def test_verdict_pass(self):
        """High moat + strong analog + clean balance sheet + no disqualifiers -> PASS."""
        moat = {"moat_score": 15, "gate_pass": True}
        analogs = {
            "best_analog": "AMPX", "best_analog_score": 6,
            "strong_match": True, "weak_match": True,
            "analogs": {"AMPX": 6, "RKLB": 2, "ASTS": 1, "NBIS": 3},
        }
        balance = {"pass": True, "flags": ["OK: cash runway 10.0 quarters"]}
        insider = {"pass": True, "cluster_selling": False, "ceo_buying": True,
                   "buy_count": 3, "sell_count": 0}
        washout = {"phase": 2, "phase_name": "Base Building", "entry_ok": True}
        disqualifiers = {"pass": True, "kills": [], "unverified": []}

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "PASS"

    def test_verdict_kill_moat_gate(self):
        """Moat 6/25 -> KILL regardless of other scores."""
        moat = {"moat_score": 6, "gate_pass": False}
        analogs = {
            "best_analog": "RKLB", "best_analog_score": 7,
            "strong_match": True, "weak_match": True,
            "analogs": {"AMPX": 2, "RKLB": 7, "ASTS": 1, "NBIS": 3},
        }
        balance = {"pass": True, "flags": []}
        insider = {"pass": True, "cluster_selling": False, "ceo_buying": False,
                   "buy_count": 0, "sell_count": 0}
        washout = {"phase": 1, "phase_name": "Capitulation", "entry_ok": True}
        disqualifiers = {"pass": True, "kills": [], "unverified": []}

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "KILL"
        assert "moat" in verdict["reason"].lower()

    def test_verdict_watchlist_phase_too_late(self):
        """Strong scores but Phase 4 -> capped at WATCHLIST."""
        moat = {"moat_score": 18, "gate_pass": True}
        analogs = {
            "best_analog": "NBIS", "best_analog_score": 6,
            "strong_match": True, "weak_match": True,
            "analogs": {"AMPX": 2, "RKLB": 2, "ASTS": 1, "NBIS": 6},
        }
        balance = {"pass": True, "flags": []}
        insider = {"pass": True, "cluster_selling": False, "ceo_buying": False,
                   "buy_count": 0, "sell_count": 0}
        washout = {"phase": 4, "phase_name": "Growth Confirmation", "entry_ok": False}
        disqualifiers = {"pass": True, "kills": [], "unverified": []}

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "WATCHLIST"

    def test_verdict_kill_balance_sheet_fail(self):
        """Going concern on balance sheet -> KILL."""
        moat = {"moat_score": 15, "gate_pass": True}
        analogs = {"best_analog": "AMPX", "best_analog_score": 6,
                   "strong_match": True, "weak_match": True}
        balance = {"pass": False, "flags": ["HARD KILL: going concern"]}
        insider = {"pass": True, "cluster_selling": False}
        washout = {"phase": 2, "phase_name": "Base Building", "entry_ok": True}
        disqualifiers = {"pass": True, "kills": [], "unverified": []}

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "KILL"
        assert "balance sheet" in verdict["reason"].lower()

    def test_verdict_kill_hard_disqualifier(self):
        """Going concern in disqualifiers -> KILL."""
        moat = {"moat_score": 20, "gate_pass": True}
        analogs = {"best_analog": "AMPX", "best_analog_score": 7,
                   "strong_match": True, "weak_match": True}
        balance = {"pass": True, "flags": []}
        insider = {"pass": True, "cluster_selling": False}
        washout = {"phase": 1, "phase_name": "Capitulation", "entry_ok": True}
        disqualifiers = {"pass": False, "kills": ["going_concern"], "unverified": []}

        verdict = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
        assert verdict["verdict"] == "KILL"
        assert "disqualifier" in verdict["reason"].lower()


# ---------------------------------------------------------------------------
# Module-specific scoring tests
# ---------------------------------------------------------------------------


class TestScoreBalanceSheet:

    def test_going_concern_kills(self):
        data = _make_data(red_flags=["going_concern_10k"])
        result = _score_balance_sheet(data)
        assert result["pass"] is False

    def test_low_runway_fails(self):
        data = _make_data(cash_runway=3)
        result = _score_balance_sheet(data)
        assert result["pass"] is False

    def test_healthy_passes(self):
        data = _make_data(cash_runway=12, debt_to_equity=0.3)
        result = _score_balance_sheet(data)
        assert result["pass"] is True


class TestScoreInsider:

    def test_ceo_buying_detected(self):
        data = _make_data(
            insider_transactions=[
                {"insider_name": "CEO Smith", "insider_title": "CEO",
                 "transaction_type": "purchase", "shares": 50000,
                 "total_value": 500000, "is_open_market": 1,
                 "filing_date": date.today().isoformat()},
            ],
        )
        result = _score_insider(data)
        assert result["ceo_buying"] is True
        assert result["buy_count"] == 1
        assert result["pass"] is True

    def test_no_transactions_passes(self):
        data = _make_data(insider_transactions=[])
        result = _score_insider(data)
        assert result["pass"] is True
        assert result["buy_count"] == 0


class TestScoreWashout:

    def test_deep_crash_phase_1(self):
        data = _make_data(pct_from_ath=-90, analyst_count=0)
        result = _score_washout(data)
        assert result["phase"] <= 2
        assert result["entry_ok"] is True

    def test_near_high_phase_5(self):
        data = _make_data(pct_from_ath=-5, pct_from_52w_high=-5, analyst_count=12)
        result = _score_washout(data)
        assert result["phase"] == 5
        assert result["entry_ok"] is False


# ---------------------------------------------------------------------------
# DB storage and integration tests
# ---------------------------------------------------------------------------


class TestStorageAndIntegration:

    def test_store_and_retrieve(self, _in_memory_db):
        """Store results and retrieve via get_latest_deep_dive_results."""
        results = [
            {
                "ticker": "TEST",
                "verdict": "PASS",
                "moat": {"moat_score": 15, "sub_scores": {"2A_ip": 3}},
                "analogs": {"best_analog": "AMPX", "best_analog_score": 6},
                "balance_sheet": {"pass": True},
                "insider": {"pass": True},
                "washout": {"phase": 2, "phase_name": "Base Building"},
                "disqualifiers": {"kills": []},
                "discovery_score": 12.5,
                "summary": "Test summary",
            }
        ]
        scan_date = date.today().isoformat()
        stored = _store_deep_dive_results(results, scan_date)
        assert stored == 1

        latest = get_latest_deep_dive_results(top_n=10)
        assert len(latest) == 1
        assert latest[0]["ticker"] == "TEST"
        assert latest[0]["verdict"] == "PASS"
        assert latest[0]["moat_score"] == 15

    def test_deep_dive_table_created(self, _in_memory_db):
        """scr_deep_dive_results table exists after migration."""
        tables = {
            r[0] for r in _in_memory_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert TABLE_DEEP_DIVE_RESULTS in tables

    @patch("sec_filing_intelligence.deep_dive._yfinance_enrich", side_effect=lambda t, d: d)
    def test_run_deep_dives_no_discovery(_mock_yf, self, _in_memory_db):
        """run_deep_dives with no discovery data returns error."""
        result = run_deep_dives(top_n=5, dry_run=True)
        assert "error" in result

    @patch("sec_filing_intelligence.deep_dive._yfinance_enrich", side_effect=lambda t, d: d)
    def test_run_deep_dives_with_data(_mock_yf, self, _in_memory_db):
        """run_deep_dives processes seeded discovery candidates."""
        sd = date.today().isoformat()
        # Seed 3 discovery flags
        for ticker, score in [("AAA", 15.0), ("BBB", 12.0), ("CCC", 8.0)]:
            _seed_discovery_flags(_in_memory_db, ticker, score=score, scan_date=sd)

        result = run_deep_dives(top_n=3, dry_run=True)
        assert "error" not in result
        assert result["total_analyzed"] == 3
        # All verdicts should be valid strings
        for r in result["results"]:
            assert r["verdict"] in ("PASS", "WATCHLIST", "KILL", "ERROR")
