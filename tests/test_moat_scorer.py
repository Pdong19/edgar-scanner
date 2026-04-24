"""Tests for the CPC moat scorer.

Given a ticker and a thesis config with cpc_prefixes, computes the fraction
of the ticker's active patents that match any thesis prefix. Returns a score
in [0.0, 1.0].

Design invariants:
- Tickers with zero patents score 0.0 (avoids false-positive moat for no-IP cos)
- Same patent matching MULTIPLE thesis prefixes counts ONCE (don't double-count)
- Inactive (expired) patents excluded
- If thesis has no cpc_prefixes (alt-signal only), returns 0.0 — caller uses
  alternative scoring path
"""

import sqlite3
from contextlib import contextmanager

import pytest

from sec_filing_intelligence.thesis_loader import ThesisConfig


@pytest.fixture
def in_memory_db(monkeypatch):
    import sec_filing_intelligence.db as db_mod

    db_mod._migrated = False

    shared = sqlite3.connect(":memory:", check_same_thread=False)
    shared.execute("PRAGMA foreign_keys=ON")
    shared.row_factory = sqlite3.Row

    @contextmanager
    def _mock_conn():
        yield shared

    monkeypatch.setattr(db_mod, "get_connection", _mock_conn)

    # moat_scorer imports get_connection at module level — patch there too
    import sec_filing_intelligence.moat_scorer as ms_mod
    monkeypatch.setattr(ms_mod, "get_connection", _mock_conn)

    yield shared
    shared.close()


def _make_config(prefixes: list[str]) -> ThesisConfig:
    return ThesisConfig(
        thesis_id="test",
        display_name="Test",
        forcing_function="test",
        moat_scoring={"cpc_prefixes": prefixes} if prefixes else {
            "alternative_moat_signals": [{
                "signal_type": "asset_scarcity",
                "description": "x",
                "data_source": "x",
            }]
        },
        supply_chain_map={
            "tier1": {"keywords": ["a"]},
            "tier2": {"keywords": ["b"]},
            "tier3": {"keywords": ["c"]},
        },
    )


def _seed_patents(conn, ticker: str, patents_with_cpcs: list[tuple[str, list[str], int]]):
    """Seed scr_patents + scr_patent_cpcs. Each entry: (patent_num, cpcs, is_active)."""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        """INSERT INTO scr_patents
           (ticker, patent_number, title, grant_date, technology_area, is_active, source, last_updated)
           VALUES (?, ?, 'x', '2020-01-01', NULL, ?, 'USPTO', ?)""",
        [(ticker, pn, active, now) for (pn, _, active) in patents_with_cpcs],
    )
    cpc_rows = [
        (ticker, pn, cpc, None, idx)
        for (pn, cpcs, _) in patents_with_cpcs
        for idx, cpc in enumerate(cpcs)
    ]
    if cpc_rows:
        conn.executemany(
            """INSERT INTO scr_patent_cpcs
               (ticker, patent_number, cpc_group_id, cpc_group_title, rank)
               VALUES (?, ?, ?, ?, ?)""",
            cpc_rows,
        )
    conn.commit()


class TestMoatScorer:
    def test_ticker_with_no_patents_returns_zero(self, in_memory_db):
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        cfg = _make_config(["G21"])
        assert score_moat("NOPAT", cfg) == 0.0

    def test_ticker_with_zero_matching_patents_returns_zero(self, in_memory_db):
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "TEST", [
            ("P1", ["H01L29/00", "G06F9/50"], 1),  # semi + software, not nuclear
        ])
        cfg = _make_config(["G21"])
        assert score_moat("TEST", cfg) == 0.0

    def test_all_patents_match_returns_one(self, in_memory_db):
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "NUKE", [
            ("P1", ["G21F1/02"], 1),
            ("P2", ["G21C17/00"], 1),
        ])
        cfg = _make_config(["G21"])
        assert score_moat("NUKE", cfg) == 1.0

    def test_half_match_returns_half(self, in_memory_db):
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "HALF", [
            ("P1", ["G21F1/02"], 1),          # match
            ("P2", ["H01L29/00"], 1),         # no match
            ("P3", ["G21C17/00"], 1),         # match
            ("P4", ["C07C1/00"], 1),          # no match
        ])
        cfg = _make_config(["G21"])
        assert score_moat("HALF", cfg) == 0.5

    def test_inactive_patents_excluded(self, in_memory_db):
        """Expired patents don't count toward active moat."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "MIX", [
            ("P1", ["G21F1/02"], 1),   # active, match
            ("P2", ["G21C17/00"], 0),  # inactive, excluded entirely
        ])
        cfg = _make_config(["G21"])
        # P1 is 1-of-1 active → 1.0
        assert score_moat("MIX", cfg) == 1.0

    def test_multiple_prefixes_any_match_counts(self, in_memory_db):
        """Thesis with G21 and Y02E30 prefixes: a patent matching EITHER counts."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "MULTI", [
            ("P1", ["G21F1/02"], 1),      # G21 match
            ("P2", ["Y02E30/30"], 1),     # Y02E30 match
            ("P3", ["H01L29/00"], 1),     # no match
        ])
        cfg = _make_config(["G21", "Y02E30"])
        # 2 of 3 match = 0.666...
        assert round(score_moat("MULTI", cfg), 4) == round(2/3, 4)

    def test_patent_with_multiple_matching_cpcs_counts_once(self, in_memory_db):
        """A patent with CPCs in 2 thesis-matching prefixes should count as
        ONE matching patent, not two. Prevents moat score inflation."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "DUP", [
            ("P1", ["G21F1/02", "G21C17/00", "Y02E30/30"], 1),  # 3 matching CPCs, 1 patent
            ("P2", ["H01L29/00"], 1),
        ])
        cfg = _make_config(["G21", "Y02E30"])
        # P1 counts as ONE match even though 3 CPCs hit. P2 doesn't match.
        # 1 of 2 active patents = 0.5
        assert score_moat("DUP", cfg) == 0.5

    def test_no_cpc_prefixes_returns_zero(self, in_memory_db):
        """Thesis with only alternative_moat_signals returns 0 from CPC scorer.
        Caller uses an alternative scoring path for alt-signal moats."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "ANY", [
            ("P1", ["G21F1/02"], 1),
        ])
        cfg = _make_config([])  # triggers alt-signal-only path
        assert score_moat("ANY", cfg) == 0.0

    def test_exact_prefix_match_not_substring(self, in_memory_db):
        """Thesis prefix 'G21' should match 'G21F1/02' (starts with) but NOT
        'HG21X' (contains). This is what LIKE 'prefix%' gives us."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "EDGE", [
            ("P1", ["G21F1/02"], 1),    # match (starts with G21)
            ("P2", ["HG21X"], 1),       # no match (doesn't start with G21)
        ])
        cfg = _make_config(["G21"])
        # P1 matches, P2 doesn't — 0.5
        assert score_moat("EDGE", cfg) == 0.5

    def test_different_tickers_isolated(self, in_memory_db):
        """Scoring ticker A doesn't leak info from ticker B."""
        from sec_filing_intelligence.db import run_migration
        from sec_filing_intelligence.moat_scorer import score_moat
        run_migration()
        _seed_patents(in_memory_db, "TICKERA", [
            ("PA1", ["G21F1/02"], 1),
        ])
        _seed_patents(in_memory_db, "TICKERB", [
            ("PB1", ["H01L29/00"], 1),
            ("PB2", ["H01L29/01"], 1),
        ])
        cfg = _make_config(["G21"])
        assert score_moat("TICKERA", cfg) == 1.0
        assert score_moat("TICKERB", cfg) == 0.0
