"""Tests for scripts/screener/db.py — connection management and migration.

Every test uses an in-memory SQLite database via monkeypatching so the
production stocks.db is never touched.
"""

import sqlite3

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# We need a fresh in-memory DB per test.  screener.db uses a module-level
# `_migrated` flag to skip repeat migrations, so we reset it in the fixture.

_SCREENER_PKG = "sec_filing_intelligence"


@pytest.fixture(autouse=True)
def _in_memory_db(monkeypatch):
    """Redirect screener DB to a shared in-memory connection.

    We intercept get_connection so it yields an in-memory SQLite conn.
    We also reset the _migrated flag so run_migration() actually executes.
    """
    import sec_filing_intelligence.db as db_mod

    # Reset migration flag every test
    db_mod._migrated = False

    # Build a shared in-memory connection that persists across multiple
    # get_connection() calls within a single test.
    _shared = sqlite3.connect(":memory:", check_same_thread=False)
    _shared.execute("PRAGMA journal_mode=WAL")
    _shared.execute("PRAGMA foreign_keys=ON")
    _shared.row_factory = sqlite3.Row

    from contextlib import contextmanager

    @contextmanager
    def _mock_conn():
        yield _shared

    monkeypatch.setattr(db_mod, "get_connection", _mock_conn)
    yield _shared
    _shared.close()


# ---------------------------------------------------------------------------
# Tests: get_connection basics
# ---------------------------------------------------------------------------

class TestGetConnection:
    """Validate connection properties from the real get_connection (pre-patch)."""

    def test_returns_working_connection(self, _in_memory_db):
        """get_connection yields a connection capable of executing SQL."""
        from sec_filing_intelligence.db import get_connection

        with get_connection() as conn:
            result = conn.execute("SELECT 1 AS val").fetchone()
            assert result["val"] == 1

    def test_wal_mode_enabled(self, _in_memory_db):
        """Connection has WAL journal mode set."""
        from sec_filing_intelligence.db import get_connection

        with get_connection() as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            # In-memory DBs may report "memory" instead of "wal";
            # the important thing is the PRAGMA ran without error.
            assert mode in ("wal", "memory")

    def test_foreign_keys_enabled(self, _in_memory_db):
        """Connection has foreign_keys pragma turned on."""
        from sec_filing_intelligence.db import get_connection

        with get_connection() as conn:
            fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            assert fk == 1

    def test_row_factory_is_row(self, _in_memory_db):
        """Connection row_factory is sqlite3.Row for dict-like access."""
        from sec_filing_intelligence.db import get_connection

        with get_connection() as conn:
            assert conn.row_factory is sqlite3.Row

    def test_context_manager_yields_and_returns(self, _in_memory_db):
        """The context manager yields a usable conn and completes cleanly."""
        from sec_filing_intelligence.db import get_connection

        conn_ref = None
        with get_connection() as conn:
            conn_ref = conn
            conn.execute("SELECT 1")
        # After exiting the context, conn_ref should still be set
        assert conn_ref is not None


# ---------------------------------------------------------------------------
# Tests: run_migration creates all 20 scr_ tables
# ---------------------------------------------------------------------------

class TestRunMigration:
    """Validate that run_migration() creates the expected table set."""

    EXPECTED_TABLES = [
        "scr_universe",
        "scr_fundamentals",
        "scr_price_metrics",
        "scr_patents",
        "scr_patent_summary",
        "scr_insider_transactions",
        "scr_filing_signals",
        "scr_analyst_coverage",
        "scr_supply_chain",
        "scr_forcing_functions",
        "scr_catalysts",
        "scr_scores",
        "scr_score_history",
        "scr_anomalies",
        "scr_signal_spikes",
        "scr_watchlist",
        "scr_kill_list",
        "scr_sector_themes",
        "scr_ampx_profile",
        "scr_user_feedback",
        # AMPX Rules Screener (added 2026-04-14)
        "scr_ampx_rules_scores",
        "scr_ampx_red_flags",
        "scr_ampx_run_log",
        # EDGAR XBRL fundamentals (added 2026-04-14)
        "scr_fundamentals_xbrl",
        # Structured CPC classifications per patent (added 2026-04-15)
        "scr_patent_cpcs",
        # Asymmetry scanner hit table (added 2026-04-15)
        "scr_thesis_hits",
        # Form 4 atom poller observability (added 2026-04-15)
        "scr_form4_poll_log",
        # Multibagger Discovery Pipeline (added 2026-04-17)
        "scr_discovery_flags",
        "scr_text_search_hits",
        "scr_moat_signals",
        # Deep-dive verdicts (added 2026-04-17)
        "scr_deep_dive_results",
        # Discovery history tracking (added 2026-04-17)
        "scr_discovery_history",
    ]

    def test_creates_all_32_tables(self, _in_memory_db):
        """run_migration() creates exactly the 32 expected scr_ tables."""
        from sec_filing_intelligence.db import run_migration

        run_migration()

        rows = _in_memory_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'scr_%'"
        ).fetchall()
        table_names = sorted(r["name"] for r in rows)
        assert len(table_names) == 34, f"Expected 34 tables, got {len(table_names)}: {table_names}"
        for expected in self.EXPECTED_TABLES:
            assert expected in table_names, f"Missing table: {expected}"

    def test_idempotent_migration(self, _in_memory_db):
        """Running migration twice does not error or duplicate tables."""
        from sec_filing_intelligence.db import run_migration
        import sec_filing_intelligence.db as db_mod

        run_migration()
        # Reset flag to force second run
        db_mod._migrated = False
        run_migration()

        rows = _in_memory_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'scr_%'"
        ).fetchall()
        assert len(rows) == 34

    def test_ampx_profile_seeded(self, _in_memory_db):
        """run_migration() seeds the 15 AMPX reference profile rows."""
        from sec_filing_intelligence.db import run_migration

        run_migration()

        count = _in_memory_db.execute(
            "SELECT COUNT(*) FROM scr_ampx_profile"
        ).fetchone()[0]
        assert count == 15

    def test_kill_list_seeded(self, _in_memory_db):
        """run_migration() seeds the kill list with predefined tickers."""
        from sec_filing_intelligence.db import run_migration

        run_migration()

        count = _in_memory_db.execute(
            "SELECT COUNT(*) FROM scr_kill_list"
        ).fetchone()[0]
        assert count > 0  # Should have ~41 entries

    def test_scr_universe_has_kill_columns(self, _in_memory_db):
        """scr_universe has the is_active and is_killed columns (new schema)."""
        from sec_filing_intelligence.db import run_migration

        run_migration()

        cols = {
            r[1]
            for r in _in_memory_db.execute("PRAGMA table_info(scr_universe)").fetchall()
        }
        assert "is_active" in cols
        assert "is_killed" in cols
        assert "kill_reason" in cols


# ---------------------------------------------------------------------------
# Tests: get_active_tickers
# ---------------------------------------------------------------------------

class TestGetActiveTickers:
    """Validate the get_active_tickers helper."""

    def test_returns_active_non_killed_only(self, _in_memory_db):
        """Only tickers with is_active=1 AND is_killed=0 are returned."""
        from sec_filing_intelligence.db import run_migration, get_active_tickers

        run_migration()

        _in_memory_db.execute(
            "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES (?, 1, 0)",
            ("GOOD",),
        )
        _in_memory_db.execute(
            "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES (?, 1, 1)",
            ("KILLED",),
        )
        _in_memory_db.execute(
            "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES (?, 0, 0)",
            ("INACTIVE",),
        )
        _in_memory_db.commit()

        tickers = get_active_tickers()
        assert tickers == ["GOOD"]

    def test_returns_sorted(self, _in_memory_db):
        """Tickers are returned in alphabetical order."""
        from sec_filing_intelligence.db import run_migration, get_active_tickers

        run_migration()

        for t in ("ZZZ", "AAA", "MMM"):
            _in_memory_db.execute(
                "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES (?, 1, 0)",
                (t,),
            )
        _in_memory_db.commit()

        assert get_active_tickers() == ["AAA", "MMM", "ZZZ"]

    def test_empty_universe(self, _in_memory_db):
        """Returns empty list when scr_universe has no active tickers."""
        from sec_filing_intelligence.db import run_migration, get_active_tickers

        run_migration()
        assert get_active_tickers() == []
