"""Tests for universe seeding and the fresh-clone bootstrap path."""

from unittest.mock import patch

import pytest

from sec_filing_intelligence import db as db_mod
from sec_filing_intelligence import universe
from sec_filing_intelligence.config import identity_is_placeholder
from sec_filing_intelligence.db import get_active_tickers, get_connection


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    """Point the module-global DB at a fresh temp file and re-arm migration."""
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "data" / "universe_test.db")
    monkeypatch.setattr(db_mod, "_migrated", False)
    return tmp_path


CIK_MAP = {
    "RCAT": {"cik": "0001526119", "company_name": "Red Cat Holdings, Inc."},
    "AEHR": {"cik": "0001040470", "company_name": "Aehr Test Systems"},
}


class TestGetConnectionCreatesParentDir:
    def test_missing_data_dir_is_created(self, fresh_db):
        # DB parent (tmp/data/) does not exist yet — a fresh clone's exact state.
        assert not (fresh_db / "data").exists()
        with get_connection() as conn:
            conn.execute("SELECT 1")
        assert (fresh_db / "data").exists()


class TestLoadTickerFile:
    def test_comments_blanks_dupes_case(self, tmp_path):
        f = tmp_path / "u.txt"
        f.write_text("# header\nrcat  # inline comment\n\nAEHR\nRCAT\n  bksy\n")
        assert universe.load_ticker_file(f) == ["RCAT", "AEHR", "BKSY"]

    def test_sample_file_ships_and_parses(self):
        from pathlib import Path

        sample = Path(__file__).parent.parent / "examples" / "sample_universe.txt"
        tickers = universe.load_ticker_file(sample)
        assert len(tickers) >= 20
        assert all(t.isalpha() and t == t.upper() for t in tickers)


class TestSeedUniverse:
    def test_seed_activates_tickers_with_ciks(self, fresh_db):
        summary = universe.seed_universe(["RCAT", "AEHR"], cik_map=CIK_MAP)
        assert summary == {"added": 2, "updated": 0, "no_cik": 0, "total": 2}
        assert get_active_tickers() == ["AEHR", "RCAT"]
        with get_connection() as conn:
            row = conn.execute(
                "SELECT cik, company_name FROM scr_universe WHERE ticker='RCAT'"
            ).fetchone()
        assert row["cik"] == "0001526119"
        assert "Red Cat" in row["company_name"]

    def test_reseed_is_idempotent_and_updates(self, fresh_db):
        universe.seed_universe(["RCAT"], cik_map=CIK_MAP)
        summary = universe.seed_universe(["RCAT", "AEHR"], cik_map=CIK_MAP)
        assert summary["added"] == 1
        assert summary["updated"] == 1
        assert get_active_tickers() == ["AEHR", "RCAT"]

    def test_unknown_ticker_seeded_without_cik(self, fresh_db):
        summary = universe.seed_universe(["ZZZFAKE"], cik_map=CIK_MAP)
        assert summary["no_cik"] == 1
        assert get_active_tickers() == ["ZZZFAKE"]

    def test_reseed_fills_in_cik_without_clobbering(self, fresh_db):
        universe.seed_universe(["RCAT"], cik_map={})  # offline first pass
        with get_connection() as conn:
            assert conn.execute(
                "SELECT cik FROM scr_universe WHERE ticker='RCAT'"
            ).fetchone()["cik"] is None
        universe.seed_universe(["RCAT"], cik_map=CIK_MAP)  # online second pass
        with get_connection() as conn:
            assert conn.execute(
                "SELECT cik FROM scr_universe WHERE ticker='RCAT'"
            ).fetchone()["cik"] == "0001526119"


class TestFetchCikMapOffline:
    def test_network_failure_returns_empty_map(self):
        with patch.object(universe.requests, "get", side_effect=OSError("offline")):
            assert universe.fetch_cik_map() == {}


class TestCli:
    def test_add_and_list(self, fresh_db, capsys):
        with patch.object(universe, "fetch_cik_map", return_value=CIK_MAP):
            assert universe.main(["--add", "rcat,AEHR"]) == 0
        assert universe.main(["--list"]) == 0
        out = capsys.readouterr().out
        assert "RCAT" in out and "AEHR" in out and "2 active tickers" in out

    def test_seed_from_file(self, fresh_db, tmp_path, capsys):
        f = tmp_path / "u.txt"
        f.write_text("RCAT\n")
        with patch.object(universe, "fetch_cik_map", return_value=CIK_MAP):
            assert universe.main(["--seed", str(f)]) == 0
        assert "1 new" in capsys.readouterr().out

    def test_list_empty_universe_exits_nonzero(self, fresh_db, capsys):
        assert universe.main(["--list"]) == 1
        assert "empty" in capsys.readouterr().out.lower()


class TestPlaceholderIdentity:
    def test_default_is_placeholder_and_warns(self, monkeypatch, capsys):
        from sec_filing_intelligence import config

        monkeypatch.setattr(config, "EDGAR_USER_AGENT", config._PLACEHOLDER_IDENTITY)
        assert config.identity_is_placeholder()
        assert config.warn_if_placeholder_identity() is True
        assert "fair-access" in capsys.readouterr().err

    def test_real_identity_does_not_warn(self, monkeypatch, capsys):
        from sec_filing_intelligence import config

        monkeypatch.setattr(config, "EDGAR_USER_AGENT", "Jane Doe jane@realdomain.io")
        assert config.warn_if_placeholder_identity() is False
        assert capsys.readouterr().err == ""

    def test_module_level_helper_importable(self):
        assert callable(identity_is_placeholder)


class TestPollerUniverseFilter:
    def test_seeded_ciks_reach_the_poller_filter(self, fresh_db):
        from sec_filing_intelligence import form4_rss_poller as poller

        db_mod.run_migration()
        with get_connection() as conn:
            assert poller._universe_cik_set(conn) == set()
        universe.seed_universe(["RCAT"], cik_map=CIK_MAP)
        with get_connection() as conn:
            assert poller._universe_cik_set(conn) == {"0001526119"}
