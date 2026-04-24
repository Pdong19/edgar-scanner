"""Tests for the AMPX rules screener."""
import sqlite3
import pytest

from sec_filing_intelligence import db as screener_db
from sec_filing_intelligence.config import (
    TABLE_AMPX_RULES_SCORES,
    TABLE_AMPX_RED_FLAGS,
)


def test_ampx_rules_scores_table_exists_after_migration(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        row = con.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_AMPX_RULES_SCORES}'"
        ).fetchone()
        assert row is not None, f"{TABLE_AMPX_RULES_SCORES} missing after migration"
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_AMPX_RULES_SCORES})")}
        expected = {
            "ticker", "scan_date", "score", "score_details",
            "dim1_crash", "dim2_revgrowth", "dim3_debt", "dim4_runway",
            "dim5_float", "dim6_institutional", "dim7_analyst", "dim8_priority_industry",
            "dim9_short", "dim10_leaps", "dim11_insider", "has_going_concern",
        }
        assert expected <= cols, f"Missing columns: {expected - cols}"


def test_ampx_red_flags_table_exists_after_migration(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        row = con.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_AMPX_RED_FLAGS}'"
        ).fetchone()
        assert row is not None
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_AMPX_RED_FLAGS})")}
        assert {"ticker", "scan_date", "flag_type", "matched_phrase"} <= cols


def test_populate_analyst_coverage_writes_row(tmp_path, monkeypatch):
    """Given a yfinance info dict with numberOfAnalystOpinions, the populator writes to scr_analyst_coverage."""
    from sec_filing_intelligence import fundamentals
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()

    fake_info = {"numberOfAnalystOpinions": 3}
    fundamentals._populate_analyst_coverage("XYZ", fake_info, str(test_db))

    with sqlite3.connect(test_db) as con:
        row = con.execute(
            "SELECT analyst_count, date FROM scr_analyst_coverage WHERE ticker='XYZ'"
        ).fetchone()
        assert row is not None
        assert row[0] == 3
        from datetime import date as _d
        assert row[1] == _d.today().isoformat()


def test_populate_analyst_coverage_handles_missing_field(tmp_path, monkeypatch):
    """If numberOfAnalystOpinions is absent from info, write analyst_count=0 rather than erroring."""
    from sec_filing_intelligence import fundamentals
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()

    fundamentals._populate_analyst_coverage("ABC", {}, str(test_db))

    with sqlite3.connect(test_db) as con:
        row = con.execute(
            "SELECT analyst_count FROM scr_analyst_coverage WHERE ticker='ABC'"
        ).fetchone()
        assert row is not None
        assert row[0] == 0


def test_populate_analyst_coverage_handles_nan(tmp_path, monkeypatch):
    """If yfinance leaks NaN into info dict, write analyst_count=0 rather than raising."""
    from sec_filing_intelligence import fundamentals
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()

    fake_info = {"numberOfAnalystOpinions": float("nan")}
    fundamentals._populate_analyst_coverage("NANCO", fake_info, str(test_db))

    with sqlite3.connect(test_db) as con:
        row = con.execute(
            "SELECT analyst_count FROM scr_analyst_coverage WHERE ticker='NANCO'"
        ).fetchone()
        assert row is not None
        assert row[0] == 0


def test_ampx_rules_module_importable():
    from sec_filing_intelligence import ampx_rules
    assert hasattr(ampx_rules, "main"), "ampx_rules.main entrypoint missing"
    assert hasattr(ampx_rules, "logger"), "ampx_rules.logger missing"


def test_ampx_rules_cli_help_exits_zero():
    import subprocess
    import sys
    result = subprocess.run(
        [sys.executable, "-m", "sec_filing_intelligence.ampx_rules", "--help"],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, f"--help failed: {result.stderr}"
    assert "--run" in result.stdout
    assert "--rescore" in result.stdout
    assert "--top" in result.stdout
    assert "--trend" in result.stdout


def _seed_fixture_db(db_path: str) -> None:
    """Minimal fixture DB with scr_universe + scr_fundamentals + scr_price_metrics + scr_kill_list.

    Drops migration-created versions of the tables we seed (schema differs from fixture's
    minimal shape) and recreates the simple shapes the fixture expects. Other migration
    tables are left intact.
    """
    import sqlite3
    with sqlite3.connect(db_path) as con:
        con.executescript("""
            DROP TABLE IF EXISTS scr_universe;
            DROP TABLE IF EXISTS scr_fundamentals;
            DROP TABLE IF EXISTS scr_price_metrics;
            DROP TABLE IF EXISTS scr_kill_list;
            DROP TABLE IF EXISTS scr_analyst_coverage;
            DROP TABLE IF EXISTS scr_insider_transactions;
            CREATE TABLE scr_universe (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                avg_volume REAL,
                is_active INTEGER DEFAULT 1,
                is_killed INTEGER DEFAULT 0,
                is_spac INTEGER DEFAULT 0,
                source TEXT
            );
            CREATE TABLE scr_fundamentals (
                ticker TEXT,
                date TEXT,
                market_cap REAL,
                revenue_growth_yoy REAL,
                revenue_growth_qoq REAL,
                revenue_ttm REAL,
                debt_to_equity REAL,
                cash_runway_quarters REAL,
                shares_outstanding REAL,
                institutional_ownership REAL,
                cash_and_equivalents REAL,
                total_debt REAL
            );
            CREATE TABLE scr_price_metrics (
                ticker TEXT PRIMARY KEY,
                current_price REAL,
                pct_from_52w_high REAL,
                pct_from_ath REAL,
                float_shares REAL,
                short_interest_pct REAL
            );
            CREATE TABLE scr_kill_list (
                ticker TEXT PRIMARY KEY,
                reason TEXT,
                killed_date TEXT
            );
            CREATE TABLE IF NOT EXISTS scr_analyst_coverage (
                ticker TEXT,
                date TEXT,
                analyst_count INTEGER,
                UNIQUE(ticker, date)
            );
            CREATE TABLE IF NOT EXISTS scr_insider_transactions (
                ticker TEXT, filing_date TEXT, insider_name TEXT, insider_title TEXT,
                transaction_type TEXT, shares INTEGER, price_per_share REAL,
                total_value REAL, shares_owned_after INTEGER, sec_url TEXT,
                is_open_market INTEGER DEFAULT 0,
                is_amended INTEGER DEFAULT 0
            );
        """)
        # PASS: small-cap semi, crashed, liquid, tech sector
        con.execute("INSERT INTO scr_universe VALUES ('PASS1', 'Semi Co', 'Technology', 'Semiconductors', 200000000, 500000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, revenue_ttm, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('PASS1', '2026-04-14', 200000000, 1.2, 10000000, 0.05, 10, 50000000, 0.15)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('PASS1', 2.0, -85, -90, 40000000, 20)")
        # KILL: on kill list
        con.execute("INSERT INTO scr_universe VALUES ('KILL1', 'Dead Co', 'Technology', 'Widgets', 100000000, 500000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('KILL1', '2026-04-14', 100000000, 0.5, 0.1, 8, 60000000, 0.2)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('KILL1', 3.0, -80, -85, 50000000, 10)")
        con.execute("INSERT INTO scr_kill_list VALUES ('KILL1', 'shell company', '2026-01-01')")
        # BIGCAP: over $500M -- should be filtered out
        con.execute("INSERT INTO scr_universe VALUES ('BIGCAP', 'Mega Co', 'Technology', 'Software', 5000000000, 10000000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('BIGCAP', '2026-04-14', 5000000000, 0.3, 0.2, 10, 200000000, 0.6)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('BIGCAP', 100.0, -85, -90, 100000000, 5)")
        # NOTCRSH: up-trending, should fail crash gate
        con.execute("INSERT INTO scr_universe VALUES ('NOTCRSH', 'Hot Co', 'Technology', 'Cloud', 150000000, 500000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('NOTCRSH', '2026-04-14', 150000000, 0.6, 0.1, 10, 50000000, 0.2)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('NOTCRSH', 10.0, -5, -10, 50000000, 3)")
        # ILLIQ: crashed but low volume
        con.execute("INSERT INTO scr_universe VALUES ('ILLIQ', 'Thin Co', 'Technology', 'Semiconductors', 100000000, 500, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('ILLIQ', '2026-04-14', 100000000, 0.8, 0.1, 12, 40000000, 0.1)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('ILLIQ', 1.0, -90, -95, 30000000, 15)")
        # HEALTH: healthcare sector -- should be excluded
        con.execute("INSERT INTO scr_universe VALUES ('HEALTH', 'Bio Co', 'Healthcare', 'Biotech', 100000000, 500000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('HEALTH', '2026-04-14', 100000000, 0.9, 0.05, 8, 40000000, 0.15)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('HEALTH', 4.0, -75, -80, 35000000, 18)")
        # UNK: sector 'Unknown' -- should be excluded
        con.execute("INSERT INTO scr_universe VALUES ('UNK', 'Mystery Co', 'Unknown', 'Unknown', 100000000, 500000, 1, 0, 0, 'edgar')")
        con.execute("INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, debt_to_equity, cash_runway_quarters, shares_outstanding, institutional_ownership) VALUES ('UNK', '2026-04-14', 100000000, 0.5, 0.1, 8, 50000000, 0.2)")
        con.execute("INSERT INTO scr_price_metrics VALUES ('UNK', 3.0, -85, -90, 40000000, 12)")
        # Analyst coverage -- populated per A3 to scr_analyst_coverage(ticker, date, analyst_count)
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('PASS1', '2026-04-14', 1)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('KILL1', '2026-04-14', 1)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('BIGCAP', '2026-04-14', 10)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('NOTCRSH', '2026-04-14', 2)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('ILLIQ', '2026-04-14', 0)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('HEALTH', '2026-04-14', 2)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('UNK', '2026-04-14', 1)")


def test_fetch_candidates_applies_all_gates(tmp_path, monkeypatch):
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "fixture.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    rows = ampx_rules.fetch_candidates(db_path=str(test_db))
    tickers = {r["ticker"] for r in rows}
    assert tickers == {"PASS1"}, f"Expected only PASS1, got {tickers}"


def test_priority_industry_word_boundary():
    from sec_filing_intelligence.ampx_rules import score_priority_industry

    # Positive cases — exact word match
    assert score_priority_industry("Semi Co", "Semiconductors") == 1.0
    assert score_priority_industry("AI Corp", "Software") == 1.0
    assert score_priority_industry("Drone Maker", "Aerospace") == 1.0
    assert score_priority_industry("Rare Earth Inc", "Mining") == 1.0
    assert score_priority_industry("EV Charging Networks", "Consumer") == 1.0

    # Negative case — substring-only should NOT match
    assert score_priority_industry("Airline Co", "Transport") == 0.0, \
        "'AI' must not match 'AIRLINE' — word boundary required"
    assert score_priority_industry("Cooling Towers Inc", "Industrial") == 1.0, \
        "'cooling' is in the keyword list — should match at word boundary"

    # Empty / None safety
    assert score_priority_industry("", "") == 0.0
    assert score_priority_industry(None, None) == 0.0


def test_going_concern_regex_catches_all_phrasings():
    from sec_filing_intelligence.ampx_rules import scan_going_concern_text

    positive_cases = [
        "There is substantial doubt about our ability to continue as a going concern.",
        "These conditions raise substantial doubt about the Company's ability to continue.",
        "Management believes these factors indicate substantial doubt regarding going concern.",
        "The Company may not be able to continue as a going concern.",
        "There is significant uncertainty regarding the Company's ability to operate.",
    ]
    for text in positive_cases:
        hit = scan_going_concern_text(text)
        assert hit is not None, f"Missed: {text}"

    negative_cases = [
        "The Company is operating profitably with substantial reserves.",
        "We continue to invest in our business.",
        "Management is doubtful the acquisition will close on time.",
    ]
    for text in negative_cases:
        assert scan_going_concern_text(text) is None, f"False positive: {text}"


def test_leaps_check_detects_long_dated_expirations(monkeypatch):
    from sec_filing_intelligence import ampx_rules
    from datetime import date, timedelta

    far = (date.today() + timedelta(days=400)).isoformat()
    near = (date.today() + timedelta(days=60)).isoformat()

    class FakeTicker:
        def __init__(self, symbol):
            self.symbol = symbol
        @property
        def options(self):
            if self.symbol == "HASLEAP":
                return (near, far)
            if self.symbol == "OPTONLY":
                return (near,)
            return ()

    monkeypatch.setattr(ampx_rules, "_yf_ticker", lambda s: FakeTicker(s))

    assert ampx_rules.check_leaps("HASLEAP") == "leaps"
    assert ampx_rules.check_leaps("OPTONLY") == "options"
    assert ampx_rules.check_leaps("NONE") == "none"


def test_score_dim10_leaps():
    from sec_filing_intelligence.ampx_rules import score_leaps
    assert score_leaps("leaps") == 0.5
    assert score_leaps("options") == 0.25
    assert score_leaps("none") == 0.0
    # Safety: unknown status returns 0
    assert score_leaps("") == 0.0
    assert score_leaps("bogus") == 0.0


def test_check_reverse_splits(monkeypatch):
    from datetime import datetime, timedelta, timezone
    import pandas as pd
    from sec_filing_intelligence import ampx_rules

    now = datetime.now(tz=timezone.utc)
    fresh = now - timedelta(days=200)
    older = now - timedelta(days=800)
    ancient = now - timedelta(days=2000)

    class FakeTicker:
        def __init__(self, symbol):
            self.symbol = symbol
            data = {
                "THREE_RS": pd.Series(
                    [0.5, 0.013, 0.004],  # all reverse
                    index=pd.to_datetime([older, fresh, now - timedelta(days=100)], utc=True),
                ),
                "ONE_RS": pd.Series(
                    [0.5], index=pd.to_datetime([fresh], utc=True),
                ),
                "OLD_RS_ONLY": pd.Series(
                    [0.5, 0.5], index=pd.to_datetime([ancient, ancient], utc=True),
                ),
                "NORMAL_SPLIT": pd.Series(
                    [2.0, 3.0], index=pd.to_datetime([fresh, older], utc=True),
                ),
                "CLEAN": pd.Series([], dtype="float64"),
            }
            self.splits = data.get(symbol, pd.Series([], dtype="float64"))

    monkeypatch.setattr(ampx_rules, "_yf_ticker", lambda s: FakeTicker(s))

    assert ampx_rules.check_reverse_splits("THREE_RS") == 3
    assert ampx_rules.check_reverse_splits("ONE_RS") == 1
    assert ampx_rules.check_reverse_splits("OLD_RS_ONLY") == 0  # outside 3yr window
    assert ampx_rules.check_reverse_splits("NORMAL_SPLIT") == 0  # forward splits don't count
    assert ampx_rules.check_reverse_splits("CLEAN") == 0


def test_insider_aggregator_counts_cluster_buys(tmp_path, monkeypatch):
    import sqlite3
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "ins.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()

    # Fixture replaces the scr_insider_transactions table to match the live schema.
    with sqlite3.connect(test_db) as con:
        con.executescript("""
            DROP TABLE IF EXISTS scr_insider_transactions;
            CREATE TABLE scr_insider_transactions (
                ticker TEXT, filing_date TEXT, insider_name TEXT, insider_title TEXT,
                transaction_type TEXT, shares INTEGER, price_per_share REAL,
                total_value REAL, shares_owned_after INTEGER, sec_url TEXT,
                is_open_market INTEGER DEFAULT 0,
                is_amended INTEGER DEFAULT 0
            );
        """)
        # CLUS: 3 different insiders bought within 10 days — CLUSTER
        con.execute("INSERT INTO scr_insider_transactions (ticker, filing_date, is_open_market, transaction_type, insider_name) VALUES ('CLUS', '2026-04-01', 1, 'purchase', 'Alice')")
        con.execute("INSERT INTO scr_insider_transactions (ticker, filing_date, is_open_market, transaction_type, insider_name) VALUES ('CLUS', '2026-04-05', 1, 'purchase', 'Bob')")
        con.execute("INSERT INTO scr_insider_transactions (ticker, filing_date, is_open_market, transaction_type, insider_name) VALUES ('CLUS', '2026-04-10', 1, 'purchase', 'Carol')")
        # SOLO: only one insider
        con.execute("INSERT INTO scr_insider_transactions (ticker, filing_date, is_open_market, transaction_type, insider_name) VALUES ('SOLO', '2026-04-05', 1, 'purchase', 'Dan')")
        # OLD: buy 100 days ago (outside 90d lookback)
        con.execute("INSERT INTO scr_insider_transactions (ticker, filing_date, is_open_market, transaction_type, insider_name) VALUES ('OLD', '2026-01-01', 1, 'purchase', 'Eve')")

    as_of = "2026-04-14"
    clus = ampx_rules.count_insider_buys("CLUS", as_of, db_path=str(test_db))
    solo = ampx_rules.count_insider_buys("SOLO", as_of, db_path=str(test_db))
    old = ampx_rules.count_insider_buys("OLD", as_of, db_path=str(test_db))

    assert clus["cluster_count"] >= 2, f"CLUS should have cluster buys, got {clus}"
    assert clus["buy_count"] == 3
    assert solo["cluster_count"] == 0
    assert solo["buy_count"] == 1
    assert old["buy_count"] == 0


def test_score_dim11_insider():
    from sec_filing_intelligence.ampx_rules import score_insider_buying
    # >= AMPX_INSIDER_CLUSTER_MIN_COUNT cluster buys → 1.0
    assert score_insider_buying({"buy_count": 3, "cluster_count": 2}) == 1.0
    # Single buy → 0.5
    assert score_insider_buying({"buy_count": 1, "cluster_count": 0}) == 0.5
    # No buys → 0.0
    assert score_insider_buying({"buy_count": 0, "cluster_count": 0}) == 0.0


def test_score_row_full_rubric():
    """A ticker hitting every top-tier threshold should score ~12.5 under weighted scoring."""
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "TOPFULL",
        "company_name": "Semi Co",
        "industry": "Semiconductors",
        "pct_from_52w_high": -85,
        "pct_from_ath": -90,
        "revenue_growth_yoy": 1.5,      # 150%
        "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.05,
        "cash_runway_quarters": 10,
        "shares_outstanding": 50_000_000,
        "float_shares": 40_000_000,
        "institutional_ownership": 0.1,
        "analyst_count": 1,
        "short_interest_pct": 0.18,
    }
    leaps_status = "leaps"
    insider_agg = {"buy_count": 3, "cluster_count": 2}

    result = score_row(row, leaps_status, insider_agg)
    assert result["score"] == pytest.approx(12.5, abs=0.01), f"Got {result['score']}"
    assert result["dim1_crash"] == 2.0
    assert result["dim2_revgrowth"] == 2.0
    assert result["dim3_debt"] == 1.0
    assert result["dim4_runway"] == 1.5
    assert result["dim5_float"] == 1.0
    assert result["dim6_institutional"] == 1.0
    assert result["dim7_analyst"] == 1.0
    assert result["dim8_priority_industry"] == 1.0
    assert result["dim9_short"] == 0.5
    assert result["dim10_leaps"] == 0.5
    assert result["dim11_insider"] == 1.0


def test_score_row_middle_thresholds():
    from sec_filing_intelligence.ampx_rules import score_row
    row = {
        "ticker": "MID", "company_name": "Widget Co", "industry": "Misc",
        "pct_from_52w_high": -65, "pct_from_ath": -70,
        "revenue_growth_yoy": 0.4, "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.2, "cash_runway_quarters": 6,
        "shares_outstanding": 120_000_000, "float_shares": 130_000_000,
        "institutional_ownership": 0.3, "analyst_count": 2, "short_interest_pct": 0.10,
    }
    r = score_row(row, "options", {"buy_count": 1, "cluster_count": 0})
    assert r["dim1_crash"] == 1.0
    assert r["dim2_revgrowth"] == 0.5
    assert r["dim3_debt"] == 0.5
    assert r["dim4_runway"] == 0.75
    assert r["dim5_float"] == 0.5
    assert r["dim6_institutional"] == 0.5
    assert r["dim7_analyst"] == 0.5
    assert r["dim8_priority_industry"] == 0.0
    assert r["dim9_short"] == 0.0
    assert r["dim10_leaps"] == 0.25
    assert r["dim11_insider"] == 0.5


def test_score_row_new_ipo_fallback_applies_haircut():
    """If revenue_growth_yoy is NULL, use qoq * 4 with 0.75 haircut on dim2."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = {
        "ticker": "IPO", "company_name": "X", "industry": "y",
        "pct_from_52w_high": -80, "pct_from_ath": -80,
        "revenue_growth_yoy": None,
        "revenue_growth_qoq": 0.30,   # annualized = 1.2 → top tier
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.5, "cash_runway_quarters": 2,
        "shares_outstanding": 200_000_000, "float_shares": 200_000_000,
        "institutional_ownership": 0.8, "analyst_count": 10, "short_interest_pct": 5,
    }
    r = score_row(row, "none", {"buy_count": 0, "cluster_count": 0})
    # 2.0 * 0.75 haircut = 1.5 (weighted dim2, top tier)
    assert r["dim2_revgrowth"] == pytest.approx(1.5, abs=0.001)


def test_run_full_scan_end_to_end(tmp_path, monkeypatch):
    """Full pipeline against fixture DB: write rows to scr_ampx_rules_scores."""
    import sqlite3
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "fullscan.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(ampx_rules, "_DEFAULT_DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    # Stub out network-bound checks
    monkeypatch.setattr(ampx_rules, "check_leaps", lambda t: "options")
    monkeypatch.setattr(ampx_rules, "check_going_concern", lambda t: None)

    exit_code = ampx_rules.run_full_scan(send_digest=False, write_csv=False)
    assert exit_code == 0

    with sqlite3.connect(test_db) as con:
        rows = con.execute(
            "SELECT ticker, score FROM scr_ampx_rules_scores ORDER BY ticker"
        ).fetchall()
        tickers = [r[0] for r in rows]
        assert tickers == ["PASS1"], f"Expected just PASS1 scored, got {tickers}"
        assert 0 <= rows[0][1] <= 12.5


def test_run_full_scan_logs_reverse_split_as_red_flag(tmp_path, monkeypatch):
    """Reverse splits (>=2 in 3yr) remain the only hard-kill red flag post-Round-6."""
    import sqlite3
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "rf.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(ampx_rules, "_DEFAULT_DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    monkeypatch.setattr(ampx_rules, "check_leaps", lambda t: "none")
    monkeypatch.setattr(ampx_rules, "check_going_concern", lambda t: None)
    monkeypatch.setattr(ampx_rules, "check_reverse_splits", lambda t, **_: 3)

    exit_code = ampx_rules.run_full_scan(send_digest=False, write_csv=False)
    assert exit_code == 0

    with sqlite3.connect(test_db) as con:
        rf = con.execute(
            "SELECT ticker, flag_type FROM scr_ampx_red_flags"
        ).fetchall()
        assert any(t == "PASS1" and "reverse_split" in f for t, f in rf), (
            f"Reverse split destruction must red-flag; got {rf}"
        )
        scored = con.execute("SELECT ticker FROM scr_ampx_rules_scores").fetchall()
        assert scored == [], "Reverse-split hits should not be scored (hard kill)"


def test_top_n_prints_latest(tmp_path, monkeypatch, capsys):
    import sqlite3
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "top.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(ampx_rules, "_DEFAULT_DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        for tkr, sc in [("A", 9.5), ("B", 8.5), ("C", 7.5)]:
            con.execute(
                """INSERT INTO scr_ampx_rules_scores
                   (ticker, scan_date, score, dim1_crash, dim2_revgrowth, dim3_debt, dim4_runway,
                    dim5_float, dim6_institutional, dim7_analyst, dim8_priority_industry,
                    dim9_short, dim10_leaps, dim11_insider, has_going_concern)
                   VALUES (?, '2026-04-14', ?, 1,1,1,1,1,1,1,1,0.5,0.5,1,0)""",
                (tkr, sc),
            )

    rc = ampx_rules.print_top_n(2)
    captured = capsys.readouterr()
    assert rc == 0
    assert "A" in captured.out and "B" in captured.out
    assert "C" not in captured.out


# ── Tests: FUNDAMENTALS_SOURCE=yfinance_xbrl_fallback hybrid mode ───────────


def _seed_hybrid_fixture(db_path: str) -> None:
    """Fixture with three tickers to exercise hybrid SQL semantics:

    - RESCUE: yfinance revenue/growth NULL, XBRL populated → should appear
      with revenue_ttm from XBRL (the migration's original goal).
    - DIVERGE: both yfinance and XBRL populated with different values →
      should appear with yfinance value (yf remains primary in hybrid mode).
    - YFONLY: yfinance populated, XBRL absent → behaves as yfinance mode.
    """
    import sqlite3
    _seed_fixture_db(db_path)  # reuse the base universe/price_metrics/etc.
    with sqlite3.connect(db_path) as con:
        # Add scr_fundamentals_xbrl table (matches production schema subset)
        con.executescript("""
            DROP TABLE IF EXISTS scr_fundamentals_xbrl;
            CREATE TABLE scr_fundamentals_xbrl (
                ticker TEXT,
                fiscal_year INTEGER,
                revenue REAL,
                revenue_growth_yoy REAL,
                debt_to_equity REAL,
                cash_runway_quarters REAL,
                shares_outstanding REAL,
                cash_and_equivalents REAL,
                total_debt REAL,
                shares_outstanding_change_2yr_pct REAL,
                data_quality_flags TEXT,
                PRIMARY KEY (ticker, fiscal_year)
            );
        """)

        # RESCUE: tech small-cap, crashed, liquid, yf has no revenue/growth,
        # XBRL fills revenue+growth. Must appear in candidates with xbrl revenue.
        con.execute(
            "INSERT INTO scr_universe VALUES ('RESCUE', 'Rescue Co', 'Technology', 'Semiconductors', 100000000, 500000, 1, 0, 0, 'edgar')"
        )
        con.execute(
            """INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, revenue_ttm,
                                             debt_to_equity, cash_runway_quarters, shares_outstanding,
                                             institutional_ownership, cash_and_equivalents)
               VALUES ('RESCUE', '2026-04-14', 100000000, NULL, NULL, 0.05, 10, 50000000, 0.15, NULL)"""
        )
        con.execute("INSERT INTO scr_price_metrics VALUES ('RESCUE', 2.0, -85, -90, 40000000, 20)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('RESCUE', '2026-04-14', 1)")
        con.execute(
            """INSERT INTO scr_fundamentals_xbrl
               (ticker, fiscal_year, revenue, revenue_growth_yoy,
                debt_to_equity, shares_outstanding, cash_and_equivalents, data_quality_flags)
               VALUES ('RESCUE', 2025, 15000000, 0.8, 0.05, 50000000, 8000000, '[]')"""
        )

        # DIVERGE: both sources populated with different values
        con.execute(
            "INSERT INTO scr_universe VALUES ('DIVERGE', 'Diverge Co', 'Technology', 'Software', 150000000, 500000, 1, 0, 0, 'edgar')"
        )
        con.execute(
            """INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, revenue_ttm,
                                             debt_to_equity, cash_runway_quarters, shares_outstanding,
                                             institutional_ownership, cash_and_equivalents)
               VALUES ('DIVERGE', '2026-04-14', 150000000, 0.5, 50000000, 0.10, 8, 40000000, 0.20, 10000000)"""
        )
        con.execute("INSERT INTO scr_price_metrics VALUES ('DIVERGE', 3.0, -85, -90, 30000000, 15)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('DIVERGE', '2026-04-14', 1)")
        con.execute(
            """INSERT INTO scr_fundamentals_xbrl
               (ticker, fiscal_year, revenue, revenue_growth_yoy,
                debt_to_equity, shares_outstanding, cash_and_equivalents, data_quality_flags)
               VALUES ('DIVERGE', 2025, 80000000, 0.3, 0.10, 40000000, 10000000, '[]')"""
        )


def test_hybrid_mode_rescues_null_yfinance_tickers(tmp_path, monkeypatch):
    """In yfinance_xbrl_fallback mode, a ticker with yfinance revenue=NULL but
    XBRL revenue=populated should appear in candidates with revenue_ttm sourced
    from XBRL. This is the safe-first-deployment path: zero regression on
    already-working yfinance tickers, rescues for the ones yfinance missed.
    """
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "hybrid.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_hybrid_fixture(str(test_db))
    monkeypatch.setattr(ampx_rules, "FUNDAMENTALS_SOURCE", "yfinance_xbrl_fallback")

    rows = ampx_rules.fetch_candidates(db_path=str(test_db))
    by_ticker = {r["ticker"]: r for r in rows}

    # RESCUE must appear (the whole point of hybrid mode)
    assert "RESCUE" in by_ticker, f"RESCUE ticker missing from candidates: {list(by_ticker)}"
    assert by_ticker["RESCUE"]["revenue_ttm"] == 15_000_000, (
        "RESCUE should inherit revenue from XBRL when yfinance is NULL"
    )
    assert by_ticker["RESCUE"]["revenue_growth_yoy"] == pytest.approx(0.8), (
        "RESCUE growth should come from XBRL fallback"
    )


def test_hybrid_mode_source_priority_on_divergence(tmp_path, monkeypatch):
    """When both sources populate but disagree, hybrid mode applies per-field priority:

    - revenue_ttm: yfinance-primary (yf's TTM is more current than XBRL's FY-annual)
    - revenue_growth_yoy: XBRL-primary (XBRL is FY-vs-FY; yf's is quarter-YoY noise)

    Growth-field priority flipped 2026-04-15 after ABAT 10-K validation — yf's
    revenueGrowth of 88% was most-recent-quarter YoY; XBRL's 1,149% matched the
    10-K consolidated income statement (FY2025 $4.29M vs FY2024 $343K) exactly.
    yf's field is systematically unreliable for AMPX's annual-inflection thesis,
    particularly for explosive-growth tickers with non-December fiscal years.
    """
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "hybrid.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_hybrid_fixture(str(test_db))
    monkeypatch.setattr(ampx_rules, "FUNDAMENTALS_SOURCE", "yfinance_xbrl_fallback")

    rows = ampx_rules.fetch_candidates(db_path=str(test_db))
    by_ticker = {r["ticker"]: r for r in rows}

    assert "DIVERGE" in by_ticker
    # revenue_ttm: yf (50M) still wins over XBRL (80M) — yf's TTM is more recent
    assert by_ticker["DIVERGE"]["revenue_ttm"] == 50_000_000, (
        "DIVERGE revenue_ttm must be yfinance value (50M), not XBRL (80M) — yf stays primary on revenue"
    )
    # revenue_growth_yoy: XBRL (0.3) now wins over yf (0.5) — XBRL FY-vs-FY is authoritative
    assert by_ticker["DIVERGE"]["revenue_growth_yoy"] == pytest.approx(0.3), (
        "DIVERGE growth must be XBRL value (0.3), not yf (0.5) — XBRL wins on annual growth"
    )


# ── Round 6: signal-quality fixes (2026-04-14) ───────────────────────────────
# Addresses post-Round-5 CSV review findings:
#   1. ATH-only qualifiers with stable 52w prices aren't AMPX-pattern washouts
#   2. XBRL scaling_bug_suspected flag must zero REVGROWTH (NXTT false positive)
#   3. Apparent share reduction + reverse split = equity destruction, not buyback


def test_fetch_candidates_excludes_ath_only_without_30pct_52w_decline(tmp_path, monkeypatch):
    """ATH gate alone is insufficient — ticker must ALSO be down ≥30% from 52w high.

    AMPX at Sept 2024 low was -60% from 52w AND -97% from ATH, so any real AMPX
    candidate clears 30% comfortably. A ticker at -15% from 52w but -85% from ATH
    is a slow multi-year grinder (EVC-style), not a washout.
    """
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "ath.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    # ATHONLY: -15% from 52w (stable), -85% from ATH (old IPO grinder). Must be excluded.
    with sqlite3.connect(str(test_db)) as con:
        con.execute(
            "INSERT INTO scr_universe VALUES ('ATHONLY', 'Grinder Co', 'Technology', "
            "'Semiconductors', 150000000, 500000, 1, 0, 0, 'edgar')"
        )
        con.execute(
            "INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, "
            "revenue_ttm, debt_to_equity, cash_runway_quarters, shares_outstanding, "
            "institutional_ownership) VALUES ('ATHONLY', '2026-04-14', 150000000, 0.5, "
            "10000000, 0.1, 8, 40000000, 0.15)"
        )
        con.execute("INSERT INTO scr_price_metrics VALUES ('ATHONLY', 3.0, -15, -85, 35000000, 10)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('ATHONLY', '2026-04-14', 1)")

    rows = ampx_rules.fetch_candidates(db_path=str(test_db))
    tickers = {r["ticker"] for r in rows}
    assert "ATHONLY" not in tickers, (
        f"ATH-only qualifier (52w=-15%, ATH=-85%) should be excluded — not in washout mode. "
        f"Got: {tickers}"
    )


def test_fetch_candidates_keeps_ath_qualifier_when_52w_also_deep(tmp_path, monkeypatch):
    """ATH qualifier with 52w ≤ -30% IS valid washout — don't exclude.

    Guards against over-correction: the 52w floor should only remove stable-price
    slow-grinders, not real crashes that happen to trigger both gates.
    """
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "athdeep.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    # ATHDEEP: -45% from 52w AND -85% from ATH. Real washout, must pass.
    with sqlite3.connect(str(test_db)) as con:
        con.execute(
            "INSERT INTO scr_universe VALUES ('ATHDEEP', 'Washout Co', 'Technology', "
            "'Semiconductors', 100000000, 500000, 1, 0, 0, 'edgar')"
        )
        con.execute(
            "INSERT INTO scr_fundamentals (ticker, date, market_cap, revenue_growth_yoy, "
            "revenue_ttm, debt_to_equity, cash_runway_quarters, shares_outstanding, "
            "institutional_ownership) VALUES ('ATHDEEP', '2026-04-14', 100000000, 0.5, "
            "10000000, 0.1, 8, 40000000, 0.15)"
        )
        con.execute("INSERT INTO scr_price_metrics VALUES ('ATHDEEP', 2.0, -45, -85, 35000000, 10)")
        con.execute("INSERT INTO scr_analyst_coverage VALUES ('ATHDEEP', '2026-04-14', 1)")

    rows = ampx_rules.fetch_candidates(db_path=str(test_db))
    tickers = {r["ticker"] for r in rows}
    assert "ATHDEEP" in tickers, (
        f"ATH qualifier with 52w=-45% is a real washout and must pass. Got: {tickers}"
    )


def test_score_row_zeros_revgrowth_when_scaling_bug_flag_present():
    """XBRL scaling_bug_suspected = REVGROWTH forced to 0, even with high yoy.

    Fixes NXTT-class: scaling_bug detected upstream means revenue_growth_yoy is
    untrustworthy (derived from mis-scaled XBRL numbers). Don't reward apparent
    growth when the source data is flagged.
    """
    import json
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "NXTT",
        "company_name": "Scaling Bug Co", "industry": "Software",
        "pct_from_52w_high": -85, "pct_from_ath": -97,
        "revenue_growth_yoy": 5.0,   # 500% — looks amazing, but scaling bug means wrong
        "revenue_growth_qoq": None,
        "revenue_ttm": 50_000_000,
        "debt_to_equity": 0.05, "cash_runway_quarters": 10,
        "shares_outstanding": 5_000_000, "institutional_ownership": 0.1,
        "analyst_count": 0,
        "short_interest_pct": 0.1,
        "xbrl_data_quality_flags": json.dumps(["scaling_bug_suspected"]),
    }
    result = score_row(row, "none", {"buy_count": 0, "cluster_count": 0})
    assert result["dim2_revgrowth"] == 0.0, (
        f"scaling_bug_suspected must zero REVGROWTH; got {result['dim2_revgrowth']}"
    )


def test_score_row_preserves_revgrowth_when_flags_benign():
    """Other XBRL flags (no_debt_reported, incomplete_history, etc.) must NOT
    zero REVGROWTH — only scaling_bug_suspected blocks the growth signal.
    """
    import json
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "OK",
        "company_name": "OK Co", "industry": "Software",
        "pct_from_52w_high": -85, "pct_from_ath": -90,
        "revenue_growth_yoy": 1.5,   # 150% — legitimate
        "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.05, "cash_runway_quarters": 10,
        "shares_outstanding": 50_000_000, "institutional_ownership": 0.1,
        "analyst_count": 1,
        "short_interest_pct": 0.1,
        "xbrl_data_quality_flags": json.dumps(["no_debt_reported", "incomplete_history"]),
    }
    result = score_row(row, "none", {"buy_count": 0, "cluster_count": 0})
    assert result["dim2_revgrowth"] == 2.0, (
        f"Benign flags must not zero REVGROWTH; got {result['dim2_revgrowth']}"
    )


def test_score_row_penalizes_apparent_buyback_coinciding_with_reverse_split():
    """Reverse split in 3yr window + negative shares_outstanding_change = equity
    destruction, not buyback. Apply -1.0 drag (same as real dilution).

    Covers FCEL -92% / NXTT -97% case: 1:33 reverse split reports as -97% shares.
    Without this check, those tickers escape the drag despite being strictly
    worse than a 200% diluter.
    """
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "RS", "company_name": "Reverse Split Co", "industry": "Widgets",
        "pct_from_52w_high": -85, "pct_from_ath": -97,
        "revenue_growth_yoy": 0.2, "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.5, "cash_runway_quarters": 4,
        "shares_outstanding": 3_000_000, "institutional_ownership": 0.3,
        "analyst_count": 0,
        "short_interest_pct": 0.05,
        "shares_outstanding_change_2yr_pct": -97.0,  # looks like buyback, but IS reverse split
    }
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, reverse_split_count=1
    )
    assert result["dim12_dilution_drag"] == -1.0, (
        f"Apparent share reduction + reverse split should trigger -1.0 drag; "
        f"got {result['dim12_dilution_drag']}"
    )


def test_score_row_no_drag_for_genuine_buyback():
    """Legitimate buybacks (negative shares change, NO reverse split) must not be
    penalized. Only heavy issuance (>80% growth) or apparent-reduction + RS triggers drag.
    """
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "BB", "company_name": "Buyback Co", "industry": "Software",
        "pct_from_52w_high": -65, "pct_from_ath": -75,
        "revenue_growth_yoy": 0.2, "revenue_growth_qoq": None,
        "revenue_ttm": 20_000_000,
        "debt_to_equity": 0.1, "cash_runway_quarters": 12,
        "shares_outstanding": 50_000_000, "institutional_ownership": 0.2,
        "analyst_count": 2,
        "short_interest_pct": 0.02,
        "shares_outstanding_change_2yr_pct": -10.0,  # real modest buyback
    }
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, reverse_split_count=0
    )
    assert result["dim12_dilution_drag"] == 0.0, (
        f"Modest buyback without RS must not trigger drag; got {result['dim12_dilution_drag']}"
    )


def test_score_row_drag_still_fires_for_heavy_issuance():
    """Regression guard: extreme dilution (>=100%) must keep firing -1.0 drag
    under the R7 tiered thresholds. Round 6 binary threshold was 80%; R7
    moved the -1.0 tier to >=100%. UPXI at 3561% still clearly qualifies.
    """
    from sec_filing_intelligence.ampx_rules import score_row

    row = {
        "ticker": "UPXI", "company_name": "Dilute Co", "industry": "Crypto",
        "pct_from_52w_high": -85, "pct_from_ath": -90,
        "revenue_growth_yoy": 0.5, "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.1, "cash_runway_quarters": 8,
        "shares_outstanding": 100_000_000, "institutional_ownership": 0.1,
        "analyst_count": 1,
        "short_interest_pct": 0.1,
        "shares_outstanding_change_2yr_pct": 3561.0,  # the real UPXI number
    }
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, reverse_split_count=0
    )
    assert result["dim12_dilution_drag"] == -1.0, (
        f"Extreme issuance (>=100%) must trigger -1.0 drag; got {result['dim12_dilution_drag']}"
    )


# ── R7 dim12 tiered dilution drag (2026-04-15) ────────────────────────────────
# Refinement of R6's binary >80% → -1.0 threshold to catch moderate diluters
# (VUZI 25%, ARBE 40%, PDYN 78%) that previously escaped penalty unscathed.
# Tier boundaries: < 10 | 10-49.99 | 50-99.99 | >= 100.
# Boundary tests at 9/11/49/51/99/101 lock the thresholds against silent drift.


def _dilution_test_row(dilution_pct: float) -> dict:
    """Shared fixture for dilution tiering boundary tests."""
    return {
        "ticker": "DIL", "company_name": "Dilution Test Co", "industry": "Semiconductors",
        "pct_from_52w_high": -65, "pct_from_ath": -75,
        "revenue_growth_yoy": 0.2, "revenue_growth_qoq": None,
        "revenue_ttm": 20_000_000,
        "debt_to_equity": 0.1, "cash_runway_quarters": 12,
        "shares_outstanding": 50_000_000, "institutional_ownership": 0.2,
        "analyst_count": 2,
        "short_interest_pct": 0.02,
        "shares_outstanding_change_2yr_pct": dilution_pct,
    }


def test_dim12_tier_boundary_just_below_mild():
    """9% dilution must score 0.0 — just below the 10% no-drag/mild boundary."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(9.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == 0.0, (
        f"9% dilution at just-below-mild boundary must score 0.0; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_boundary_just_above_mild():
    """11% dilution must score -0.25 — just crossed into mild tier."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(11.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -0.25, (
        f"11% dilution at just-above-mild boundary must score -0.25; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_boundary_just_below_moderate():
    """49% dilution must score -0.25 — just below 50% moderate boundary."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(49.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -0.25, (
        f"49% dilution at just-below-moderate boundary must score -0.25; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_boundary_just_above_moderate():
    """51% dilution must score -0.5 — just crossed into moderate tier."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(51.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -0.5, (
        f"51% dilution at just-above-moderate boundary must score -0.5; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_boundary_just_below_heavy():
    """99% dilution must score -0.5 — just below 100% heavy boundary."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(99.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -0.5, (
        f"99% dilution at just-below-heavy boundary must score -0.5; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_boundary_just_above_heavy():
    """101% dilution must score -1.0 — just crossed into heavy tier (AMPG/ABAT zone)."""
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(101.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -1.0, (
        f"101% dilution at just-above-heavy boundary must score -1.0; got {result['dim12_dilution_drag']}"
    )


def test_dim12_no_drag_for_small_secondary_offering():
    """A healthy company doing a modest 6% secondary to fund a specific initiative
    must NOT trigger dilution drag. Real-world case: profitable small-cap raises
    ~5-8% shares for a targeted acquisition, R&D program, or debt repayment.
    The point of the mild threshold at 10% is to avoid punishing normal capital
    activity; only sustained multi-raise dilution should drag scores down.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    # 6% dilution is squarely within "small secondary" territory — below mild tier
    result = score_row(_dilution_test_row(6.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == 0.0, (
        f"6% dilution (small healthy secondary) must NOT trigger drag; got {result['dim12_dilution_drag']}"
    )


def test_dim12_tier_moderate_catches_ampx_bucket_b_diluters():
    """Real-world sanity: VUZI at 25% and ARBE at 40% should land in the -0.25
    moderate tier that R6's binary threshold missed. These are the mid-range
    diluters the tiering is designed to catch.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    # VUZI-shaped case: 25% 2yr dilution
    vuzi = score_row(_dilution_test_row(25.0), "none", {"buy_count": 0, "cluster_count": 0},
                     reverse_split_count=0)
    assert vuzi["dim12_dilution_drag"] == -0.25, (
        f"VUZI-shaped 25% dilution must score -0.25 (not 0.0 as under R6 binary); "
        f"got {vuzi['dim12_dilution_drag']}"
    )
    # ARBE-shaped case: 40% 2yr dilution
    arbe = score_row(_dilution_test_row(40.0), "none", {"buy_count": 0, "cluster_count": 0},
                     reverse_split_count=0)
    assert arbe["dim12_dilution_drag"] == -0.25, (
        f"ARBE-shaped 40% dilution must score -0.25; got {arbe['dim12_dilution_drag']}"
    )


def test_dim12_tier_significant_catches_pdyn_class():
    """PDYN's 78% dilution previously escaped the 80% binary threshold. Under
    R7 tiered thresholds, 78% lands in the -0.5 significant tier.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    result = score_row(_dilution_test_row(78.0), "none", {"buy_count": 0, "cluster_count": 0},
                       reverse_split_count=0)
    assert result["dim12_dilution_drag"] == -0.5, (
        f"PDYN-shaped 78% dilution must score -0.5 (was 0.0 under R6 80% cliff); "
        f"got {result['dim12_dilution_drag']}"
    )


def test_dim12_rs_mask_still_fires_at_full_drag_regardless_of_magnitude():
    """Reverse-split-masked dilution stays at -1.0 regardless of absolute
    magnitude of the shares_outstanding_change_2yr_pct field — the magnitude
    is meaningless when the sign is wrong (RS reports negative shares change
    even though equity was destroyed, not bought back).
    """
    from sec_filing_intelligence.ampx_rules import score_row
    # Small-magnitude RS-masked case: -5% (near zero) but RS happened
    small = score_row(_dilution_test_row(-5.0), "none", {"buy_count": 0, "cluster_count": 0},
                      reverse_split_count=1)
    assert small["dim12_dilution_drag"] == -1.0, (
        f"RS-masked -5% must still score -1.0; got {small['dim12_dilution_drag']}"
    )
    # Large-magnitude RS-masked case: -97% (NXTT-style)
    big = score_row(_dilution_test_row(-97.0), "none", {"buy_count": 0, "cluster_count": 0},
                    reverse_split_count=1)
    assert big["dim12_dilution_drag"] == -1.0, (
        f"RS-masked -97% must still score -1.0; got {big['dim12_dilution_drag']}"
    )


# ── Round 6 #4: tiered going-concern penalty (2026-04-14) ────────────────────
# Going-concern is information, not a verdict. Tickers keep scoring through the
# full rubric and take a tiered penalty based on how close they actually are
# to dying (runway + revenue trajectory). User gets visibility + risk labeling
# instead of a silent hard-kill regex making the judgment call.


def _gc_row_baseline():
    """Shared test row for GC penalty tests — tuned so total before GC is ~7.0."""
    return {
        "ticker": "GC",
        "company_name": "GC Co", "industry": "Semiconductors",
        "pct_from_52w_high": -85, "pct_from_ath": -90,
        "revenue_growth_yoy": 0.3, "revenue_growth_qoq": None,
        "revenue_ttm": 10_000_000,
        "debt_to_equity": 0.05,
        "cash_runway_quarters": 10,
        "shares_outstanding": 50_000_000, "institutional_ownership": 0.15,
        "analyst_count": 1,
        "short_interest_pct": 0.02,
    }


def test_score_row_gc_penalty_with_long_runway():
    """GC + runway >=6Q = -0.5 (boilerplate auditor conservatism)."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 10
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -0.5, f"Got {result['gc_penalty']}"
    assert result["has_going_concern"] is True


def test_score_row_gc_penalty_with_medium_runway():
    """GC + runway 3-6Q = -1.0 (real but manageable risk)."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 4
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -1.0, f"Got {result['gc_penalty']}"


def test_score_row_gc_penalty_with_short_runway():
    """GC + runway <3Q = -1.5 (genuinely at risk of running out)."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 2
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -1.5, f"Got {result['gc_penalty']}"


def test_score_row_gc_penalty_with_unknown_runway_is_worst_case():
    """GC + runway None is treated as worst-case (-1.5). Absence of runway
    data on a GC-flagged name is a red flag — don't give benefit of the doubt.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = None
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -1.5, f"Got {result['gc_penalty']}"


def test_score_row_gc_penalty_stacks_with_shrinking_revenue():
    """GC + negative YoY revenue = additional -0.5 on top of runway tier.
    Shrinking business + going-concern warning is the profile that actually
    goes to zero.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 10          # -0.5 tier
    row["revenue_growth_yoy"] = -0.05         # shrinking → additional -0.5
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -1.0, (
        f"GC + long runway + shrinking = -0.5 + -0.5 = -1.0; got {result['gc_penalty']}"
    )


def test_score_row_gc_penalty_floor_minus_two():
    """GC + short runway + shrinking revenue = -1.5 + -0.5 = -2.0 floor."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 2
    row["revenue_growth_yoy"] = -0.10
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert result["gc_penalty"] == -2.0, f"Got {result['gc_penalty']}"


def test_score_row_no_gc_penalty_when_flag_absent():
    """has_going_concern=False → no penalty, no flag."""
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 2
    result = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=False
    )
    assert result["gc_penalty"] == 0.0
    assert result["has_going_concern"] is False


def test_score_row_gc_penalty_reduces_total():
    """GC penalty is applied to total score — same row scores 1.5 less under
    GC + short runway than when clean.
    """
    from sec_filing_intelligence.ampx_rules import score_row
    row = _gc_row_baseline()
    row["cash_runway_quarters"] = 2
    clean = score_row(row, "none", {"buy_count": 0, "cluster_count": 0})
    with_gc = score_row(
        row, "none", {"buy_count": 0, "cluster_count": 0}, has_going_concern=True
    )
    assert with_gc["score"] == pytest.approx(clean["score"] - 1.5, abs=0.01), (
        f"Expected clean-1.5; clean={clean['score']} gc={with_gc['score']}"
    )


def test_run_full_scan_scores_gc_tickers_with_penalty_not_red_flag(tmp_path, monkeypatch):
    """Round 6 #4: GC is no longer a hard kill. Affected tickers appear in
    scr_ampx_rules_scores with has_going_concern=1 and a tiered penalty baked
    into the total. Reverse splits remain the only hard kill.
    """
    import sqlite3
    from sec_filing_intelligence import ampx_rules, db as screener_db
    test_db = tmp_path / "gc.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(ampx_rules, "_DEFAULT_DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    _seed_fixture_db(str(test_db))

    monkeypatch.setattr(ampx_rules, "check_leaps", lambda t: "none")
    monkeypatch.setattr(
        ampx_rules, "check_going_concern",
        lambda t: "substantial doubt about the Company's ability to continue",
    )
    monkeypatch.setattr(ampx_rules, "check_reverse_splits", lambda t, **_: 0)

    exit_code = ampx_rules.run_full_scan(send_digest=False, write_csv=False)
    assert exit_code == 0

    with sqlite3.connect(test_db) as con:
        rf = con.execute(
            "SELECT ticker, flag_type FROM scr_ampx_red_flags"
        ).fetchall()
        assert ("PASS1", "going_concern") not in rf, (
            "GC tickers must no longer be red-flagged — they get scored with penalty"
        )
        scored = con.execute(
            "SELECT ticker, has_going_concern FROM scr_ampx_rules_scores"
        ).fetchall()
        assert any(t == "PASS1" and int(gc) == 1 for t, gc in scored), (
            f"PASS1 must be scored with has_going_concern=1; got {scored}"
        )


def test_dim11_excludes_rows_with_is_amended_true(tmp_path, monkeypatch):
    """dim 11 SQL must NOT count is_amended=1 rows toward insider cluster detection."""
    import sqlite3
    from sec_filing_intelligence.ampx_rules import count_insider_buys

    db_path = tmp_path / "test_amx.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript("""
            CREATE TABLE scr_insider_transactions (
                ticker TEXT, filing_date TEXT, insider_name TEXT,
                transaction_type TEXT, is_open_market INTEGER,
                is_amended INTEGER DEFAULT 0
            );
            INSERT INTO scr_insider_transactions
              (ticker, filing_date, insider_name, transaction_type, is_open_market, is_amended)
            VALUES
              ('AAPL', '2025-04-10', 'Alice', 'purchase', 1, 0),
              ('AAPL', '2025-04-11', 'Bob',   'purchase', 1, 0),
              ('AAPL', '2025-04-01', 'Alice', 'purchase', 1, 1);  -- amended, should NOT count
        """)
        con.commit()

    result = count_insider_buys("AAPL", "2025-04-15", db_path=str(db_path))
    # 2 non-amended purchases — not 3
    assert result["buy_count"] == 2
