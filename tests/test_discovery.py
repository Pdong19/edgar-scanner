"""Tests for the Multibagger Discovery Pipeline DDL, config, and discovery search."""
import csv
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sec_filing_intelligence import db as screener_db
from sec_filing_intelligence import discovery as discovery_mod
from sec_filing_intelligence.config import (
    DISCOVERY_CONTEXT_MDA_BONUS,
    DISCOVERY_CONTEXT_RISK_FACTOR_PENALTY,
    DISCOVERY_CONTEXT_SELF_CLAIM_BONUS,
    DISCOVERY_HISTORY_NEW_TICKER_BONUS,
    TABLE_DISCOVERY_FLAGS,
    TABLE_DISCOVERY_HISTORY,
    TABLE_TEXT_SEARCH_HITS,
    TABLE_MOAT_SIGNALS,
)
from sec_filing_intelligence.discovery import (
    _build_discovery_flags,
    _classify_keyword_section,
    _classify_sector,
    _clean_company_name,
    _compute_deltas,
    _compute_diamond_score,
    _enrich_flags,
    _ensure_phase2_columns,
    _format_delta_report,
    _format_scan_report,
    _parse_discovery_hits,
    _score_flags,
    _store_discovery_flags,
    _store_history_snapshot,
    _store_moat_signals,
    _store_text_search_hits,
    _write_csv,
    get_latest_results,
    run_discovery,
    search_layer_2c,
)


def test_discovery_tables_exist_after_migration(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for tbl in (TABLE_DISCOVERY_FLAGS, TABLE_TEXT_SEARCH_HITS, TABLE_MOAT_SIGNALS):
            assert tbl in tables, f"{tbl} missing after migration"


def test_discovery_flags_columns(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_DISCOVERY_FLAGS})")}
        expected = {
            "ticker", "scan_date", "layer_2a", "layer_2b", "layer_2c",
            "flag_count", "keywords_matched", "moat_types", "composite_score",
        }
        assert expected <= cols, f"Missing columns: {expected - cols}"


def test_text_search_hits_columns(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_TEXT_SEARCH_HITS})")}
        expected = {
            "ticker", "scan_date", "keyword", "keyword_layer",
            "moat_type", "filing_date", "filing_type", "filing_url",
            "cik", "company_name",
        }
        assert expected <= cols, f"Missing columns: {expected - cols}"


def test_moat_signals_columns(tmp_path, monkeypatch):
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    with sqlite3.connect(test_db) as con:
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_MOAT_SIGNALS})")}
        expected = {
            "ticker", "scan_date", "moat_type", "signal_source",
            "signal_strength", "evidence",
        }
        assert expected <= cols, f"Missing columns: {expected - cols}"


# ── Helpers & fixtures for discovery.py tests ────────────────────────────────


def _setup_db(tmp_path, monkeypatch):
    """Create a fresh test DB with all screener tables."""
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(screener_db, "DB_PATH", str(test_db))
    monkeypatch.setattr(screener_db, "_migrated", False)
    screener_db.run_migration()
    return test_db


SAMPLE_EFTS_RESPONSE = {
    "hits": {
        "total": {"value": 2, "relation": "eq"},
        "hits": [
            {
                "_source": {
                    "ciks": ["0001876198"],
                    "display_names": [
                        "Nebius Group N.V.  (NBIS)  (CIK 0001876198)"
                    ],
                    "file_date": "2026-03-15",
                    "root_forms": ["10-K"],
                    "form": "10-K",
                    "adsh": "0001876198-26-000012",
                    "file_description": "Annual report",
                    "sics": ["7372"],
                }
            },
            {
                "_source": {
                    "ciks": ["0001855631"],
                    "display_names": [
                        "Amprius Technologies Inc  (AMPX)  (CIK 0001855631)"
                    ],
                    "file_date": "2026-02-28",
                    "root_forms": ["10-K"],
                    "form": "10-K",
                    "adsh": "0001855631-26-000008",
                    "file_description": "Annual report for fiscal year",
                    "sics": ["3674"],
                }
            },
        ],
    }
}


# ── Discovery parser tests ──────────────────────────────────────────────────


def test_parse_efts_hits_extracts_tickers():
    """Parser extracts NBIS and AMPX from sample EFTS response."""
    hits = _parse_discovery_hits(SAMPLE_EFTS_RESPONSE, "sole source", "2a")
    tickers = {h["ticker"] for h in hits}
    assert "NBIS" in tickers, "NBIS not extracted"
    assert "AMPX" in tickers, "AMPX not extracted"
    assert len(hits) == 2


def test_parse_efts_hits_assigns_moat_types():
    """'sole source' keyword maps to supply_chain,regulatory from config."""
    hits = _parse_discovery_hits(SAMPLE_EFTS_RESPONSE, "sole source", "2a")
    assert len(hits) > 0
    # sole source maps to ["supply_chain", "regulatory"] in config
    for h in hits:
        assert "supply_chain" in h["moat_types"]
        assert "regulatory" in h["moat_types"]


def test_parse_efts_hits_assigns_layer():
    """Layer '2b' is preserved in output and relevance_score is 0.7."""
    hits = _parse_discovery_hits(SAMPLE_EFTS_RESPONSE, "proprietary technology", "2b")
    assert len(hits) > 0
    for h in hits:
        assert h["keyword_layer"] == "2b"
        assert h["relevance_score"] == 0.7


# ── Storage tests ────────────────────────────────────────────────────────────


def test_store_text_search_hits(tmp_path, monkeypatch):
    """Inserting a hit stores it in scr_text_search_hits with correct fields."""
    _setup_db(tmp_path, monkeypatch)

    hits = _parse_discovery_hits(SAMPLE_EFTS_RESPONSE, "sole source", "2a")
    inserted = _store_text_search_hits(hits, "2026-04-17")
    assert inserted == 2

    with screener_db.get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker, keyword, keyword_layer, moat_type, relevance_score "
            "FROM scr_text_search_hits ORDER BY ticker"
        ).fetchall()
    assert len(rows) == 2
    ampx_row = [r for r in rows if r["ticker"] == "AMPX"][0]
    assert ampx_row["keyword"] == "sole source"
    assert ampx_row["keyword_layer"] == "2a"
    assert "supply_chain" in ampx_row["moat_type"]
    assert ampx_row["relevance_score"] == 1.0


def test_store_text_search_hits_deduplicates(tmp_path, monkeypatch):
    """Inserting the same hit twice returns 0 on the second call."""
    _setup_db(tmp_path, monkeypatch)

    hits = _parse_discovery_hits(SAMPLE_EFTS_RESPONSE, "sole source", "2a")
    first = _store_text_search_hits(hits, "2026-04-17")
    assert first == 2

    second = _store_text_search_hits(hits, "2026-04-17")
    assert second == 0


# ── Layer 2C tests ─────────────────────────────────────────────────────────


def _insert_universe_ticker(conn, ticker, company_name="Test Co", is_active=1, is_killed=0):
    """Helper: insert a ticker into scr_universe."""
    conn.execute(
        "INSERT OR REPLACE INTO scr_universe (ticker, company_name, is_active, is_killed) "
        "VALUES (?, ?, ?, ?)",
        (ticker, company_name, is_active, is_killed),
    )


def test_layer_2c_analyst_signal(tmp_path, monkeypatch):
    """Ticker with 1 analyst in scr_analyst_coverage appears as under_followed."""
    _setup_db(tmp_path, monkeypatch)

    with screener_db.get_connection() as conn:
        _insert_universe_ticker(conn, "TINY", "Tiny Corp")
        conn.execute(
            "INSERT INTO scr_analyst_coverage (ticker, date, analyst_count) "
            "VALUES ('TINY', '2026-04-10', 1)"
        )
        conn.commit()

    results = search_layer_2c("2026-04-17")
    under_followed = [r for r in results if r["ticker"] == "TINY" and r["signal_type"] == "under_followed"]
    assert len(under_followed) == 1
    assert "analyst_count=1" in under_followed[0]["evidence"]


def test_layer_2c_insider_cluster_signal(tmp_path, monkeypatch):
    """Ticker with 3 distinct open-market insider purchases appears as insider_cluster."""
    _setup_db(tmp_path, monkeypatch)
    scan_date = "2026-04-17"
    recent_date = (date(2026, 4, 17) - timedelta(days=30)).isoformat()

    with screener_db.get_connection() as conn:
        _insert_universe_ticker(conn, "CLUS", "Cluster Inc")
        for i, name in enumerate(["Alice CEO", "Bob CFO", "Carol CTO"]):
            conn.execute(
                "INSERT INTO scr_insider_transactions "
                "(ticker, filing_date, insider_name, insider_title, transaction_type, "
                " shares, price_per_share, total_value, is_open_market, transaction_date) "
                "VALUES (?, ?, ?, ?, 'purchase', 5000, 25.0, 125000, 1, ?)",
                ("CLUS", recent_date, name, "Officer", recent_date),
            )
        conn.commit()

    results = search_layer_2c(scan_date)
    cluster = [r for r in results if r["ticker"] == "CLUS" and r["signal_type"] == "insider_cluster"]
    assert len(cluster) == 1
    assert "distinct_buyers=3" in cluster[0]["evidence"]


def test_layer_2c_excludes_killed(tmp_path, monkeypatch):
    """Killed ticker does NOT appear in 2C results even with qualifying data."""
    _setup_db(tmp_path, monkeypatch)

    with screener_db.get_connection() as conn:
        _insert_universe_ticker(conn, "DEAD", "Dead Corp", is_active=1, is_killed=1)
        conn.execute(
            "INSERT INTO scr_analyst_coverage (ticker, date, analyst_count) "
            "VALUES ('DEAD', '2026-04-10', 1)"
        )
        conn.commit()

    results = search_layer_2c("2026-04-17")
    dead_results = [r for r in results if r["ticker"] == "DEAD"]
    assert len(dead_results) == 0, "Killed ticker should be excluded from 2C results"


# ── Union + scoring tests ──────────────────────────────────────────────────


def test_build_discovery_flags_unions_layers():
    """Flags correctly union tickers from 2A, 2B, and 2C layers."""
    text_hits = [
        # ALPHA in both 2A and 2B
        {"ticker": "ALPHA", "keyword": "sole source", "keyword_layer": "2a",
         "moat_types": "supply_chain,regulatory", "company_name": "Alpha Inc", "cik": "000111"},
        {"ticker": "ALPHA", "keyword": "proprietary technology", "keyword_layer": "2b",
         "moat_types": "technology", "company_name": "Alpha Inc", "cik": "000111"},
        # BETA only in 2B
        {"ticker": "BETA", "keyword": "network effect", "keyword_layer": "2b",
         "moat_types": "network", "company_name": "Beta LLC", "cik": "000222"},
    ]
    structural = [
        # ALPHA also in 2C
        {"ticker": "ALPHA", "signal_type": "under_followed", "evidence": "analyst_count=1",
         "company_name": "Alpha Inc"},
        # GAMMA only in 2C
        {"ticker": "GAMMA", "signal_type": "patent_concentration", "evidence": "patents=15",
         "company_name": "Gamma Corp"},
    ]

    flags = _build_discovery_flags(text_hits, structural, "2026-04-17")

    # ALPHA: all 3 layers
    assert flags["ALPHA"]["layer_2a"] == 1
    assert flags["ALPHA"]["layer_2b"] == 1
    assert flags["ALPHA"]["layer_2c"] == 1
    assert flags["ALPHA"]["flag_count"] == 3

    # BETA: only 2B
    assert flags["BETA"]["layer_2a"] == 0
    assert flags["BETA"]["layer_2b"] == 1
    assert flags["BETA"]["layer_2c"] == 0
    assert flags["BETA"]["flag_count"] == 1

    # GAMMA: only 2C
    assert flags["GAMMA"]["layer_2a"] == 0
    assert flags["GAMMA"]["layer_2b"] == 0
    assert flags["GAMMA"]["layer_2c"] == 1
    assert flags["GAMMA"]["flag_count"] == 1


def test_score_discovery_flags():
    """Ticker with 3 flags + 3 moat types scores higher than 1 flag + 1 moat type."""
    text_hits = [
        {"ticker": "HIGH", "keyword": "sole source", "keyword_layer": "2a",
         "moat_types": "supply_chain,regulatory", "company_name": "High Co", "cik": ""},
        {"ticker": "HIGH", "keyword": "proprietary technology", "keyword_layer": "2b",
         "moat_types": "technology", "company_name": "High Co", "cik": ""},
        {"ticker": "LOW", "keyword": "network effect", "keyword_layer": "2b",
         "moat_types": "network", "company_name": "Low Co", "cik": ""},
    ]
    structural = [
        {"ticker": "HIGH", "signal_type": "under_followed", "evidence": "a=1",
         "company_name": "High Co"},
    ]

    flags = _build_discovery_flags(text_hits, structural, "2026-04-17")
    scored = _score_flags(flags)

    assert scored["HIGH"]["composite_score"] > scored["LOW"]["composite_score"], (
        f"HIGH ({scored['HIGH']['composite_score']}) should outscore "
        f"LOW ({scored['LOW']['composite_score']})"
    )


def test_store_discovery_flags(tmp_path, monkeypatch):
    """Stored flags are retrievable with correct JSON-encoded fields."""
    _setup_db(tmp_path, monkeypatch)

    flags = {
        "TEST": {
            "layer_2a": 1, "layer_2b": 0, "layer_2c": 1,
            "flag_count": 2,
            "keywords_matched": ["sole source"],
            "structural_signals": ["under_followed"],
            "moat_types": ["regulatory", "supply_chain"],
            "moat_type_count": 2,
            "composite_score": 12.5,
            "company_name": "Test Corp",
            "cik": "000999",
        }
    }

    written = _store_discovery_flags(flags, "2026-04-17")
    assert written == 1

    with screener_db.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM scr_discovery_flags WHERE ticker='TEST' AND scan_date='2026-04-17'"
        ).fetchone()

    assert row is not None
    assert row["layer_2a"] == 1
    assert row["layer_2c"] == 1
    assert row["flag_count"] == 2
    assert row["composite_score"] == 12.5
    assert json.loads(row["keywords_matched"]) == ["sole source"]
    assert json.loads(row["structural_signals"]) == ["under_followed"]
    assert json.loads(row["moat_types"]) == ["regulatory", "supply_chain"]


# ── Task 5/6: scan report, CSV, orchestrator, query helper tests ────────


def test_format_scan_report():
    """scan report includes both tickers, moat labels, and scan date."""
    flags = {
        "NBIS": {
            "layer_2a": 1, "layer_2b": 1, "layer_2c": 1, "flag_count": 3,
            "keywords_matched": ["sole source", "proprietary technology"],
            "structural_signals": ["under_followed"],
            "moat_types": ["regulatory", "supply_chain", "technology"],
            "moat_type_count": 3, "composite_score": 15.5,
            "company_name": "Nebius Group", "cik": "0001876198",
        },
        "AMPX": {
            "layer_2a": 0, "layer_2b": 1, "layer_2c": 0, "flag_count": 1,
            "keywords_matched": ["proprietary technology"],
            "structural_signals": [],
            "moat_types": ["technology"],
            "moat_type_count": 1, "composite_score": 5.0,
            "company_name": "Amprius Technologies", "cik": "0001855631",
        },
    }
    report = _format_scan_report(flags, "2026-04-17", top_n=50)
    assert "2026-04-17" in report
    assert "NBIS" in report
    assert "AMPX" in report
    # NBIS has regulatory -> "Reg" label
    assert "Reg" in report
    assert "Tech" in report
    # NBIS should be first (higher score)
    nbis_pos = report.index("NBIS")
    ampx_pos = report.index("AMPX")
    assert nbis_pos < ampx_pos, "NBIS (score 15.5) should appear before AMPX (score 5.0)"


def test_write_csv_output(tmp_path, monkeypatch):
    """CSV file is created with correct fields and row count."""
    monkeypatch.setattr(discovery_mod, "DISCOVERY_OUTPUT_DIR", tmp_path)

    flags = {
        "NBIS": {
            "layer_2a": 1, "layer_2b": 1, "layer_2c": 0, "flag_count": 2,
            "keywords_matched": ["sole source"],
            "structural_signals": [],
            "moat_types": ["regulatory", "supply_chain"],
            "moat_type_count": 2, "composite_score": 10.0,
            "company_name": "Nebius Group", "cik": "001",
            "sector": "technology", "market_cap": 250_000_000,
            "analyst_count": 2, "thesis_match_count": 1,
        },
        "TEST": {
            "layer_2a": 0, "layer_2b": 1, "layer_2c": 1, "flag_count": 2,
            "keywords_matched": ["proprietary technology"],
            "structural_signals": ["under_followed"],
            "moat_types": ["technology"],
            "moat_type_count": 1, "composite_score": 7.5,
            "company_name": "Test Corp", "cik": "002",
            "sector": "unknown", "market_cap": None,
            "analyst_count": None, "thesis_match_count": 0,
        },
    }
    csv_path = _write_csv(flags, "2026-04-17")

    assert csv_path.exists()
    assert csv_path.name == "2026-04-17.csv"

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2
    # First row should be NBIS (higher score)
    assert rows[0]["ticker"] == "NBIS"
    assert rows[0]["rank"] == "1"
    assert "sole source" in rows[0]["keywords_matched"]
    assert rows[0]["sector"] == "technology"
    assert rows[0]["market_cap"] == "250000000"
    # Second row
    assert rows[1]["ticker"] == "TEST"
    assert rows[1]["rank"] == "2"
    assert rows[1]["sector"] == "unknown"


def test_run_discovery_dry_run(tmp_path, monkeypatch):
    """Dry run produces flagged tickers and a CSV without sending report."""
    _setup_db(tmp_path, monkeypatch)
    monkeypatch.setattr(discovery_mod, "DISCOVERY_OUTPUT_DIR", tmp_path)

    # Monkeypatch _efts_search to return canned data
    monkeypatch.setattr(discovery_mod, "_efts_search", lambda *a, **kw: SAMPLE_EFTS_RESPONSE)
    # Monkeypatch _load_cik_maps to no-op
    monkeypatch.setattr(discovery_mod, "_load_cik_maps", lambda: None)
    # Narrow keywords to limit iterations
    monkeypatch.setattr(discovery_mod, "DISCOVERY_HARD_KEYWORDS", ["sole source"])
    monkeypatch.setattr(discovery_mod, "DISCOVERY_SOFT_KEYWORDS", ["proprietary technology"])

    summary = run_discovery(dry_run=True)

    assert summary["total_flagged"] > 0
    assert Path(summary["csv_path"]).exists()
    assert summary["scan_date"] == date.today().isoformat()


def test_get_latest_results(tmp_path, monkeypatch):
    """get_latest_results returns data from scr_discovery_flags."""
    _setup_db(tmp_path, monkeypatch)

    # Insert a flag row directly
    with screener_db.get_connection() as conn:
        conn.execute(
            """INSERT INTO scr_discovery_flags
               (ticker, scan_date, company_name, cik,
                layer_2a, layer_2b, layer_2c, flag_count,
                keywords_matched, structural_signals,
                moat_types, moat_type_count, composite_score, rank)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("ZETA", "2026-04-17", "Zeta Inc", "999",
             1, 0, 1, 2,
             '["sole source"]', '["under_followed"]',
             '["regulatory"]', 1, 8.5, 1),
        )
        conn.commit()

    results = get_latest_results(top_n=10)
    assert len(results) >= 1
    assert results[0]["ticker"] == "ZETA"
    assert results[0]["composite_score"] == 8.5


# ── Phase 2: Sector classification tests ──────────────────────────────────


def test_classify_sector_defense():
    """SIC 3761 (guided missiles) maps to 'defense'."""
    assert _classify_sector("3761") == "defense"


def test_classify_sector_pharma():
    """SIC 2834 (pharmaceutical preparations) maps to 'pharma'."""
    assert _classify_sector("2834") == "pharma"


def test_classify_sector_unknown():
    """SIC 9999 (no matching range) maps to 'unknown'."""
    assert _classify_sector("9999") == "unknown"


def test_classify_sector_empty():
    """Empty string maps to 'unknown'."""
    assert _classify_sector("") == "unknown"


def test_score_with_sector_bonus():
    """Defense ticker scores higher than pharma ticker with same keywords."""
    # Build identical flags for two tickers, only sector differs
    flags = {
        "DEF": {
            "layer_2a": 1, "layer_2b": 0, "layer_2c": 0,
            "flag_count": 1,
            "keywords_matched": ["sole source"],
            "structural_signals": [],
            "moat_types": ["supply_chain", "regulatory"],
            "moat_type_count": 2,
            "company_name": "Defense Corp", "cik": "",
            "sector": "defense",
            "market_cap": None,
            "analyst_count": None,
            "thesis_match_count": 0,
        },
        "PHR": {
            "layer_2a": 1, "layer_2b": 0, "layer_2c": 0,
            "flag_count": 1,
            "keywords_matched": ["sole source"],
            "structural_signals": [],
            "moat_types": ["supply_chain", "regulatory"],
            "moat_type_count": 2,
            "company_name": "Pharma Corp", "cik": "",
            "sector": "pharma",
            "market_cap": None,
            "analyst_count": None,
            "thesis_match_count": 0,
        },
    }
    scored = _score_flags(flags)
    # defense gets +5.0, pharma gets -3.0, so DEF should outscore PHR by 8.0
    assert scored["DEF"]["composite_score"] > scored["PHR"]["composite_score"], (
        f"DEF ({scored['DEF']['composite_score']}) should outscore "
        f"PHR ({scored['PHR']['composite_score']})"
    )
    diff = scored["DEF"]["composite_score"] - scored["PHR"]["composite_score"]
    assert abs(diff - 8.0) < 0.01, f"Score difference should be 8.0, got {diff}"


def test_enrich_flags_adds_sector(tmp_path, monkeypatch):
    """After enrichment, flags have sector field populated from SIC data in DB."""
    _setup_db(tmp_path, monkeypatch)
    _ensure_phase2_columns()

    # Insert a text search hit with SIC code for a defense ticker
    with screener_db.get_connection() as conn:
        conn.execute(
            """INSERT INTO scr_text_search_hits
               (ticker, scan_date, keyword, keyword_layer, moat_type,
                filing_date, filing_type, filing_url, cik, company_name,
                relevance_score, sic)
               VALUES ('OPXS', '2026-04-17', 'sole source', '2a',
                       'supply_chain', '2026-03-01', '10-K', '', '999',
                       'Optex Systems', 1.0, '3812')""",
        )
        conn.commit()

    flags = {
        "OPXS": {
            "layer_2a": 1, "layer_2b": 0, "layer_2c": 0,
            "flag_count": 1,
            "keywords_matched": ["sole source"],
            "structural_signals": [],
            "moat_types": ["supply_chain"],
            "moat_type_count": 1,
            "company_name": "Optex Systems", "cik": "999",
        }
    }
    enriched = _enrich_flags(flags)
    # SIC 3812 matches defense range (3812, 3812)
    assert enriched["OPXS"]["sector"] == "defense"
    assert "market_cap" in enriched["OPXS"]
    assert "analyst_count" in enriched["OPXS"]
    assert "thesis_match_count" in enriched["OPXS"]


# ── Phase 3: Fundamentals scoring tests ──────────────────────────────────


def _make_base_flag(**overrides) -> dict:
    """Build a minimal flag dict with overrides for Phase 3 scoring tests."""
    base = {
        "layer_2a": 1, "layer_2b": 0, "layer_2c": 0,
        "flag_count": 1,
        "keywords_matched": ["sole source"],
        "structural_signals": [],
        "moat_types": ["supply_chain", "regulatory"],
        "moat_type_count": 2,
        "company_name": "Test Corp", "cik": "",
        "sector": "unknown",
        "market_cap": None,
        "analyst_count": None,
        "thesis_match_count": 0,
        "revenue": None,
        "revenue_growth": None,
        "debt_to_equity": None,
        "cash_runway": None,
        "pct_from_52w_high": None,
    }
    base.update(overrides)
    return base


def test_score_already_discovered_penalty():
    """Ticker with cap=$15B, analysts=12, pct_from_high=-5% gets penalty.
    Ticker with cap=$15B, analysts=12, pct_from_high=-20% does NOT (not near high)."""
    flags = {
        "NEAR_HIGH": _make_base_flag(
            market_cap=15_000_000_000,
            analyst_count=12,
            pct_from_52w_high=-5.0,
        ),
        "FAR_FROM_HIGH": _make_base_flag(
            market_cap=15_000_000_000,
            analyst_count=12,
            pct_from_52w_high=-20.0,
        ),
    }
    scored = _score_flags(flags)
    # NEAR_HIGH should have the -5.0 penalty applied; FAR_FROM_HIGH should not
    assert scored["NEAR_HIGH"]["composite_score"] < scored["FAR_FROM_HIGH"]["composite_score"], (
        f"NEAR_HIGH ({scored['NEAR_HIGH']['composite_score']}) should score lower than "
        f"FAR_FROM_HIGH ({scored['FAR_FROM_HIGH']['composite_score']}) due to already-discovered penalty"
    )
    diff = scored["FAR_FROM_HIGH"]["composite_score"] - scored["NEAR_HIGH"]["composite_score"]
    assert abs(diff - 5.0) < 0.01, f"Penalty difference should be 5.0, got {diff}"


def test_score_unfunded_debt_penalty():
    """D/E=10, runway=2 gets penalty. D/E=10, runway=8 does NOT (has cash)."""
    flags = {
        "HIGH_DEBT_LOW_RUNWAY": _make_base_flag(
            debt_to_equity=10.0,
            cash_runway=2.0,
        ),
        "HIGH_DEBT_OK_RUNWAY": _make_base_flag(
            debt_to_equity=10.0,
            cash_runway=8.0,
        ),
    }
    scored = _score_flags(flags)
    assert scored["HIGH_DEBT_LOW_RUNWAY"]["composite_score"] < scored["HIGH_DEBT_OK_RUNWAY"]["composite_score"], (
        f"HIGH_DEBT_LOW_RUNWAY ({scored['HIGH_DEBT_LOW_RUNWAY']['composite_score']}) should score lower than "
        f"HIGH_DEBT_OK_RUNWAY ({scored['HIGH_DEBT_OK_RUNWAY']['composite_score']}) due to unfunded debt penalty"
    )
    diff = scored["HIGH_DEBT_OK_RUNWAY"]["composite_score"] - scored["HIGH_DEBT_LOW_RUNWAY"]["composite_score"]
    assert abs(diff - 4.0) < 0.01, f"Penalty difference should be 4.0, got {diff}"


def test_score_debt_free_bonus():
    """D/E=0.1 gets +2.0 bonus."""
    flags = {
        "DEBT_FREE": _make_base_flag(debt_to_equity=0.1),
        "BASELINE": _make_base_flag(),  # debt_to_equity=None, no bonus
    }
    scored = _score_flags(flags)
    diff = scored["DEBT_FREE"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 2.0) < 0.01, f"Debt-free bonus should be 2.0, got {diff}"


def test_score_revenue_decline_penalty():
    """revenue_growth=-0.15 gets -3.0 penalty."""
    flags = {
        "DECLINING": _make_base_flag(revenue_growth=-0.15),
        "BASELINE": _make_base_flag(),  # revenue_growth=None, no penalty
    }
    scored = _score_flags(flags)
    diff = scored["BASELINE"]["composite_score"] - scored["DECLINING"]["composite_score"]
    assert abs(diff - 3.0) < 0.01, f"Revenue decline penalty should be 3.0, got {diff}"


def test_score_zero_analyst_bonus():
    """analyst_count=0 gets +3.0 bonus."""
    flags = {
        "ZERO_ANALYST": _make_base_flag(analyst_count=0),
        "BASELINE": _make_base_flag(),  # analyst_count=None, no bonus
    }
    scored = _score_flags(flags)
    diff = scored["ZERO_ANALYST"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 3.0) < 0.01, f"Zero-analyst bonus should be 3.0, got {diff}"


def test_score_low_analyst_bonus():
    """analyst_count=2 gets +1.5 bonus."""
    flags = {
        "LOW_ANALYST": _make_base_flag(analyst_count=2),
        "BASELINE": _make_base_flag(),  # analyst_count=None, no bonus
    }
    scored = _score_flags(flags)
    diff = scored["LOW_ANALYST"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 1.5) < 0.01, f"Low-analyst bonus should be 1.5, got {diff}"


# ── Phase 4: USAspending contract validation tests ───────────────────────────


def test_clean_company_name():
    """_clean_company_name strips suffixes and trims long names."""
    assert _clean_company_name("Coda Octopus Group, Inc.") == "Coda Octopus"
    assert _clean_company_name("AMPX Technologies Holdings, Inc./DE") == "AMPX Technologies"
    assert _clean_company_name("Nebius Group N.V.") == "Nebius"
    assert _clean_company_name("Lockheed Martin Corp.") == "Lockheed Martin"
    assert _clean_company_name("Tesla") == "Tesla"
    assert _clean_company_name("") == ""
    # > 4 words gets trimmed to first 3
    assert _clean_company_name("One Two Three Four Five") == "One Two Three"


def test_score_gov_sole_source_bonus():
    """Flag with gov_sole_source=True gets +5.0 bonus."""
    flags = {
        "SOLE_SRC": _make_base_flag(gov_sole_source=True, gov_contracts=2),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["SOLE_SRC"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 5.0) < 0.01, f"Sole source bonus should be 5.0, got {diff}"


def test_score_gov_single_bidder_bonus():
    """Flag with gov_single_bidder=True (no sole_source) gets +3.0 bonus."""
    flags = {
        "SINGLE_BID": _make_base_flag(gov_single_bidder=True, gov_contracts=1),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["SINGLE_BID"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 3.0) < 0.01, f"Single bidder bonus should be 3.0, got {diff}"


def test_score_gov_contract_bonus():
    """Flag with gov_contracts=3 (no sole_source, no single_bidder) gets +1.0 bonus."""
    flags = {
        "HAS_GOV": _make_base_flag(gov_contracts=3),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["HAS_GOV"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 1.0) < 0.01, f"Gov contract bonus should be 1.0, got {diff}"


def test_score_gov_sole_source_takes_priority():
    """When both sole_source and single_bidder are True, only sole_source bonus applies (elif)."""
    flags = {
        "BOTH": _make_base_flag(gov_sole_source=True, gov_single_bidder=True, gov_contracts=5),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["BOTH"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 5.0) < 0.01, f"Sole source should take priority, got diff={diff}"


# ── Phase 4b: Customer 10-K cross-validation scoring tests ──────────────────


def test_score_customer_mention_bonus():
    """customer_mentions=1 gets +4.0 bonus (DISCOVERY_CUSTOMER_XVAL_BONUS)."""
    flags = {
        "MENTIONED": _make_base_flag(customer_mentions=1),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["MENTIONED"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 4.0) < 0.01, f"Customer mention bonus should be 4.0, got {diff}"


def test_score_customer_multi_mention_bonus():
    """customer_mentions=3 gets +6.0 bonus (DISCOVERY_CUSTOMER_XVAL_MULTI_BONUS)."""
    flags = {
        "MULTI_MENTIONED": _make_base_flag(customer_mentions=3),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["MULTI_MENTIONED"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff - 6.0) < 0.01, f"Customer multi-mention bonus should be 6.0, got {diff}"


def test_score_no_customer_mention():
    """customer_mentions=0 gets no bonus."""
    flags = {
        "NO_MENTIONS": _make_base_flag(customer_mentions=0),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["NO_MENTIONS"]["composite_score"] - scored["BASELINE"]["composite_score"]
    assert abs(diff) < 0.01, f"No customer mentions should give no bonus, got diff={diff}"


# ── Phase 5: Historical tracking tests ─────────────────────────────────────


def test_store_history_snapshot(tmp_path, monkeypatch):
    """Store flags into scr_discovery_history and verify rows are persisted."""
    _setup_db(tmp_path, monkeypatch)

    flags = {
        "OPXS": {
            "composite_score": 25.5,
            "flag_count": 3,
            "moat_type_count": 2,
            "sector": "defense",
            "gov_sole_source": True,
            "customer_mentions": 4,
        },
        "NBIS": {
            "composite_score": 18.0,
            "flag_count": 2,
            "moat_type_count": 3,
            "sector": "technology",
            "gov_sole_source": False,
            "customer_mentions": 0,
        },
    }

    written = _store_history_snapshot(flags, "2026-04-17")
    assert written == 2

    with screener_db.get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM {TABLE_DISCOVERY_HISTORY} ORDER BY ticker"
        ).fetchall()

    assert len(rows) == 2
    nbis_row = [r for r in rows if r["ticker"] == "NBIS"][0]
    assert nbis_row["composite_score"] == 18.0
    assert nbis_row["flag_count"] == 2
    assert nbis_row["moat_type_count"] == 3
    assert nbis_row["sector"] == "technology"
    assert nbis_row["gov_sole_source"] == 0
    assert nbis_row["customer_mentions"] == 0

    opxs_row = [r for r in rows if r["ticker"] == "OPXS"][0]
    assert opxs_row["composite_score"] == 25.5
    assert opxs_row["gov_sole_source"] == 1
    assert opxs_row["customer_mentions"] == 4


def test_compute_deltas_first_scan(tmp_path, monkeypatch):
    """No previous data: all tickers are new, deltas empty, prev_scan_date is None."""
    _setup_db(tmp_path, monkeypatch)

    flags = {
        "AAA": _make_base_flag(composite_score=10.0),
        "BBB": _make_base_flag(composite_score=20.0),
    }
    # Score them so composite_score is set via the flag dict (already set via override)

    deltas = _compute_deltas(flags, "2026-04-17")

    assert deltas["prev_scan_date"] is None
    assert set(deltas["new_tickers"]) == {"AAA", "BBB"}
    assert deltas["biggest_gainers"] == []
    assert deltas["biggest_losers"] == []
    assert deltas["dropped_out"] == []
    # All tickers should be marked as new
    assert flags["AAA"]["is_new_ticker"] is True
    assert flags["BBB"]["is_new_ticker"] is True


def test_compute_deltas_with_previous(tmp_path, monkeypatch):
    """With previous scan data, correctly computes deltas, new tickers, and dropped tickers."""
    _setup_db(tmp_path, monkeypatch)

    # Insert previous scan data into scr_discovery_history
    with screener_db.get_connection() as conn:
        for ticker, score, sector in [
            ("AAA", 15.0, "defense"),
            ("BBB", 20.0, "technology"),
            ("CCC", 12.0, "energy"),  # will drop out
        ]:
            conn.execute(
                f"""INSERT INTO {TABLE_DISCOVERY_HISTORY}
                   (ticker, scan_date, composite_score, flag_count,
                    moat_type_count, sector, gov_sole_source, customer_mentions)
                   VALUES (?, '2026-04-10', ?, 2, 1, ?, 0, 0)""",
                (ticker, score, sector),
            )
        conn.commit()

    # Current scan: AAA gained, BBB lost, CCC dropped, DDD is new
    flags = {
        "AAA": _make_base_flag(composite_score=20.0),  # +5.0
        "BBB": _make_base_flag(composite_score=15.0),  # -5.0
        "DDD": _make_base_flag(composite_score=10.0),  # new
    }

    deltas = _compute_deltas(flags, "2026-04-17")

    assert deltas["prev_scan_date"] == "2026-04-10"

    # DDD is new
    assert "DDD" in deltas["new_tickers"]
    assert flags["DDD"]["is_new_ticker"] is True

    # AAA gained +5.0 (>= threshold of 3.0)
    gainers = {g["ticker"]: g for g in deltas["biggest_gainers"]}
    assert "AAA" in gainers
    assert abs(gainers["AAA"]["delta"] - 5.0) < 0.01
    assert flags["AAA"]["score_delta"] == 5.0
    assert flags["AAA"]["is_new_ticker"] is False

    # BBB lost -5.0 (<= -threshold of -3.0)
    losers = {l["ticker"]: l for l in deltas["biggest_losers"]}
    assert "BBB" in losers
    assert abs(losers["BBB"]["delta"] - (-5.0)) < 0.01
    assert flags["BBB"]["score_delta"] == -5.0

    # CCC dropped out (was in top 200 but not in current flags)
    dropped = {d["ticker"]: d for d in deltas["dropped_out"]}
    assert "CCC" in dropped
    assert dropped["CCC"]["last_score"] == 12.0


def test_score_new_ticker_bonus():
    """Ticker with is_new_ticker=True gets +DISCOVERY_HISTORY_NEW_TICKER_BONUS."""
    flags = {
        "NEW_TICK": _make_base_flag(is_new_ticker=True),
        "OLD_TICK": _make_base_flag(is_new_ticker=False),
    }
    scored = _score_flags(flags)
    diff = scored["NEW_TICK"]["composite_score"] - scored["OLD_TICK"]["composite_score"]
    assert abs(diff - DISCOVERY_HISTORY_NEW_TICKER_BONUS) < 0.01, (
        f"New ticker bonus should be {DISCOVERY_HISTORY_NEW_TICKER_BONUS}, got diff={diff}"
    )

# ── Phase 5b: 10-K context analysis tests ─────────────────────────────────────


def test_classify_keyword_section_business():
    """Text with 'Item 1. Business' header before 'sole source' classifies as 'business'."""
    text = (
        "Table of Contents "
        "Item 1. Business Description "
        "We are a sole source provider of advanced materials for aerospace applications. "
        "Item 1A. Risk Factors "
        "We face various risks in our operations."
    )
    result = _classify_keyword_section(text, "sole source")
    assert result == "business", f"Expected 'business', got '{result}'"


def test_classify_keyword_section_risk_factors():
    """Text with 'Item 1A. Risk Factors' header before 'sole source' classifies as 'risk_factors'."""
    text = (
        "Table of Contents "
        "Item 1. Business Description "
        "We are a technology company. "
        "Item 1A. Risk Factors "
        "We depend on a sole source supplier for key raw materials. "
        "Item 2. Properties "
        "Our facilities are located in California."
    )
    result = _classify_keyword_section(text, "sole source")
    assert result == "risk_factors", f"Expected 'risk_factors', got '{result}'"


def test_classify_keyword_section_unknown():
    """Text without any Item headers classifies as 'unknown'."""
    text = "This is a document about sole source suppliers without section headers."
    result = _classify_keyword_section(text, "sole source")
    assert result == "unknown", f"Expected 'unknown', got '{result}'"


def test_score_self_claim_bonus():
    """self_claim_count=2 gets +4.0 bonus (2 * DISCOVERY_CONTEXT_SELF_CLAIM_BONUS)."""
    flags = {
        "SELF_CLAIM": _make_base_flag(self_claim_count=2),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["SELF_CLAIM"]["composite_score"] - scored["BASELINE"]["composite_score"]
    expected = 2 * DISCOVERY_CONTEXT_SELF_CLAIM_BONUS
    assert abs(diff - expected) < 0.01, f"Self-claim bonus should be {expected}, got {diff}"


def test_score_risk_factor_only_penalty():
    """risk_factor_count=1 with self_claim_count=0 gets -1.0 penalty."""
    flags = {
        "RISK_ONLY": _make_base_flag(risk_factor_count=1, self_claim_count=0),
        "BASELINE": _make_base_flag(),
    }
    scored = _score_flags(flags)
    diff = scored["RISK_ONLY"]["composite_score"] - scored["BASELINE"]["composite_score"]
    expected = DISCOVERY_CONTEXT_RISK_FACTOR_PENALTY
    assert abs(diff - expected) < 0.01, f"Risk-factor-only penalty should be {expected}, got {diff}"


# ── Diamond Score tests ──────────────────────────────────────────────────────


def test_diamond_score_ampx_pattern():
    """Ticker with -85% crash, 0 analysts, 150% growth, zero debt, 4 moat types + gov contract -> diamond_score >= 22/25."""
    flags = {
        "AMPX_LIKE": _make_base_flag(
            pct_from_52w_high=-85.0,
            analyst_count=0,
            revenue_growth=1.50,
            debt_to_equity=0.0,
            moat_type_count=4,
            gov_sole_source=True,
            gov_contracts=2,
            keywords_matched=["sole source", "only provider", "exclusive license", "critical component"],
            moat_types=["regulatory", "supply_chain", "technology", "government"],
        ),
    }
    result = _compute_diamond_score(flags)
    ds = result["AMPX_LIKE"]["diamond_score"]
    assert ds >= 22, f"AMPX-like pattern should score >= 22/25, got {ds}"
    # Verify sub-dimensions
    assert result["AMPX_LIKE"]["diamond_crash"] == 4   # -85% => 4
    assert result["AMPX_LIKE"]["diamond_undiscovered"] == 5  # 0 analysts => 5
    assert result["AMPX_LIKE"]["diamond_growth"] == 5   # 150% => 5
    assert result["AMPX_LIKE"]["diamond_balance"] == 5  # D/E=0 => 5
    assert result["AMPX_LIKE"]["diamond_moat"] >= 3     # 4 moats + hard kw + gov


def test_diamond_score_low_quality():
    """Ticker with -10% from high, 15 analysts, declining revenue, high debt, 1 moat type -> diamond_score <= 5/25."""
    flags = {
        "JUNK": _make_base_flag(
            pct_from_52w_high=-10.0,
            analyst_count=15,
            revenue_growth=-0.05,
            debt_to_equity=12.0,
            moat_type_count=1,
            moat_types=["technology"],
            keywords_matched=["proprietary technology"],
        ),
    }
    result = _compute_diamond_score(flags)
    ds = result["JUNK"]["diamond_score"]
    assert ds <= 5, f"Low-quality ticker should score <= 5/25, got {ds}"
    assert result["JUNK"]["diamond_crash"] == 0        # -10% => 0
    assert result["JUNK"]["diamond_undiscovered"] == 0  # 15 analysts => 0
    assert result["JUNK"]["diamond_growth"] == 0        # negative => 0
    assert result["JUNK"]["diamond_balance"] == 0       # D/E=12 => 0


def test_diamond_score_stored_in_flags():
    """After _compute_diamond_score, each flag has diamond_score and all sub-dimension fields."""
    flags = {
        "AAA": _make_base_flag(
            pct_from_52w_high=-50.0,
            analyst_count=3,
            revenue_growth=0.25,
            debt_to_equity=1.0,
            moat_type_count=2,
            moat_types=["regulatory", "technology"],
            keywords_matched=["proprietary technology"],
        ),
        "BBB": _make_base_flag(),
    }
    result = _compute_diamond_score(flags)

    for ticker in ("AAA", "BBB"):
        assert "diamond_score" in result[ticker], f"{ticker} missing diamond_score"
        assert "diamond_crash" in result[ticker], f"{ticker} missing diamond_crash"
        assert "diamond_undiscovered" in result[ticker], f"{ticker} missing diamond_undiscovered"
        assert "diamond_growth" in result[ticker], f"{ticker} missing diamond_growth"
        assert "diamond_balance" in result[ticker], f"{ticker} missing diamond_balance"
        assert "diamond_moat" in result[ticker], f"{ticker} missing diamond_moat"
        # Total is sum of sub-dimensions
        total = (
            result[ticker]["diamond_crash"]
            + result[ticker]["diamond_undiscovered"]
            + result[ticker]["diamond_growth"]
            + result[ticker]["diamond_balance"]
            + result[ticker]["diamond_moat"]
        )
        assert result[ticker]["diamond_score"] == total, (
            f"{ticker}: diamond_score ({result[ticker]['diamond_score']}) != sum of dims ({total})"
        )
