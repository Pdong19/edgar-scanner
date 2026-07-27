"""Regression tests for cross-module seam bugs (2026-07 review).

Each test pins a fix for a bug that lived in the WIRING between modules —
unit conventions (percent vs fraction, dollars vs millions), dict keys, and
threshold semantics — the places per-module tests never looked.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sec_filing_intelligence import db as db_mod
from sec_filing_intelligence import deep_dive, forward_moat
from sec_filing_intelligence.ampx_rules import score_insider_buying, score_row
from sec_filing_intelligence.db import get_connection


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "seam_test.db")
    monkeypatch.setattr(db_mod, "_migrated", False)
    db_mod.run_migration()
    return tmp_path


class TestExpectedValueUsesMoatScore:
    """deep_dive read moat['total'] — a key _score_moat never produced — so the
    moat score never influenced expected value."""

    DATA = {"revenue_growth": 0, "debt_to_equity": None, "pct_from_52w_high": -75}
    ANALOGS = {"best_analog_score": 0}
    WASHOUT = {"phase": 3}

    def test_moat_strength_moves_probabilities(self):
        strong = deep_dive._estimate_expected_value(
            self.DATA, {"moat_score": 25}, self.ANALOGS, self.WASHOUT)
        weak = deep_dive._estimate_expected_value(
            self.DATA, {"moat_score": 0}, self.ANALOGS, self.WASHOUT)
        assert strong["p_bull"] > weak["p_bull"]
        assert strong["p_bear"] < weak["p_bear"]
        assert strong["expected_value"] > weak["expected_value"]


class TestCapexSignalUnitConversion:
    """scr_price_metrics stores pct_from_ath as PERCENT (-85.0); the scoring
    contract is a FRACTION (-0.85). The wrapper must convert."""

    def test_percent_input_reaches_scorer_as_fraction(self, monkeypatch):
        yf_mock = MagicMock()
        yf_mock.Ticker.return_value.cash_flow = None
        yf_mock.Ticker.return_value.income_stmt = None
        monkeypatch.setitem(sys.modules, "yfinance", yf_mock)
        with patch.object(forward_moat, "score_capex_inflection",
                          return_value=(0, {})) as spy:
            forward_moat._fetch_capex_signal("TEST", -85.0)
        assert spy.call_args.args[4] == pytest.approx(-0.85)

    def test_none_pct_stays_none(self, monkeypatch):
        yf_mock = MagicMock()
        yf_mock.Ticker.return_value.cash_flow = None
        yf_mock.Ticker.return_value.income_stmt = None
        monkeypatch.setitem(sys.modules, "yfinance", yf_mock)
        with patch.object(forward_moat, "score_capex_inflection",
                          return_value=(0, {})) as spy:
            forward_moat._fetch_capex_signal("TEST", None)
        assert spy.call_args.args[4] is None

    def test_barely_off_ath_scores_zero_end_to_end(self):
        # A stock 0.6% below ATH must NOT clear the "60% crash" gate.
        # (Pre-fix, -0.6 percent passed the -0.60 fraction comparison.)
        score, _ = forward_moat.score_capex_inflection(200.0, 100.0, None, None, -0.006)
        assert score == 0


class TestInsiderClusterScoring:
    """cluster_count counts window EVENTS (each already 2+ distinct insiders);
    the README-advertised '2+ insiders within 30 days' must score full credit."""

    def test_single_two_insider_cluster_scores_full(self):
        assert score_insider_buying({"buy_count": 2, "cluster_count": 1}) == 1.0

    def test_solo_buyer_still_half(self):
        assert score_insider_buying({"buy_count": 1, "cluster_count": 0}) == 0.5


class TestRevGrowthMissingTtm:
    """A source that doesn't supply revenue_ttm (shared-fundamentals path
    hardcodes None) must not zero the growth dimension; a genuinely tiny
    revenue base still must."""

    @staticmethod
    def _row(revenue_ttm):
        return {
            "ticker": "SEAM", "company_name": "Seam Co", "industry": "Semiconductors",
            "pct_from_52w_high": -85, "pct_from_ath": -90,
            "revenue_growth_yoy": 1.5, "revenue_growth_qoq": None,
            "revenue_ttm": revenue_ttm, "debt_to_equity": 0.05,
            "cash_runway_quarters": 10, "shares_outstanding": 50_000_000,
            "institutional_ownership": 0.10, "float_shares": 40_000_000,
            "short_interest_pct": 20, "market_cap": 100_000_000,
            "current_price": 2.0, "analyst_count": 0,
        }

    def test_none_ttm_keeps_growth_score(self):
        result = score_row(self._row(None), "none", {"buy_count": 0, "cluster_count": 0})
        assert result["dim2_revgrowth"] == 2.0

    def test_tiny_ttm_still_zeroed(self):
        result = score_row(self._row(500_000), "none", {"buy_count": 0, "cluster_count": 0})
        assert result["dim2_revgrowth"] == 0.0


class TestConvergenceRewardDirection:
    """combined_score sorts ascending; the strong-moat 'reward' must rank a
    converging strong-moat ticker BETTER, not worse."""

    def test_strong_moat_wins_at_equal_ranks(self, tmp_path):
        moat_rows = [
            {"ticker": "STRONG", "composite_score": 25.0, "rank": 1},
            {"ticker": "WEAK", "composite_score": 10.0, "rank": 1},
        ]
        forward_rows = [
            {"ticker": "STRONG", "forward_score": 10.0, "rank": 1},
            {"ticker": "WEAK", "forward_score": 10.0, "rank": 1},
        ]
        out = tmp_path / "combined.csv"
        result = forward_moat.merge_combined_csv(moat_rows, forward_rows, output_path=out)
        import csv
        with open(result) as f:
            ranks = {r["ticker"]: int(r["combined_rank"]) for r in csv.DictReader(f)}
        assert ranks["STRONG"] < ranks["WEAK"]


class TestDryRunComposesWithRun:
    def test_run_dry_run_passes_flag_through(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["forward_moat", "--run", "--dry-run"])
        with patch.object(forward_moat, "run_forward_scan",
                          return_value={"total_scored": 0, "csv_path": None}) as spy:
            forward_moat.main()
        assert spy.call_args.kwargs["dry_run"] is True


class TestForm4TransactionDate:
    """<transactionDate> is a container (<value> child); _get_text saw only the
    container's whitespace text, so every transaction silently carried the
    FILING date — collapsing dedup keys and breaking amendment matching. The
    original fixture test masked it by passing filing_date == XML date."""

    FIXTURES = Path(__file__).parent / "fixtures" / "form4"

    def test_transaction_date_read_from_value_child(self):
        import re
        import xml.etree.ElementTree as ET

        from sec_filing_intelligence.form4_parser import parse_form4_xml

        xml_text = (self.FIXTURES / "single_purchase.xml").read_text()
        xml_date = re.search(
            r"<transactionDate>\s*<value>([\d-]+)</value>", xml_text).group(1)
        root = ET.fromstring(xml_text)
        # Filing date deliberately different from the XML transaction date
        txns = parse_form4_xml(root, filing_date="2099-12-31", accession="acc-1")
        assert txns[0]["transaction_date"] == xml_date
        assert txns[0]["transaction_date"] != "2099-12-31"

    def test_missing_date_still_falls_back_to_filing_date(self):
        import re
        import xml.etree.ElementTree as ET

        from sec_filing_intelligence.form4_parser import parse_form4_xml

        xml_text = (self.FIXTURES / "single_purchase.xml").read_text()
        stripped = re.sub(r"<transactionDate>.*?</transactionDate>", "",
                          xml_text, flags=re.DOTALL)
        txns = parse_form4_xml(ET.fromstring(stripped),
                               filing_date="2099-12-31", accession="acc-1")
        assert txns[0]["transaction_date"] == "2099-12-31"


class TestInsiderStoreColumns:
    """store_transactions named two columns the DDL never creates
    (shares_owned_after/sec_url vs ownership_after/source_url) — every insert
    failed inside the except and 'Stored 0' was reported as success."""

    def test_store_persists_against_production_ddl(self, fresh_db):
        from sec_filing_intelligence.insider_tracker import store_transactions

        txn = {
            "ticker": "SEAM", "filing_date": "2026-04-15",
            "transaction_date": "2026-04-01", "insider_name": "Doe Jane",
            "insider_title": "CEO", "transaction_type": "purchase",
            "shares": 1000, "price_per_share": 5.0, "total_value": 5000.0,
            "shares_owned_after": 5000, "is_open_market": 1,
            "sec_url": "https://www.sec.gov/x", "accession_number": "acc-1",
        }
        assert store_transactions([txn]) == 1
        with get_connection() as conn:
            row = conn.execute(
                "SELECT ownership_after, source_url FROM scr_insider_transactions "
                "WHERE ticker='SEAM'"
            ).fetchone()
        assert row["ownership_after"] == 5000
        assert row["source_url"] == "https://www.sec.gov/x"


class TestDocumentHrefPicker:
    """The first .htm href on a real EDGAR index page is the masthead
    /index.htm site-nav link — following it 404'd every Phase 5b fetch."""

    INDEX_HTML = """
    <a href="/index.htm">EDGAR home</a>
    <a href="/cgi-bin/browse-edgar?action=getcompany&CIK=1&type=10-K&index.htm">nav</a>
    <a href="/ix?doc=/Archives/edgar/data/1/000000000126000001/co-20251231.htm">iXBRL</a>
    <a href="co-20251231.htm">10-K</a>
    <a href="form10k-index.htm">index</a>
    <a href="R1.xml">xbrl</a>
    """

    def test_masthead_skipped_ixbrl_unwrapped(self):
        from sec_filing_intelligence.discovery import _pick_document_href

        href = _pick_document_href(self.INDEX_HTML)
        assert href == "/Archives/edgar/data/1/000000000126000001/co-20251231.htm"

    def test_relative_document_chosen_without_ixbrl(self):
        from sec_filing_intelligence.discovery import _pick_document_href

        html = self.INDEX_HTML.replace(
            '<a href="/ix?doc=/Archives/edgar/data/1/000000000126000001/co-20251231.htm">iXBRL</a>', "")
        assert _pick_document_href(html) == "co-20251231.htm"

    def test_nav_only_page_yields_none(self):
        from sec_filing_intelligence.discovery import _pick_document_href

        assert _pick_document_href('<a href="/index.htm">home</a>') is None


class TestLayer2CGrowthUnits:
    """revenue_growth_yoy is stored as a fraction; the percent threshold made
    'small_growing' demand >3000% YoY growth."""

    def test_forty_five_percent_growth_fires(self, fresh_db):
        from sec_filing_intelligence.discovery import search_layer_2c

        with get_connection() as conn:
            conn.execute(
                "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES ('GROW', 1, 0)")
            conn.execute(
                "INSERT INTO scr_universe (ticker, is_active, is_killed) VALUES ('SLOW', 1, 0)")
            conn.execute(
                """INSERT INTO scr_fundamentals (ticker, date, revenue_ttm, revenue_growth_yoy)
                   VALUES ('GROW', '2026-07-01', 50000000, 0.45)""")
            conn.execute(
                """INSERT INTO scr_fundamentals (ticker, date, revenue_ttm, revenue_growth_yoy)
                   VALUES ('SLOW', '2026-07-01', 50000000, 0.10)""")
            conn.commit()
        signals = search_layer_2c("2026-07-27")
        small_growing = {s["ticker"] for s in signals if s["signal_type"] == "small_growing"}
        assert "GROW" in small_growing
        assert "SLOW" not in small_growing
        evidence = next(s for s in signals
                        if s["ticker"] == "GROW" and s["signal_type"] == "small_growing")["evidence"]
        assert "45.0%" in evidence


class TestHistoryStoresIntrinsicScore:
    """Persisting the +2.0 new-ticker bonus into history inflated next week's
    delta by exactly the bonus for every ex-new ticker."""

    def test_bonus_stripped_from_snapshot(self, fresh_db):
        from sec_filing_intelligence.discovery import _store_history_snapshot

        flags = {
            "NEWT": {"composite_score": 12.0, "is_new_ticker": True},
            "OLDT": {"composite_score": 12.0, "is_new_ticker": False},
        }
        _store_history_snapshot(flags, "2026-07-27")
        with get_connection() as conn:
            scores = dict(conn.execute(
                "SELECT ticker, composite_score FROM scr_discovery_history"
            ).fetchall())
        assert scores["OLDT"] == 12.0
        assert scores["NEWT"] == 10.0  # 12.0 − DISCOVERY_HISTORY_NEW_TICKER_BONUS (2.0)


class TestSharedRateLimiter:
    """Each decorated function previously carried its own full budget — six
    SEC call sites × the policy ceiling. The bucket budget must be shared."""

    def test_two_functions_share_one_budget(self, monkeypatch):
        from sec_filing_intelligence import utils

        sleeps = []
        monkeypatch.setattr(utils.time, "monotonic", lambda: 100.0)
        monkeypatch.setattr(utils.time, "sleep", sleeps.append)

        @utils.rate_limiter(10, bucket="test-shared-budget")
        def f():
            return "f"

        @utils.rate_limiter(10, bucket="test-shared-budget")
        def g():
            return "g"

        f()  # takes the first slot — no sleep
        g()  # same bucket: must wait one interval even though g never ran before
        assert sleeps == [pytest.approx(0.1)]


class TestDeepDiveMarketCapUnits:
    """scr_discovery_flags.market_cap arrives in raw dollars; deep_dive works
    in millions. A $900M company must not be treated as $900,000,000M."""

    def test_db_market_cap_normalized_to_millions(self, fresh_db):
        with get_connection() as conn:
            # market_cap is a Phase-2 column added by discovery's own migration
            conn.execute("ALTER TABLE scr_discovery_flags ADD COLUMN market_cap REAL")
            conn.execute(
                """INSERT INTO scr_discovery_flags
                   (ticker, scan_date, composite_score, moat_types, keywords_matched,
                    flag_count, moat_type_count, market_cap)
                   VALUES ('SEAM', '2026-07-01', 10.0, 'technology', 'sole source',
                           1, 1, 900000000.0)"""
            )
            conn.commit()
        with patch.object(deep_dive, "_yfinance_enrich", lambda _ticker, data: data):
            data = deep_dive._collect_ticker_data("SEAM")
        assert data["market_cap"] == pytest.approx(900.0)
