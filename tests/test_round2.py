"""Round-2 regression tests: GC negation, insider quality, XBRL edge cases,
backlog windows, Form 4 line identity (2026-07 review follow-up)."""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from sec_filing_intelligence import db as db_mod
from sec_filing_intelligence import insider_tracker
from sec_filing_intelligence import xbrl_fundamentals as xf
from sec_filing_intelligence.ampx_rules import scan_going_concern_text
from sec_filing_intelligence.db import get_connection


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "round2.db")
    monkeypatch.setattr(db_mod, "_migrated", False)
    db_mod.run_migration()
    return tmp_path


# ── R2-1: going-concern negation ────────────────────────────────────────────


class TestGoingConcernNegation:
    """Healthy 10-Ks carry the ASU 2014-15 evaluation boilerplate; a negated
    evaluation must not take the −0.5..−2.0 penalty."""

    NEGATED = [
        "We evaluated whether there are conditions that raise substantial doubt "
        "about the Company's ability to continue as a going concern and "
        "concluded that no such conditions exist.",
        "Management believes there is no substantial doubt about its ability to "
        "continue as a going concern for at least twelve months.",
        "These factors did not raise substantial doubt regarding the Company's "
        "ability to continue as a going concern.",
    ]

    def test_negated_boilerplate_does_not_flag(self):
        for text in self.NEGATED:
            assert scan_going_concern_text(text) is None, text[:60]

    def test_real_disclosure_still_flags(self):
        text = ("These conditions raise substantial doubt about the Company's "
                "ability to continue as a going concern within one year.")
        assert scan_going_concern_text(text) is not None

    def test_alleviated_doubt_still_flags(self):
        # Doubt raised then alleviated by management's plans is a disclosure.
        text = ("Conditions that raise substantial doubt about our ability to "
                "continue as a going concern existed; management's plans "
                "alleviated that doubt.")
        assert scan_going_concern_text(text) is not None

    def test_negated_boilerplate_plus_real_disclosure_flags(self):
        text = self.NEGATED[0] + " " * 400 + (
            "Recurring losses raise substantial doubt about the ability to "
            "continue as a going concern.")
        assert scan_going_concern_text(text) is not None


class TestDeepDiveRedFlagFreshness:
    """A going-concern flag from an old scan that has since cleared must not
    keep hard-killing the ticker."""

    def test_only_latest_scan_flags_returned(self, fresh_db):
        from sec_filing_intelligence import deep_dive

        with get_connection() as conn:
            conn.execute(
                "INSERT INTO scr_ampx_red_flags VALUES ('OLDGC', '2026-05-01', 'going_concern', 'x')")
            conn.execute(
                "INSERT INTO scr_ampx_red_flags VALUES ('OLDGC', '2026-07-01', 'reverse_split', 'y')")
            conn.commit()
        with patch.object(deep_dive, "_yfinance_enrich", lambda _t, data: data):
            data = deep_dive._collect_ticker_data("OLDGC")
        assert data["red_flags"] == ["reverse_split"]


# ── R2-2: insider netting + titles ──────────────────────────────────────────


class TestInsiderNetting:
    def _seed(self, rows):
        today = date.today().isoformat()
        with get_connection() as conn:
            for name, ttype, value in rows:
                conn.execute(
                    """INSERT INTO scr_insider_transactions
                       (ticker, filing_date, insider_name, insider_title,
                        transaction_type, total_value, is_open_market)
                       VALUES ('NETT', ?, ?, 'Officer', ?, ?, 1)""",
                    (today, name, ttype, value),
                )
            conn.commit()

    def test_token_buy_cannot_mask_distribution(self, fresh_db):
        self._seed([("A", "purchase", 15_000)]
                   + [(f"S{i}", "sale", 400_000) for i in range(5)])
        with patch.object(insider_tracker, "_get_price_context",
                          return_value=(None, None)):
            result = insider_tracker.score_insider_activity("NETT")
        assert result["score"] == 0.0
        assert "Net insider selling" in result["reason"]

    def test_balanced_activity_not_zeroed(self, fresh_db):
        self._seed([("A", "purchase", 300_000), ("S1", "sale", 200_000)])
        with patch.object(insider_tracker, "_get_price_context",
                          return_value=(None, None)):
            result = insider_tracker.score_insider_activity("NETT")
        assert result["score"] > 0.0


class TestTitleClassification:
    def test_vice_president_is_not_c_suite(self):
        for title in ("Vice President, Sales", "Senior Vice President",
                      "Assistant Vice President", "Vice Chairman",
                      "Deputy Chairman"):
            assert insider_tracker._is_c_suite(title) is False, title

    def test_real_c_suite_still_matches(self):
        for title in ("Chief Executive Officer", "President", "Chairman",
                      "CEO", "EVP & Chief Financial Officer",
                      "Vice President & Chief Financial Officer"):
            assert insider_tracker._is_c_suite(title) is True, title

    def test_managing_director_is_not_board_director(self):
        assert insider_tracker._is_director("Managing Director") is False
        assert insider_tracker._is_director("Director") is True


class TestPollerSkipsStoredAccessions:
    def test_no_refetch_for_accession_already_in_db(self, fresh_db):
        from sec_filing_intelligence import form4_rss_poller as poller
        from sec_filing_intelligence import universe

        universe.seed_universe(
            ["RCAT"], cik_map={"RCAT": {"cik": "0001526119", "company_name": "Red Cat"}})
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO scr_insider_transactions
                   (ticker, filing_date, transaction_type, accession_number)
                   VALUES ('RCAT', '2026-07-01', 'purchase', 'ACC-SEEN')""")
            conn.commit()
        feed = [{"cik": "0001526119", "accession_number": "ACC-SEEN", "updated": "2026-07-27T12:00:00Z"}]
        with patch.object(poller, "fetch_latest_form4_atom", return_value=feed), \
             patch.object(poller, "cik_to_ticker", return_value="RCAT"), \
             patch.object(poller, "_find_form4_xml_url") as never_fetch:
            summary = poller.run_poll()
        never_fetch.assert_not_called()
        assert summary["filings_already_stored"] == 1
        assert summary["new_transactions_inserted"] == 0


# ── R2-5: Form 4 line identity + amendment chaining ─────────────────────────


class TestForm4LineIdentity:
    """The v1 dedup key (ticker, accession, insider, transaction_date) collapsed
    every extra transaction line inside one Form 4 — the repo's own multi-line
    fixture stored 1 of 3 transactions, silently dropping 10,300 of 10,800
    shares. Identity is now (accession_number, txn_line)."""

    def _parse_fixture(self):
        import xml.etree.ElementTree as ET
        from pathlib import Path

        from sec_filing_intelligence.form4_parser import parse_form4_xml

        xml = (Path(__file__).parent / "fixtures" / "form4" / "multi_transaction.xml").read_text()
        return parse_form4_xml(ET.fromstring(xml), filing_date="2025-04-01",
                               accession="0000789019-25-000100")

    def test_parser_emits_stable_line_ordinals(self):
        txns = self._parse_fixture()
        assert [t["txn_line"] for t in txns] == [0, 1, 2]

    def test_all_lines_of_multi_transaction_filing_stored(self, fresh_db):
        from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

        txns = self._parse_fixture()
        assert len(txns) == 3
        with get_connection() as conn:
            inserted = sum(
                _upsert_with_reconciliation(conn, {**t, "cik": "0000789019"})[0]
                for t in txns)
            conn.commit()
        assert inserted == 3  # was 1 under the v1 dedup key
        with get_connection() as conn:
            total_shares = conn.execute(
                "SELECT SUM(shares) FROM scr_insider_transactions").fetchone()[0]
        assert total_shares == sum(t["shares"] for t in txns)

    def test_reprocessing_same_filing_is_idempotent(self, fresh_db):
        from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

        txns = self._parse_fixture()
        with get_connection() as conn:
            for _ in range(2):  # poll re-delivers the same feed entry
                for t in txns:
                    _upsert_with_reconciliation(conn, {**t, "cik": "0000789019"})
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM scr_insider_transactions").fetchone()[0]
        assert count == 3

    def test_unrelated_same_day_filings_not_chained_as_amendments(self, fresh_db):
        from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

        base = {
            "ticker": "SEAM", "cik": "0000000001",
            "filing_date": "2026-07-01", "transaction_date": "2026-07-01",
            "insider_name": "Doe Jane", "insider_title": "CEO",
            "transaction_type": "purchase", "shares": 100, "price_per_share": 5.0,
            "total_value": 500.0, "shares_owned_after": 1000,
            "is_open_market": 1, "txn_line": 0,
        }
        with get_connection() as conn:
            # Two independent (non-amendment) filings, same insider, same day
            _upsert_with_reconciliation(conn, {**base, "accession_number": "ACC-1"})
            _upsert_with_reconciliation(conn, {**base, "accession_number": "ACC-2"})
            conn.commit()
            amended = conn.execute(
                "SELECT COUNT(*) FROM scr_insider_transactions WHERE is_amended = 1"
            ).fetchone()[0]
        assert amended == 0  # both stay live — no false amendment chain

    def test_store_transactions_persists_txn_line(self, fresh_db):
        from sec_filing_intelligence.insider_tracker import store_transactions

        txns = [{**t, "sec_url": "https://sec.gov/x"} for t in self._parse_fixture()]
        assert store_transactions(txns) == 3
        with get_connection() as conn:
            lines = [r[0] for r in conn.execute(
                "SELECT txn_line FROM scr_insider_transactions ORDER BY txn_line")]
        assert lines == [0, 1, 2]


# ── R2-3: XBRL prior-year denominator + backoff ─────────────────────────────


class _FakeQuery:
    def __init__(self, frames):
        self._frames = frames
        self._concept = None

    def by_concept(self, c):
        self._concept = c
        return self

    def by_fiscal_period(self, _p):
        return self

    def to_dataframe(self):
        return self._frames.get(self._concept, pd.DataFrame())


class _FakeFacts:
    """Minimal facts stub: only .query() as used by _collect_fy_rows."""

    def __init__(self, frames):
        self._frames = frames

    def query(self):
        return _FakeQuery(self._frames)


def _frame(rows):
    return pd.DataFrame(
        [{"period_end": pe, "numeric_value": v, "fiscal_year": fy, "filing_date": fd}
         for pe, v, fy, fd in rows])


RFC = "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
LEGACY = "us-gaap:Revenues"


class TestPriorYearDenominator:
    def test_same_concept_beats_later_filed_stale_tag(self):
        facts = _FakeFacts({
            RFC: _frame([("2025-12-31", 120.0, 2025, "2026-03-01"),
                         ("2024-12-31", 100.0, 2024, "2025-03-01")]),
            LEGACY: _frame([("2024-12-31", 8.0, 2024, "2025-06-01")]),  # stale, filed LATER
        })
        prior = xf._prior_revenue_yoy(facts, 2025, latest_pe="2025-12-31",
                                      anchor_concept=RFC)
        assert prior == 100.0  # not the stale 8.0 → growth 20%, not +1400%

    def test_cross_concept_fallback_when_anchor_has_no_prior(self):
        facts = _FakeFacts({
            RFC: _frame([("2025-12-31", 120.0, 2025, "2026-03-01")]),
            LEGACY: _frame([("2024-12-31", 95.0, 2024, "2025-03-01")]),
        })
        prior = xf._prior_revenue_yoy(facts, 2025, latest_pe="2025-12-31",
                                      anchor_concept=RFC)
        assert prior == 95.0

    def test_52_53_week_year_matched_by_anniversary(self):
        # FY ends 2025-01-03; prior FY ended 2023-12-29 — plain "2024-" prefix loses it
        facts = _FakeFacts({
            RFC: _frame([("2025-01-03", 120.0, 2025, "2025-03-01"),
                         ("2023-12-29", 90.0, 2023, "2024-03-01")]),
        })
        prior = xf._prior_revenue_yoy(facts, 2025, latest_pe="2025-01-03",
                                      anchor_concept=RFC)
        assert prior == 90.0


class TestOrdinal10KSelection:
    """The old fiscal-year windows overlapped by six months — calendar-year
    filers matched the SAME filing for current and prior, so backlog growth
    was structurally zero for most US issuers."""

    SUBMISSIONS = {
        "filings": {"recent": {
            "form": ["8-K", "10-K", "10-Q", "10-K/A", "10-K"],
            "filingDate": ["2026-05-01", "2026-02-20", "2025-11-01",
                           "2025-04-01", "2025-02-21"],
            "accessionNumber": ["a0", "a1", "a2", "a3", "a4"],
            "primaryDocument": ["d0.htm", "d1.htm", "d2.htm", "d3.htm", "d4.htm"],
        }}
    }

    def _run(self, nth):
        from sec_filing_intelligence import forward_moat as fm

        calls = []

        class _Resp:
            text = "ten-k text"

            def json(self):
                return TestOrdinal10KSelection.SUBMISSIONS

        def fake_get(url, **kw):
            calls.append(url)
            return _Resp()

        with patch.object(fm, "_sec_get", side_effect=fake_get):
            text = fm._fetch_10k_text("TEST", "1234", nth=nth)
        return text, calls

    def test_nth0_and_nth1_pick_distinct_original_10ks(self):
        _, calls0 = self._run(0)
        _, calls1 = self._run(1)
        assert "a1" in calls0[1]  # latest original 10-K, not the 8-K or the /A
        assert "a4" in calls1[1]  # PREVIOUS original 10-K — never the same filing
        assert calls0[1] != calls1[1]

    def test_amendment_used_only_when_originals_exhausted(self):
        subs = {"filings": {"recent": {
            "form": ["10-K/A", "10-K"],
            "filingDate": ["2026-04-01", "2026-02-20"],
            "accessionNumber": ["amend", "orig"],
            "primaryDocument": ["a.htm", "o.htm"],
        }}}
        from sec_filing_intelligence import forward_moat as fm

        calls = []

        class _Resp:
            text = "t"

            def json(self):
                return subs

        with patch.object(fm, "_sec_get", side_effect=lambda url, **kw: (calls.append(url), _Resp())[1]):
            fm._fetch_10k_text("TEST", "1234", nth=0)
            fm._fetch_10k_text("TEST", "1234", nth=1)
        assert "orig" in calls[1]   # nth=0 → the one original
        assert "amend" in calls[3]  # nth=1 → falls back to the amendment


class TestBacklogSourceLabeling:
    HITS = {"backlog": ["...our backlog grew..."]}  # EFTS gate must open

    def test_10k_text_source_labeled(self, monkeypatch):
        from sec_filing_intelligence import filing_scanner, forward_moat as fm

        monkeypatch.setattr(fm, "_load_cik_maps", lambda: None)
        monkeypatch.setitem(filing_scanner._ticker_to_cik, "BKLG", "0000001234")
        texts = {0: "Our backlog of $500.0 million grew.",
                 1: "Our backlog of $250.0 million grew."}
        with patch.object(fm, "_fetch_10k_text",
                          side_effect=lambda t, c, nth=0: texts[nth]):
            score, meta = fm._fetch_backlog_signal(
                "BKLG", "2026-07-27", current_hits=self.HITS)
        assert meta["backlog_source"] == "10k_text"
        assert score > 0

    def test_revenue_proxy_labeled(self, monkeypatch):
        import sys as _sys
        from unittest.mock import MagicMock

        from sec_filing_intelligence import filing_scanner, forward_moat as fm

        monkeypatch.setattr(fm, "_load_cik_maps", lambda: None)
        monkeypatch.setattr(filing_scanner, "_ticker_to_cik", {})  # no CIK → proxy path
        inc = pd.DataFrame([[200e6, 100e6]], index=["Total Revenue"],
                           columns=["2025", "2024"])
        yf_mock = MagicMock()
        yf_mock.Ticker.return_value.income_stmt = inc
        monkeypatch.setitem(_sys.modules, "yfinance", yf_mock)
        score, meta = fm._fetch_backlog_signal(
            "PRXY", "2026-07-27", current_hits=self.HITS)
        assert meta["backlog_source"] == "revenue_proxy"

    def test_no_hits_meta_carries_null_source(self):
        from sec_filing_intelligence import forward_moat as fm

        score, meta = fm._fetch_backlog_signal("NONE", "2026-07-27")
        assert score == 0
        assert meta["backlog_source"] is None


class TestBackoffIgnoresNoCoverage:
    def test_uncovered_tickers_do_not_ratchet_batch_size(self, tmp_path, monkeypatch):
        sleeps = []
        monkeypatch.setattr(xf.time, "sleep", sleeps.append)
        with patch.object(xf, "_extract_with_reason",
                          return_value=(None, "no_coverage")), \
             patch.object(xf, "_write_batch"):
            summary = xf.refresh_xbrl_fundamentals(
                [f"T{i}" for i in range(20)], batch_size=10,
                db_path=str(tmp_path / "x.db"), sleep_per_call=0.0)
        assert summary["failed"] == 20
        # No backoff sleep fired (XBRL_BATCH_BACKOFF_SEC-sized sleeps absent)
        assert all(s < 1 for s in sleeps)

    def test_real_errors_still_ratchet(self, tmp_path, monkeypatch):
        sleeps = []
        monkeypatch.setattr(xf.time, "sleep", sleeps.append)
        with patch.object(xf, "_extract_with_reason",
                          return_value=(None, "error")), \
             patch.object(xf, "_write_batch"):
            xf.refresh_xbrl_fundamentals(
                [f"T{i}" for i in range(20)], batch_size=10,
                db_path=str(tmp_path / "x.db"), sleep_per_call=0.0)
        assert any(s >= 1 for s in sleeps)  # backoff engaged
