"""Tests for scripts/screener/form4_rss_poller.py — atom poll, universe filter, amendment logic."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "form4"


@pytest.fixture
def mem_db(monkeypatch):
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")

    import sec_filing_intelligence.db as db_mod

    @contextmanager
    def mock_conn():
        yield con

    monkeypatch.setattr(db_mod, "get_connection", mock_conn)
    monkeypatch.setattr(db_mod, "_migrated", False, raising=False)
    from sec_filing_intelligence.db import run_migration
    run_migration()

    # Redirect form4_parser's _get_connection too
    from sec_filing_intelligence import form4_parser
    monkeypatch.setattr(form4_parser, "_get_connection", mock_conn)
    monkeypatch.setattr(form4_parser, "_sec_cache_loaded", True)  # skip real SEC fetch

    yield con
    con.close()


def test_atom_feed_returns_100_recent_filings(monkeypatch, mem_db):
    """fetch_latest_form4_atom returns parsed list of (cik, accession, filing_date, is_amend)."""
    from sec_filing_intelligence import form4_rss_poller

    atom_bytes = (FIXTURES / "atom_feed_sample.xml").read_bytes()
    fake_resp = MagicMock()
    fake_resp.content = atom_bytes
    fake_resp.text = atom_bytes.decode()
    fake_resp.status_code = 200

    def fake_get(url, *, accept_xml=False):
        return fake_resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    filings = form4_rss_poller.fetch_latest_form4_atom()

    assert len(filings) == 5
    # Expected entries from the fixture (ordering preserved)
    ciks = [f["cik"] for f in filings]
    assert ciks == [
        "0000320193", "0000320193", "0000789019", "0009999999", "0001234567",
    ]
    accessions = [f["accession_number"] for f in filings]
    assert accessions[0] == "0000320193-25-000042"
    # Second entry is the 4/A
    assert filings[1]["is_amendment"] is True
    assert filings[0]["is_amendment"] is False


def test_poll_filters_filings_not_in_universe(monkeypatch, mem_db):
    """run_poll should skip filings whose CIK isn't in scr_universe."""
    from sec_filing_intelligence import form4_rss_poller, form4_parser

    # Seed scr_universe with AAPL (0000320193) + MSFT (0000789019), nothing else
    mem_db.execute(
        "INSERT INTO scr_universe (ticker, cik, is_active, is_killed) "
        "VALUES ('AAPL', '0000320193', 1, 0)"
    )
    mem_db.execute(
        "INSERT INTO scr_universe (ticker, cik, is_active, is_killed) "
        "VALUES ('MSFT', '0000789019', 1, 0)"
    )
    mem_db.commit()

    # Prime the ticker cache (avoid the SEC HTTP call in the test)
    form4_parser._cik_to_ticker_cache.update({
        "0000320193": "AAPL", "0000789019": "MSFT",
    })

    atom_bytes = (FIXTURES / "atom_feed_sample.xml").read_bytes()
    xml_template = (FIXTURES / "single_purchase.xml").read_text()

    # Vary shares per accession so the legacy inline UNIQUE constraint
    # (ticker,filing_date,insider_name,transaction_type,shares) doesn't fire
    # across the three in-universe filings (they all parse from the same
    # template). With the narrowed I1 catch, a legacy collision is a hard
    # error — so we sidestep it at the fixture level rather than hide it.
    share_map = {
        "0000320193-25-000042": 1000,  # AAPL original
        "0000320193-25-000041": 1001,  # AAPL 4/A amendment
        "0000789019-25-000100": 2000,  # MSFT
        "0009999999-25-000001": 3000,  # off-universe
        "0001234567-25-000050": 4000,  # off-universe
    }

    def _xml_for_url(url: str) -> bytes:
        for acc, shares in share_map.items():
            if acc.replace("-", "") in url:
                return xml_template.replace(
                    "<transactionShares><value>1000</value></transactionShares>",
                    f"<transactionShares><value>{shares}</value></transactionShares>",
                ).encode()
        return xml_template.encode()

    def fake_get(url, *, accept_xml=False):
        resp = MagicMock()
        if "getcurrent" in url:
            resp.content = atom_bytes
        elif "Archives/edgar/data" in url and url.endswith("/"):
            # filing-directory listing → return HTML with a fake .xml href
            resp.text = '<a href="primary.xml">primary.xml</a>'
            resp.content = resp.text.encode()
        else:
            resp.content = _xml_for_url(url)
        resp.status_code = 200
        return resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    result = form4_rss_poller.run_poll()

    assert result["filings_fetched"] == 5
    # AAPL×2 + MSFT×1 in universe; 2 off-universe CIKs skipped
    assert result["filings_in_universe"] == 3
    assert result["filings_skipped_no_cik"] == 2
    # Regression guard: mock directory listings must resolve cleanly — if the
    # branch-condition above drifts, the poller's _find_form4_xml_url fallback
    # will error and this will catch it.
    assert result["errors_n"] == 0


def test_poll_logs_row_per_run_with_correct_counts(monkeypatch, mem_db):
    """run_poll writes exactly one row to scr_form4_poll_log per invocation."""
    from sec_filing_intelligence import form4_rss_poller, form4_parser

    mem_db.execute(
        "INSERT INTO scr_universe (ticker, cik, is_active, is_killed) "
        "VALUES ('AAPL', '0000320193', 1, 0)"
    )
    mem_db.commit()
    form4_parser._cik_to_ticker_cache.update({"0000320193": "AAPL"})

    atom_bytes = (FIXTURES / "atom_feed_sample.xml").read_bytes()
    xml_bytes = (FIXTURES / "single_purchase.xml").read_bytes()

    def fake_get(url, *, accept_xml=False):
        resp = MagicMock()
        if "getcurrent" in url:
            resp.content = atom_bytes
        elif "Archives/edgar/data" in url and url.endswith("/"):
            resp.text = '<a href="primary.xml">primary.xml</a>'
            resp.content = resp.text.encode()
        else:
            resp.content = xml_bytes
        resp.status_code = 200
        return resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    form4_rss_poller.run_poll()

    log_rows = mem_db.execute(
        "SELECT source, filings_fetched, filings_in_universe FROM scr_form4_poll_log"
    ).fetchall()
    assert len(log_rows) == 1
    assert log_rows[0]["source"] == "atom-poll"
    assert log_rows[0]["filings_fetched"] == 5
    assert log_rows[0]["filings_in_universe"] == 2


def test_legacy_5tuple_collision_inserts_cleanly_post_migration(mem_db):
    """The legacy UNIQUE(ticker,filing_date,insider_name,transaction_type,shares) is
    dropped by run_migration() (see test_migration_drops_legacy_unique_on_rebuild).
    The intended dedup is idx_insider_dedup on (ticker, accession_number, insider_name,
    transaction_date). A Form 4/A amendment with unchanged shares and a distinct
    accession must therefore insert cleanly — this was the silent-amendment-drop bug."""
    from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

    # Insert a row manually via the legacy 5-tuple shape (no accession)
    mem_db.execute(
        """INSERT INTO scr_insider_transactions
            (ticker, filing_date, insider_name, transaction_type, shares, is_amended)
           VALUES ('AAPL', '2025-03-15', 'Cook Timothy D', 'purchase', 1000, 0)"""
    )
    mem_db.commit()

    # Upsert with identical 5-tuple but distinct accession. Post-migration,
    # this must succeed (legacy UNIQUE is gone; dedup is on accession instead).
    txn = {
        "ticker": "AAPL", "cik": "0000320193",
        "filing_date": "2025-03-15", "transaction_date": "2025-03-15",
        "insider_name": "Cook Timothy D", "insider_title": "CEO",
        "transaction_type": "purchase", "shares": 1000, "price_per_share": 175.75,
        "total_value": 175750.0, "shares_owned_after": 500000,
        "is_open_market": 1, "accession_number": "0000320193-25-000099",
    }
    _upsert_with_reconciliation(mem_db, txn)

    rows = mem_db.execute(
        "SELECT accession_number FROM scr_insider_transactions "
        "WHERE ticker='AAPL' ORDER BY id"
    ).fetchall()
    assert len(rows) == 2, "Both the legacy row and the amendment should coexist"
    assert rows[1]["accession_number"] == "0000320193-25-000099"


def test_upsert_idempotent_on_replay_same_batch(monkeypatch, mem_db):
    """Running run_poll twice on the same atom feed must NOT create duplicate rows."""
    from sec_filing_intelligence import form4_rss_poller, form4_parser

    mem_db.execute(
        "INSERT INTO scr_universe (ticker, cik, is_active, is_killed) "
        "VALUES ('AAPL', '0000320193', 1, 0)"
    )
    mem_db.commit()
    form4_parser._cik_to_ticker_cache.update({"0000320193": "AAPL"})

    atom_bytes = (FIXTURES / "atom_feed_sample.xml").read_bytes()
    xml_bytes = (FIXTURES / "single_purchase.xml").read_bytes()

    def fake_get(url, *, accept_xml=False):
        resp = MagicMock()
        if "getcurrent" in url:
            resp.content = atom_bytes
        elif "Archives/edgar/data" in url and url.endswith("/"):
            resp.text = '<a href="primary.xml">primary.xml</a>'
            resp.content = resp.text.encode()
        else:
            resp.content = xml_bytes
        resp.status_code = 200
        return resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    form4_rss_poller.run_poll()
    count_after_first = mem_db.execute(
        "SELECT COUNT(*) FROM scr_insider_transactions"
    ).fetchone()[0]

    form4_rss_poller.run_poll()
    count_after_second = mem_db.execute(
        "SELECT COUNT(*) FROM scr_insider_transactions"
    ).fetchone()[0]

    assert count_after_first == count_after_second, \
        f"Second run inserted {count_after_second - count_after_first} duplicate rows"


def test_amendment_marks_original_is_amended_and_inserts_new_row(mem_db):
    """In-order case: insert original, then insert amendment; original flips is_amended=1."""
    from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

    original = {
        "ticker": "AAPL", "cik": "0000320193",
        "filing_date": "2025-03-15", "transaction_date": "2025-03-15",
        "insider_name": "Cook Timothy D", "insider_title": "Chief Executive Officer",
        "transaction_type": "purchase", "shares": 1000, "price_per_share": 175.50,
        "total_value": 175500.0, "shares_owned_after": 500000,
        "is_open_market": 1, "accession_number": "0000320193-25-000001",
    }
    # Amendment has distinct shares (1001 vs 1000) to avoid the legacy UNIQUE collision
    # on (ticker, filing_date, insider_name, transaction_type, shares). In production a
    # real amendment often changes price (not shares), and we have a migration item
    # queued to drop the legacy UNIQUE. This test isolates amendment-flag behaviour
    # from the unrelated legacy-constraint issue.
    amendment = {**original, "price_per_share": 175.75, "shares": 1001,
                 "total_value": 175_775.75,
                 "accession_number": "0000320193-25-000002",
                 "filing_date": "2025-03-20"}

    with mem_db:
        _upsert_with_reconciliation(mem_db, original)
        _upsert_with_reconciliation(mem_db, amendment)

    rows = mem_db.execute(
        "SELECT accession_number, is_amended, amendment_accession "
        "FROM scr_insider_transactions WHERE ticker='AAPL' "
        "ORDER BY filing_date"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["accession_number"] == "0000320193-25-000001"
    assert rows[0]["is_amended"] == 1
    assert rows[0]["amendment_accession"] == "0000320193-25-000002"
    assert rows[1]["accession_number"] == "0000320193-25-000002"
    assert rows[1]["is_amended"] == 0


def test_amendment_out_of_order_reconciled_by_later_insertion(mem_db):
    """Out-of-order: insert amendment first, then original. Same end state as in-order."""
    from sec_filing_intelligence.form4_rss_poller import _upsert_with_reconciliation

    original = {
        "ticker": "AAPL", "cik": "0000320193",
        "filing_date": "2025-03-15", "transaction_date": "2025-03-15",
        "insider_name": "Cook Timothy D", "insider_title": "Chief Executive Officer",
        "transaction_type": "purchase", "shares": 1000, "price_per_share": 175.50,
        "total_value": 175500.0, "shares_owned_after": 500000,
        "is_open_market": 1, "accession_number": "0000320193-25-000001",
    }
    amendment = {**original, "price_per_share": 175.75, "shares": 1001,
                 "total_value": 175_775.75,
                 "accession_number": "0000320193-25-000002",
                 "filing_date": "2025-03-20"}

    with mem_db:
        _upsert_with_reconciliation(mem_db, amendment)  # amendment first
        _upsert_with_reconciliation(mem_db, original)   # original second

    rows = mem_db.execute(
        "SELECT accession_number, is_amended, amendment_accession "
        "FROM scr_insider_transactions WHERE ticker='AAPL' "
        "ORDER BY filing_date"
    ).fetchall()
    assert len(rows) == 2
    canonical = [r for r in rows if r["is_amended"] == 0]
    superseded = [r for r in rows if r["is_amended"] == 1]
    assert len(canonical) == 1
    assert len(superseded) == 1
    assert canonical[0]["accession_number"] == "0000320193-25-000002"
    assert superseded[0]["accession_number"] == "0000320193-25-000001"
    assert superseded[0]["amendment_accession"] == "0000320193-25-000002"


def test_429_triggers_backoff_and_retry(monkeypatch, mem_db):
    """_sec_get should retry on HTTP 429 and eventually succeed."""
    import requests
    from sec_filing_intelligence import form4_rss_poller

    attempts = {"n": 0}

    def flaky_get(url, headers=None, timeout=None):
        attempts["n"] += 1
        resp = MagicMock()
        if attempts["n"] < 3:
            resp.status_code = 429
        else:
            resp.status_code = 200
            resp.content = b"<feed/>"
            resp.text = "<feed/>"
        return resp

    # form4_rss_poller._sec_get imports requests inline, so patching requests.get
    # at module level is sufficient — no wrapper or namespace tricks needed.
    monkeypatch.setattr(requests, "get", flaky_get)
    monkeypatch.setattr("time.sleep", lambda s: None)  # skip backoff delays

    resp = form4_rss_poller._sec_get(form4_rss_poller.ATOM_URL, accept_xml=True)
    assert resp.status_code == 200
    assert attempts["n"] == 3


def test_malformed_atom_increments_errors_and_populates_notes(monkeypatch, mem_db):
    from sec_filing_intelligence import form4_rss_poller

    def fake_get(url, *, accept_xml=False):
        resp = MagicMock()
        resp.content = b"<html><body>EDGAR is down</body></html>"
        resp.text = resp.content.decode()
        resp.status_code = 200
        return resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    result = form4_rss_poller.run_poll()

    assert result["errors_n"] == 1

    log_row = mem_db.execute(
        "SELECT notes FROM scr_form4_poll_log ORDER BY poll_ts DESC LIMIT 1"
    ).fetchone()
    assert "atom fetch failed" in log_row["notes"] or "html" in log_row["notes"].lower()


def test_three_consecutive_zero_filings_market_hours_triggers_alert(monkeypatch, mem_db):
    """Three consecutive market-hour polls with 0 filings → check_stalled_feed returns True."""
    from sec_filing_intelligence import form4_rss_poller

    now = "2025-04-15T20:00:00+00:00"  # US market hours (13-21 UTC per check_stalled_feed)
    rows = [
        (now, "atom-poll", 0, 0, 0, 0, 0, 0, 0, "ok"),
        ("2025-04-15T19:00:00+00:00", "atom-poll", 0, 0, 0, 0, 0, 0, 0, "ok"),
        ("2025-04-15T18:00:00+00:00", "atom-poll", 0, 0, 0, 0, 0, 0, 0, "ok"),
    ]
    mem_db.executemany(
        """INSERT INTO scr_form4_poll_log
             (poll_ts, source, filings_fetched, filings_in_universe,
              filings_skipped_no_cik, new_transactions_inserted,
              amendments_resolved, errors_n, duration_s, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    mem_db.commit()

    assert form4_rss_poller.check_stalled_feed() is True

    # Single-row case — not stalled
    mem_db.execute("DELETE FROM scr_form4_poll_log")
    mem_db.execute(
        """INSERT INTO scr_form4_poll_log
             (poll_ts, source, filings_fetched, filings_in_universe,
              filings_skipped_no_cik, new_transactions_inserted,
              amendments_resolved, errors_n, duration_s, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (now, "atom-poll", 0, 0, 0, 0, 0, 0, 0, "ok"),
    )
    mem_db.commit()

    assert form4_rss_poller.check_stalled_feed() is False


def test_atom_feed_handles_archives_path_url_and_legacy_query_string(monkeypatch, mem_db):
    """Regression: EDGAR 2025+ atom feed uses /Archives/edgar/data/{cik}/ URLs,
    not legacy ?CIK={cik}. The regex must handle both for robustness.

    Discovered 2026-04-15 when T12's dry-run against live EDGAR returned 0 filings
    because the old query-string-only regex didn't match."""
    from sec_filing_intelligence import form4_rss_poller

    # Atom feed with MIXED URL formats — one entry path-based, one query-string-based
    mixed_atom = b'''<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>4 - APPLE INC (0000320193) (Issuer)</title>
    <link rel="alternate" type="text/html"
      href="https://www.sec.gov/Archives/edgar/data/320193/000032019325000042/0000320193-25-000042-index.htm"/>
    <updated>2025-04-15T13:55:00-04:00</updated>
    <category term="4"/>
    <id>urn:tag:sec.gov,2008:accession-number=0000320193-25-000042</id>
  </entry>
  <entry>
    <title>4 - MICROSOFT (legacy URL)</title>
    <link rel="alternate" type="text/html"
      href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0000789019&amp;type=4"/>
    <updated>2025-04-15T13:45:00-04:00</updated>
    <category term="4"/>
    <id>urn:tag:sec.gov,2008:accession-number=0000789019-25-000100</id>
  </entry>
</feed>'''

    fake_resp = MagicMock()
    fake_resp.content = mixed_atom
    fake_resp.text = mixed_atom.decode()
    fake_resp.status_code = 200

    def fake_get(url, *, accept_xml=False):
        return fake_resp

    monkeypatch.setattr(form4_rss_poller, "_sec_get", fake_get)

    filings = form4_rss_poller.fetch_latest_form4_atom()

    assert len(filings) == 2, "Both URL formats should parse successfully"
    assert filings[0]["cik"] == "0000320193"
    assert filings[0]["accession_number"] == "0000320193-25-000042"
    assert filings[1]["cik"] == "0000789019"
    assert filings[1]["accession_number"] == "0000789019-25-000100"
