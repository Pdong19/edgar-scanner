"""Tests for scripts/screener/form4_parser.py — XML parsing + CIK resolution."""

from pathlib import Path
import xml.etree.ElementTree as ET


FIXTURES = Path(__file__).parent / "fixtures" / "form4"


def _load(name: str) -> ET.Element:
    return ET.fromstring((FIXTURES / name).read_text())


def test_parse_extracts_ticker_and_cik_from_issuer():
    from sec_filing_intelligence.form4_parser import parse_form4_xml

    root = _load("single_purchase.xml")
    txns = parse_form4_xml(root, filing_date="2025-03-15", accession="0000320193-25-000001")

    assert len(txns) == 1
    assert txns[0]["ticker"] == "AAPL"
    assert txns[0]["cik"] == "0000320193"


def test_parse_non_derivative_P_purchase_to_transaction_row():
    from sec_filing_intelligence.form4_parser import parse_form4_xml

    root = _load("single_purchase.xml")
    txns = parse_form4_xml(root, filing_date="2025-03-15", accession="0000320193-25-000001")

    t = txns[0]
    assert t["transaction_type"] == "purchase"
    assert t["transaction_code"] == "P"
    assert t["is_open_market"] == 1
    assert t["shares"] == 1000
    assert t["price_per_share"] == 175.50
    assert t["transaction_date"] == "2025-03-15"
    assert t["insider_name"] == "Cook Timothy D"
    assert t["insider_title"] == "Chief Executive Officer"
    assert t["accession_number"] == "0000320193-25-000001"


def test_parse_filters_out_grant_award_codes_A_G():
    """Award code A should be parsed but flagged is_open_market=0, transaction_type='grant'."""
    from sec_filing_intelligence.form4_parser import parse_form4_xml

    root = _load("multi_transaction.xml")
    txns = parse_form4_xml(root, filing_date="2025-04-01", accession="0000789019-25-000100")

    assert len(txns) == 3
    purchases = [t for t in txns if t["transaction_type"] == "purchase"]
    grants = [t for t in txns if t["transaction_type"] == "grant"]
    assert len(purchases) == 2
    assert len(grants) == 1
    assert all(p["is_open_market"] == 1 for p in purchases)
    assert grants[0]["is_open_market"] == 0


def test_parse_handles_multi_transaction_single_accession():
    from sec_filing_intelligence.form4_parser import parse_form4_xml

    root = _load("multi_transaction.xml")
    txns = parse_form4_xml(root, filing_date="2025-04-01", accession="0000789019-25-000100")

    assert len(txns) == 3
    assert all(t["accession_number"] == "0000789019-25-000100" for t in txns)
    assert all(t["ticker"] == "MSFT" for t in txns)
    assert all(t["insider_name"] == "Nadella Satya" for t in txns)


def test_accession_extraction_from_sec_url_for_legacy_migration():
    from sec_filing_intelligence.form4_parser import extract_accession_from_sec_url

    url = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000001/0000320193-25-000001-index.htm"
    assert extract_accession_from_sec_url(url) == "0000320193-25-000001"

    assert extract_accession_from_sec_url("https://example.com/no-accession-here") is None
    assert extract_accession_from_sec_url(None) is None


def test_is_amendment_detects_form_4a_document_type():
    from sec_filing_intelligence.form4_parser import is_amendment

    plain = _load("single_purchase.xml")
    assert is_amendment(plain) is False

    amend = _load("amendment.xml")
    assert is_amendment(amend) is True


def test_cik_to_ticker_returns_none_for_delisted(monkeypatch):
    """CIK not in SEC cache AND not in scr_universe → returns None + logs a warning."""
    from sec_filing_intelligence import form4_parser

    # Force the SEC cache to be empty
    monkeypatch.setattr(form4_parser, "_cik_to_ticker_cache", {})
    monkeypatch.setattr(form4_parser, "_sec_cache_loaded", True)

    # Mock get_connection to return a db with an empty scr_universe
    import sqlite3
    from contextlib import contextmanager
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE scr_universe (ticker TEXT, cik TEXT)")
    con.commit()

    @contextmanager
    def mock_conn():
        yield con

    monkeypatch.setattr(form4_parser, "_get_connection", mock_conn)

    assert form4_parser.cik_to_ticker("0009999999") is None


def test_cik_to_ticker_uses_scr_universe_on_cik_cache_miss(monkeypatch):
    """SEC cache miss but scr_universe has the CIK → returns the universe ticker."""
    from sec_filing_intelligence import form4_parser

    monkeypatch.setattr(form4_parser, "_cik_to_ticker_cache", {})
    monkeypatch.setattr(form4_parser, "_sec_cache_loaded", True)

    import sqlite3
    from contextlib import contextmanager
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE scr_universe (ticker TEXT, cik TEXT)")
    con.execute("INSERT INTO scr_universe (ticker, cik) VALUES ('DNN', '0001385849')")
    con.commit()

    @contextmanager
    def mock_conn():
        yield con

    monkeypatch.setattr(form4_parser, "_get_connection", mock_conn)

    assert form4_parser.cik_to_ticker("0001385849") == "DNN"
    # Second call — should hit cache (not touch the connection mock again)
    assert form4_parser.cik_to_ticker("0001385849") == "DNN"
