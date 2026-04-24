"""Shared Form 4 parser module for the SEC Filing Intelligence system.

Owns XML transaction extraction, CIK-to-ticker resolution, accession-number
helpers, and Form 4/A amendment detection. No HTTP, no DB writes -- pure
parse + lookup.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Optional

logger = logging.getLogger("form4_parser")

# In-process CIK↔ticker caches. Populated lazily from SEC + scr_universe.
_cik_to_ticker_cache: dict[str, str] = {}
_ticker_to_cik_cache: dict[str, str] = {}
_sec_cache_loaded = False


def _get_connection():
    """Indirection point so tests can monkeypatch without importing db at module load time."""
    from .db import get_connection
    return get_connection()


TRANSACTION_CODES = {
    "P": "purchase",
    "S": "sale",
    "A": "grant",
    "M": "exercise",
    "G": "gift",
    "F": "tax_withholding",
    "C": "conversion",
    "J": "other",
}


def _local_tag(elem: ET.Element) -> str:
    tag = elem.tag
    return tag.split("}")[-1] if "}" in tag else tag


def _find_all(root: ET.Element, tag: str) -> list[ET.Element]:
    return [e for e in root.iter() if _local_tag(e) == tag]


def _get_text(root: ET.Element, tag: str) -> Optional[str]:
    for elem in root.iter():
        if _local_tag(elem) == tag and elem.text:
            return elem.text.strip()
    return None


def _get_value(root: ET.Element, tag: str) -> Optional[float]:
    """Form 4 XML stores values as <tag><value>N</value></tag>; extract the float."""
    for elem in root.iter():
        if _local_tag(elem) == tag:
            children = list(elem)
            if children and children[0].text:
                try:
                    return float(children[0].text.replace(",", ""))
                except ValueError:
                    pass
            break
    return None


def parse_form4_xml(root: ET.Element, filing_date: str, accession: str) -> list[dict]:
    """Parse a Form 4 XML document into transaction dicts.

    Args:
        root: parsed XML element for the Form 4 document.
        filing_date: the SEC filing date (NOT the transaction date — that comes from XML).
        accession: the accession number of THIS filing (with hyphens: NNNNNNNNNN-NN-NNNNNN).

    Returns:
        List of transaction dicts, one per <nonDerivativeTransaction>. Derivative
        transactions are intentionally skipped — future dim refinements may want them,
        but at write-time we only surface non-derivative rows for dim 11.
    """
    ticker = _get_text(root, "issuerTradingSymbol") or ""
    cik = (_get_text(root, "issuerCik") or "").zfill(10) if _get_text(root, "issuerCik") else ""
    owner_name = _get_text(root, "rptOwnerName")
    officer_title = _get_text(root, "officerTitle")
    is_director = _get_text(root, "isDirector") == "1"
    is_ten_pct = _get_text(root, "isTenPercentOwner") == "1"

    if officer_title:
        title = officer_title
    elif is_director:
        title = "Director"
    elif is_ten_pct:
        title = "10% Owner"
    else:
        title = "Other"

    transactions = []
    for txn in _find_all(root, "nonDerivativeTransaction"):
        code = _get_text(txn, "transactionCode")
        if not code:
            continue

        shares_f = _get_value(txn, "transactionShares")
        price = _get_value(txn, "transactionPricePerShare")
        txn_date = _get_text(txn, "transactionDate") or filing_date
        shares_after_f = _get_value(txn, "sharesOwnedFollowingTransaction")

        shares = int(shares_f) if shares_f is not None else None
        shares_after = int(shares_after_f) if shares_after_f is not None else None
        total_value = round(shares * price, 2) if (shares and price and price > 0) else None

        transactions.append({
            "ticker": ticker,
            "cik": cik,
            "filing_date": filing_date,
            "transaction_date": txn_date,
            "insider_name": owner_name,
            "insider_title": title,
            "transaction_type": TRANSACTION_CODES.get(code, "other"),
            "transaction_code": code,
            "shares": shares,
            "price_per_share": price,
            "total_value": total_value,
            "shares_owned_after": shares_after,
            "is_open_market": 1 if code in ("P", "S") else 0,
            "accession_number": accession,
        })

    return transactions


_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def extract_accession_from_sec_url(url: Optional[str]) -> Optional[str]:
    """Extract a hyphenated accession number from a SEC archives URL.

    Example input:
      https://www.sec.gov/Archives/edgar/data/320193/000032019325000001/0000320193-25-000001-index.htm
    Returns: '0000320193-25-000001' (or None if no accession pattern in url).
    """
    if not url:
        return None
    m = _ACCESSION_RE.search(url)
    return m.group(1) if m else None


def is_amendment(root: ET.Element) -> bool:
    """True if the filing is a Form 4/A (amendment), False otherwise."""
    doc_type = _get_text(root, "documentType")
    return doc_type == "4/A" if doc_type else False


def _load_sec_cik_cache() -> None:
    """Load the SEC's company_tickers.json into the in-process caches. Called on first miss."""
    global _sec_cache_loaded
    if _sec_cache_loaded:
        return
    try:
        import requests
        from .config import EDGAR_USER_AGENT
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for entry in data.values():
            ticker = str(entry.get("ticker", "")).upper()
            cik = str(entry.get("cik_str", "")).zfill(10)
            if ticker and cik:
                _cik_to_ticker_cache[cik] = ticker
                _ticker_to_cik_cache[ticker] = cik
        logger.info("Loaded %d ticker↔CIK mappings from SEC", len(_cik_to_ticker_cache))
    except Exception as exc:
        logger.warning("Failed to load SEC company_tickers.json: %s", exc)
    _sec_cache_loaded = True


def cik_to_ticker(cik: str) -> Optional[str]:
    """Resolve CIK → ticker using SEC's company_tickers.json, falling back to scr_universe.

    Returns None if neither source has a match — emits a structured warning so we can
    audit skip rates in the poll log. Handles:
      - Leading-zero normalisation (both 1385849 and 0001385849 work)
      - Delisted tickers still in scr_universe
      - Newly-IPO'd tickers not yet in SEC's public ticker map
    """
    if not cik:
        return None
    cik_z = str(cik).zfill(10)

    if cik_z in _cik_to_ticker_cache:
        return _cik_to_ticker_cache[cik_z]

    _load_sec_cik_cache()
    if cik_z in _cik_to_ticker_cache:
        return _cik_to_ticker_cache[cik_z]

    # Fallback: scr_universe. Matches on either zero-padded or bare numeric CIK.
    try:
        with _get_connection() as conn:
            row = conn.execute(
                "SELECT ticker FROM scr_universe WHERE cik = ? OR cik = ? LIMIT 1",
                (cik_z, cik_z.lstrip("0") or "0"),
            ).fetchone()
    except Exception as exc:
        logger.warning("scr_universe fallback failed for CIK %s: %s", cik_z, exc)
        return None

    if row:
        ticker = row["ticker"] if hasattr(row, "keys") else row[0]
        _cik_to_ticker_cache[cik_z] = ticker
        _ticker_to_cik_cache[ticker] = cik_z
        return ticker

    logger.warning("CIK %s not resolvable (neither SEC cache nor scr_universe)", cik_z)
    return None


def ticker_to_cik(ticker: str) -> Optional[str]:
    """Inverse of cik_to_ticker — same cache, same fallback logic."""
    if not ticker:
        return None
    t = ticker.upper()
    if t in _ticker_to_cik_cache:
        return _ticker_to_cik_cache[t]

    _load_sec_cik_cache()
    if t in _ticker_to_cik_cache:
        return _ticker_to_cik_cache[t]

    try:
        with _get_connection() as conn:
            row = conn.execute(
                "SELECT cik FROM scr_universe WHERE ticker = ? LIMIT 1", (t,)
            ).fetchone()
    except Exception:
        return None

    if row:
        cik = row["cik"] if hasattr(row, "keys") else row[0]
        if cik:
            cik_z = str(cik).zfill(10)
            _ticker_to_cik_cache[t] = cik_z
            _cik_to_ticker_cache[cik_z] = t
            return cik_z
    return None
