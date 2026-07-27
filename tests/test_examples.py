"""Tests for the public efts_search wrapper and the shipped example scripts."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

from sec_filing_intelligence import filing_scanner

REPO_ROOT = Path(__file__).parent.parent
EXAMPLES = REPO_ROOT / "examples"


def _efts_page(hits: list[dict], total: int) -> dict:
    return {"hits": {"total": {"value": total}, "hits": hits}}


def _hit(display: str, cik: str = "0001526119", form: str = "10-K",
         filed: str = "2026-03-01", adsh: str = "0001-26-000001", **src_extra) -> dict:
    src = {
        "ciks": [cik],
        "display_names": [display],
        "form": form,
        "file_date": filed,
        "adsh": adsh,
    }
    src.update(src_extra)
    return {"_source": src}


class TestEftsSearchWrapper:
    def test_returns_hit_dicts_with_resolved_ticker(self):
        page = _efts_page([_hit("Red Cat Holdings, Inc.  (RCAT)  (CIK 0001526119)")], 1)
        with patch.object(filing_scanner, "_efts_search", side_effect=[page, _efts_page([], 1)]):
            hits = filing_scanner.efts_search("sole source", limit=10)
        assert len(hits) == 1
        h = hits[0]
        assert h["ticker"] == "RCAT"
        assert h["entity_name"] == "Red Cat Holdings, Inc."
        assert h["entity_id"] == "0001526119"
        assert h["form"] == "10-K"
        assert h["file_date"] == "2026-03-01"
        assert h["accession"] == "0001-26-000001"

    def test_limit_and_pagination(self):
        page1 = _efts_page([_hit(f"Co {i}  (AAA)  (CIK 1)") for i in range(3)], 5)
        page2 = _efts_page([_hit(f"Co {i}  (BBB)  (CIK 2)") for i in range(2)], 5)
        with patch.object(filing_scanner, "_efts_search",
                          side_effect=[page1, page2]) as mock_search:
            hits = filing_scanner.efts_search("x", limit=4)
        assert len(hits) == 4
        # Second call must advance start_from by the actual page size (3)
        assert mock_search.call_args_list[1].kwargs["start_from"] == 3

    def test_network_failure_returns_empty(self):
        with patch.object(filing_scanner, "_efts_search", return_value=None):
            assert filing_scanner.efts_search("x") == []

    def test_empty_root_forms_does_not_crash(self):
        page = _efts_page([_hit("NoForm Co  (NFC)  (CIK 3)", form="", root_forms=[])], 1)
        page["hits"]["hits"][0]["_source"].pop("form")
        with patch.object(filing_scanner, "_efts_search", side_effect=[page, _efts_page([], 1)]):
            hits = filing_scanner.efts_search("x", limit=5)
        assert hits[0]["form"] == ""


class TestExampleScriptsSmoke:
    """The examples must at minimum import and wire argparse — the failure
    mode that shipped (ImportError on line 1 of the README Usage block)."""

    def _run(self, script: str, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(EXAMPLES / script), *args],
            capture_output=True, text=True, timeout=60, cwd=REPO_ROOT,
        )

    def test_search_example_help_runs(self):
        proc = self._run("search_sec_filings.py", "--help")
        assert proc.returncode == 0, proc.stderr
        assert "keyword" in proc.stdout.lower()

    def test_score_example_usage_runs(self):
        proc = self._run("score_single_ticker.py")
        assert proc.returncode == 1, proc.stderr
        assert "Usage" in proc.stdout
