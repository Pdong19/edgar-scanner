"""Shared test fixtures for SEC Filing Intelligence."""

import os
import tempfile

import pytest

# Point DB to a temp file before any module imports config.DB_PATH
_tmp_dir = tempfile.mkdtemp()
os.environ.setdefault("SFI_DB_PATH", os.path.join(_tmp_dir, "test.db"))
os.environ.setdefault("SFI_LOG_DIR", os.path.join(_tmp_dir, "logs"))
