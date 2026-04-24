"""Shared helpers for the screener module."""

import logging
import os
import time
from functools import wraps
from pathlib import Path

LOG_DIR = Path(os.environ.get("SFI_LOG_DIR", str(Path(__file__).parent.parent.parent / "logs")))


def get_logger(name: str, log_file: str = "screener.log") -> logging.Logger:
    """Get a logger configured for the screener module."""
    logger = logging.getLogger(f"screener.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        log_dir = Path(LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(log_dir / log_file))
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def rate_limiter(max_rps: float):
    """Decorator to enforce rate limiting on a function.

    Args:
        max_rps: Maximum requests per second.
    """
    min_interval = 1.0 / max_rps

    def decorator(func):
        last_call = [0.0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.monotonic() - last_call[0]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            last_call[0] = time.monotonic()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def chunk_list(lst: list, size: int) -> list:
    """Split a list into chunks of the given size."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]
