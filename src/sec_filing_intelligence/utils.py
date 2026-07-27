"""Shared helpers for the screener module."""

import logging
import os
import threading
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


_rate_lock = threading.Lock()
_rate_next_slot: dict[str, float] = {}


def rate_limiter(max_rps: float, bucket: str = "sec"):
    """Decorator enforcing a rate limit SHARED across every function in `bucket`.

    A per-function budget would multiply the aggregate rate by the number of
    decorated call sites (six SEC callers each holding the full policy budget).
    Slot reservation happens under a lock, so the limit holds under threads.

    Args:
        max_rps: Maximum requests per second for the whole bucket.
        bucket: Budget name — all decorations sharing it share one budget.
    """
    min_interval = 1.0 / max_rps

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with _rate_lock:
                now = time.monotonic()
                slot = max(_rate_next_slot.get(bucket, 0.0), now)
                _rate_next_slot[bucket] = slot + min_interval
            wait = slot - now
            if wait > 0:
                time.sleep(wait)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def chunk_list(lst: list, size: int) -> list:
    """Split a list into chunks of the given size."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]
