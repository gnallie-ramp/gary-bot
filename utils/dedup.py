"""Message deduplication tracker.

Persists processed-message keys with timestamps to a JSON file so that
duplicate Slack events / channel messages are silently skipped within the
configured TTL window.
"""
from __future__ import annotations

import json
import os
import time
import logging
import threading

from config import DEDUP_STATE_FILE, DEDUP_TTL_DAYS

logger = logging.getLogger(__name__)


class DedupTracker:
    """Thread-safe deduplication tracker backed by a JSON file.

    State format: ``{key: unix_timestamp, ...}``
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._state: dict[str, float] = self._load()
        self._cleanup()

    # ── public API ────────────────────────────────────────────────────────

    def is_processed(self, key: str) -> bool:
        """Return True if *key* was already processed within the TTL window."""
        with self._lock:
            ts = self._state.get(key)
            if ts is None:
                return False
            age_days = (time.time() - ts) / 86400
            if age_days > DEDUP_TTL_DAYS:
                # Expired — treat as not processed
                del self._state[key]
                self._save()
                return False
            return True

    def mark_processed(self, key: str) -> None:
        """Record *key* as processed with the current timestamp and persist."""
        with self._lock:
            self._state[key] = time.time()
            self._save()

    # ── persistence helpers ───────────────────────────────────────────────

    def _load(self) -> dict[str, float]:
        """Read state from the JSON file.  Returns empty dict if file is
        missing or unreadable."""
        if not os.path.exists(DEDUP_STATE_FILE):
            return {}
        try:
            with open(DEDUP_STATE_FILE, "r") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
            logger.warning("Dedup state file has unexpected type; resetting.")
            return {}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load dedup state (%s); starting fresh.", exc)
            return {}

    def _save(self) -> None:
        """Write current state to the JSON file."""
        try:
            with open(DEDUP_STATE_FILE, "w") as fh:
                json.dump(self._state, fh)
        except OSError as exc:
            logger.error("Failed to save dedup state: %s", exc)

    def _cleanup(self) -> None:
        """Remove entries older than ``DEDUP_TTL_DAYS``."""
        cutoff = time.time() - (DEDUP_TTL_DAYS * 86400)
        expired = [k for k, ts in self._state.items() if ts < cutoff]
        if expired:
            for k in expired:
                del self._state[k]
            self._save()
            logger.debug("Dedup cleanup: removed %d expired entries.", len(expired))


# ── module-level singleton ────────────────────────────────────────────────
tracker = DedupTracker()
