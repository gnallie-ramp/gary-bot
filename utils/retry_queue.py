"""Persistent retry queue for failed MCP tasks.

When a task fails due to auth expiry or transient MCP errors, it gets queued
here with all context needed to retry.  The auth health check triggers a
queue drain whenever a connector recovers (expired -> ok).

Queue entries are persisted to a JSON file so tasks survive bot restarts.
Dedup keys prevent duplicate retries for the same event.
"""
from __future__ import annotations

import json
import os
import time
import logging
import threading
from typing import Any, Callable

from config import GREG_SLACK_ID

logger = logging.getLogger(__name__)

RETRY_QUEUE_FILE = os.path.expanduser("~/.gary_bot_retry_queue.json")

# Max age before a queued task is considered stale and discarded (48 hours)
_MAX_AGE_SECONDS = 48 * 3600

# Max retry attempts per task before giving up
_MAX_RETRIES = 3


class RetryQueue:
    """Thread-safe persistent queue for failed tasks that should be retried."""

    def __init__(self):
        self._lock = threading.Lock()
        self._queue: list[dict] = self._load()
        self._cleanup_stale()

    # ── public API ────────────────────────────────────────────────────────

    def enqueue(
        self,
        task_type: str,
        connector: str,
        dedup_key: str,
        context: dict[str, Any],
        error: str = "",
    ) -> bool:
        """Add a failed task to the retry queue.

        Parameters
        ----------
        task_type : str
            Category of task: "post_meeting_email", "post_meeting_opp",
            "alert_email", "opp_update", etc.
        connector : str
            The MCP connector that failed: "gmail", "salesforce", "gong", etc.
        dedup_key : str
            Unique key to prevent duplicate queue entries for the same event.
        context : dict
            All data needed to retry the task (recipient, subject, body, etc).
        error : str
            The error message from the failed attempt.

        Returns
        -------
        bool
            True if enqueued, False if already in queue (dedup hit).
        """
        with self._lock:
            # Dedup check
            if any(item["dedup_key"] == dedup_key for item in self._queue):
                logger.debug("Retry queue: dedup hit for %s, skipping.", dedup_key)
                return False

            entry = {
                "task_type": task_type,
                "connector": connector,
                "dedup_key": dedup_key,
                "context": context,
                "error": error,
                "queued_at": time.time(),
                "retries": 0,
            }
            self._queue.append(entry)
            self._save()
            logger.info(
                "Retry queue: enqueued %s (connector=%s, key=%s)",
                task_type, connector, dedup_key,
            )
            return True

    def drain(self, connector: str, executor: Callable[[dict], bool]) -> tuple[int, int]:
        """Retry all queued tasks for a recovered connector.

        Parameters
        ----------
        connector : str
            The connector that just recovered.
        executor : callable
            Function that takes a queue entry dict and attempts to execute it.
            Should return True on success, False on failure.

        Returns
        -------
        tuple[int, int]
            (succeeded, failed) counts.
        """
        with self._lock:
            pending = [item for item in self._queue if item["connector"] == connector]

        if not pending:
            return 0, 0

        logger.info("Retry queue: draining %d tasks for recovered connector %s", len(pending), connector)
        succeeded = 0
        failed = 0

        for item in pending:
            try:
                ok = executor(item)
                if ok:
                    succeeded += 1
                    with self._lock:
                        self._queue = [q for q in self._queue if q["dedup_key"] != item["dedup_key"]]
                        self._save()
                else:
                    failed += 1
                    with self._lock:
                        for q in self._queue:
                            if q["dedup_key"] == item["dedup_key"]:
                                q["retries"] += 1
                                if q["retries"] >= _MAX_RETRIES:
                                    logger.warning(
                                        "Retry queue: giving up on %s after %d retries",
                                        item["dedup_key"], _MAX_RETRIES,
                                    )
                                    self._queue = [
                                        x for x in self._queue
                                        if x["dedup_key"] != item["dedup_key"]
                                    ]
                                break
                        self._save()
            except Exception as exc:
                logger.error("Retry queue: executor failed for %s: %s", item["dedup_key"], exc)
                failed += 1

        return succeeded, failed

    def get_pending(self, connector: str | None = None) -> list[dict]:
        """Return pending tasks, optionally filtered by connector."""
        with self._lock:
            if connector:
                return [dict(item) for item in self._queue if item["connector"] == connector]
            return [dict(item) for item in self._queue]

    def pending_count(self, connector: str | None = None) -> int:
        """Return count of pending tasks."""
        return len(self.get_pending(connector))

    def remove(self, dedup_key: str) -> bool:
        """Remove a specific task by dedup key."""
        with self._lock:
            before = len(self._queue)
            self._queue = [q for q in self._queue if q["dedup_key"] != dedup_key]
            if len(self._queue) < before:
                self._save()
                return True
            return False

    def format_status(self) -> str:
        """Return a human-readable summary of the queue."""
        with self._lock:
            if not self._queue:
                return "Retry queue: empty"
            by_connector: dict[str, int] = {}
            for item in self._queue:
                c = item["connector"]
                by_connector[c] = by_connector.get(c, 0) + 1
            parts = [f"{c}: {n}" for c, n in sorted(by_connector.items())]
            return f"Retry queue: {len(self._queue)} pending ({', '.join(parts)})"

    # ── persistence helpers ───────────────────────────────────────────────

    def _load(self) -> list[dict]:
        if not os.path.exists(RETRY_QUEUE_FILE):
            return []
        try:
            with open(RETRY_QUEUE_FILE, "r") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                return data
            logger.warning("Retry queue file has unexpected type; resetting.")
            return []
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load retry queue (%s); starting fresh.", exc)
            return []

    def _save(self) -> None:
        try:
            with open(RETRY_QUEUE_FILE, "w") as fh:
                json.dump(self._queue, fh)
        except OSError as exc:
            logger.error("Failed to save retry queue: %s", exc)

    def _cleanup_stale(self) -> None:
        cutoff = time.time() - _MAX_AGE_SECONDS
        stale = [item for item in self._queue if item["queued_at"] < cutoff]
        if stale:
            self._queue = [item for item in self._queue if item["queued_at"] >= cutoff]
            self._save()
            logger.info("Retry queue: cleaned up %d stale entries.", len(stale))


# ── module-level singleton ────────────────────────────────────────────────
retry_queue = RetryQueue()
