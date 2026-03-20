"""Persistent task queue for MCP-dependent tasks with retry capability.

Tasks are persisted to a JSON file so they survive process restarts.
Each task tracks its progress through an ordered list of steps, with
retry counts and cached context for resumability.
"""
from __future__ import annotations

import json
import os
import time
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

TASK_QUEUE_FILE = os.path.expanduser("~/.gary_bot_task_queue.json")


class TaskQueue:
    """Thread-safe persistent task queue backed by a JSON file.

    State format: ``{task_id: {task_type, meeting_id, ...}, ...}``
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._state: dict[str, dict] = self._load()

    # ── public API ────────────────────────────────────────────────────────

    def add_task(
        self,
        task_id: str,
        task_type: str,
        meeting_id: str,
        meeting_date: str,
        attendees: list[str],
        account_name: str,
        steps: list[str],
        context: dict[str, Any] | None = None,
    ) -> bool:
        """Add a new task.  Returns True if added, False if task_id already exists."""
        with self._lock:
            if task_id in self._state:
                logger.debug("Task %s already exists; skipping.", task_id)
                return False
            self._state[task_id] = {
                "task_id": task_id,
                "task_type": task_type,
                "meeting_id": meeting_id,
                "meeting_date": meeting_date,
                "attendees": attendees,
                "account_name": account_name,
                "created_at": time.time(),
                "last_attempt": 0,
                "retry_count": 0,
                "error": None,
                "steps_completed": [],
                "steps_remaining": list(steps),
                "context": context or {},
            }
            self._save()
            logger.info("Task %s added (%d steps).", task_id, len(steps))
            return True

    def get_pending_tasks(self, task_type: str | None = None) -> list[dict]:
        """Return all tasks with remaining steps, ordered by created_at ASC.

        Optionally filter by *task_type*.
        """
        with self._lock:
            tasks = [
                t for t in self._state.values()
                if len(t["steps_remaining"]) > 0
                and (task_type is None or t["task_type"] == task_type)
            ]
        tasks.sort(key=lambda t: t["created_at"])
        return tasks

    def complete_step(
        self,
        task_id: str,
        step_name: str,
        context_update: dict[str, Any] | None = None,
    ) -> None:
        """Move *step_name* from remaining to completed.

        Optionally merge *context_update* into the task's context dict.
        """
        with self._lock:
            task = self._state.get(task_id)
            if task is None:
                logger.warning("complete_step: task %s not found.", task_id)
                return
            if step_name in task["steps_remaining"]:
                task["steps_remaining"].remove(step_name)
                task["steps_completed"].append(step_name)
            else:
                logger.warning(
                    "Step %s not in remaining for task %s.", step_name, task_id
                )
            if context_update:
                task["context"].update(context_update)
            task["last_attempt"] = time.time()
            self._save()
            logger.info(
                "Task %s step '%s' completed (%d remaining).",
                task_id,
                step_name,
                len(task["steps_remaining"]),
            )

    def fail_step(self, task_id: str, step_name: str, error_msg: str) -> None:
        """Record failure for *step_name* without moving it.

        Increments retry_count and updates last_attempt.
        """
        with self._lock:
            task = self._state.get(task_id)
            if task is None:
                logger.warning("fail_step: task %s not found.", task_id)
                return
            task["error"] = error_msg
            task["retry_count"] += 1
            task["last_attempt"] = time.time()
            self._save()
            logger.warning(
                "Task %s step '%s' failed (retry #%d): %s",
                task_id,
                step_name,
                task["retry_count"],
                error_msg,
            )

    def remove_task(self, task_id: str) -> None:
        """Delete a task from the queue."""
        with self._lock:
            if task_id in self._state:
                del self._state[task_id]
                self._save()
                logger.info("Task %s removed.", task_id)

    def is_task_exists(self, task_id: str) -> bool:
        """Return True if *task_id* is in the queue."""
        with self._lock:
            return task_id in self._state

    def get_abandoned_tasks(self, max_retries: int = 20) -> list[dict]:
        """Return tasks that have exceeded *max_retries*."""
        with self._lock:
            return [
                t for t in self._state.values()
                if t["retry_count"] > max_retries
            ]

    def cleanup_old_tasks(self, max_age_days: int = 14) -> int:
        """Remove tasks older than *max_age_days*.  Returns count removed."""
        cutoff = time.time() - (max_age_days * 86400)
        with self._lock:
            expired = [
                tid for tid, t in self._state.items()
                if t["created_at"] < cutoff
            ]
            for tid in expired:
                del self._state[tid]
            if expired:
                self._save()
                logger.info("Task queue cleanup: removed %d old tasks.", len(expired))
            return len(expired)

    # ── persistence helpers ───────────────────────────────────────────────

    def _load(self) -> dict[str, dict]:
        """Read state from the JSON file.  Returns empty dict on failure."""
        if not os.path.exists(TASK_QUEUE_FILE):
            return {}
        try:
            with open(TASK_QUEUE_FILE, "r") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
            logger.warning("Task queue state has unexpected type; resetting.")
            return {}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load task queue state (%s); starting fresh.", exc)
            return {}

    def _save(self) -> None:
        """Write current state to the JSON file."""
        try:
            with open(TASK_QUEUE_FILE, "w") as fh:
                json.dump(self._state, fh)
        except OSError as exc:
            logger.error("Failed to save task queue state: %s", exc)


# ── module-level singleton ────────────────────────────────────────────────
queue = TaskQueue()
