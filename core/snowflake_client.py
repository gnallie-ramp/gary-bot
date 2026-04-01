"""Snowflake client using the snow CLI with browser-based SSO auth.

Runs queries via ``snow sql --format json --silent``.

**Workaround for snow CLI 2.8.x JSON truncation bug:** When a single JSON row
exceeds ~530 bytes (many columns), the CLI silently truncates stdout.
When this is detected, the query is retried using
``OBJECT_CONSTRUCT_KEEP_NULL(*)`` which packs each row into a single JSON
string field, bypassing the per-row byte limit entirely.
"""

import json
import logging
import os
import subprocess
import tempfile
import threading

import pandas as pd

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_SNOW_CLI = "/opt/homebrew/bin/snow"


def run_query(sql, params=None):
    """Execute *sql* via the snow CLI and return results as a pandas DataFrame.

    Column names are lowercased for consistency across the codebase.
    """
    with _lock:
        return _run_query_impl(sql)


def _run_snow_sql(sql_text, timeout=180):
    """Write *sql_text* to a temp file, run snow CLI, return (stdout, stderr, returncode)."""
    fd, tmp_path = tempfile.mkstemp(suffix=".sql", prefix="gary_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(sql_text)
        result = subprocess.run(
            [_SNOW_CLI, "sql", "--filename", tmp_path,
             "--format", "json", "--silent"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _try_parse_json(stdout):
    """Parse JSON array from snow CLI output.  Returns list[dict] or None."""
    if not stdout:
        return []
    try:
        data = json.loads(stdout)
        if isinstance(data, list):
            return data
        return None
    except json.JSONDecodeError:
        return None  # Truncated


def _to_dataframe(rows):
    """Convert list[dict] to DataFrame with lowercased columns."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    return df


def _run_query_impl(sql):
    """Execute query, with OBJECT_CONSTRUCT retry on truncation."""
    stdout, stderr, rc = _run_snow_sql(sql)

    # Fast path: JSON parsed successfully
    rows = _try_parse_json(stdout)
    if rows is not None:
        if rc != 0 and rows:
            logger.debug("snow CLI exited %d but returned valid JSON (%d rows)", rc, len(rows))
        return _to_dataframe(rows)

    # JSON truncated — retry with OBJECT_CONSTRUCT_KEEP_NULL(*)
    logger.info("JSON truncated (%d bytes), retrying with OBJECT_CONSTRUCT", len(stdout) if stdout else 0)

    clean_sql = sql.rstrip().rstrip(";")
    obj_sql = f"SELECT OBJECT_CONSTRUCT_KEEP_NULL(*) AS obj FROM ({clean_sql});"
    obj_out, obj_err, obj_rc = _run_snow_sql(obj_sql)
    obj_rows = _try_parse_json(obj_out)

    if obj_rows is None:
        raise RuntimeError(
            f"Snowflake OBJECT_CONSTRUCT retry also truncated "
            f"({len(obj_out) if obj_out else 0} bytes). stderr: {obj_err[:200] if obj_err else 'none'}"
        )

    # Each row is {"OBJ": "{...json...}"} — parse the inner JSON
    parsed_rows = []
    for r in obj_rows:
        inner = r.get("OBJ") or r.get("obj")
        if inner:
            if isinstance(inner, str):
                parsed_rows.append(json.loads(inner))
            elif isinstance(inner, dict):
                parsed_rows.append(inner)

    logger.info("OBJECT_CONSTRUCT retry succeeded: %d rows", len(parsed_rows))
    return _to_dataframe(parsed_rows)


def get_connection():
    """Compatibility shim — returns None."""
    logger.warning("get_connection() called — use run_query() instead (CLI-based auth)")
    return None


def check_connection():
    """Verify Snowflake access by running a trivial query."""
    df = run_query("SELECT 1 AS ok")
    return not df.empty
