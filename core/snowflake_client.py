"""Snowflake client using the snow CLI with browser-based SSO auth.

Runs queries via ``snow sql --format json --silent``, which uses the
cached Okta/SSO session from ``~/.snowflake/config.toml``.  This avoids
the network-policy restriction that blocks PAT connections outside VPN.
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

    Parameters
    ----------
    sql : str
        The SQL query to execute.
    params : ignored
        Kept for API compatibility; not used with the CLI approach.

    Returns
    -------
    pd.DataFrame
    """
    with _lock:
        return _run_query_impl(sql)


def _run_query_impl(sql):
    """Internal: write SQL to a temp file, invoke snow CLI, parse JSON."""
    fd, tmp_path = tempfile.mkstemp(suffix=".sql", prefix="gary_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(sql)

        result = subprocess.run(
            [_SNOW_CLI, "sql", "--filename", tmp_path,
             "--format", "json", "--silent"],
            capture_output=True,
            text=True,
            timeout=180,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"Snowflake query failed (exit {result.returncode}): {stderr}")

    stdout = result.stdout.strip()
    if not stdout:
        return pd.DataFrame()

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse snow CLI output as JSON: {e}\n{stdout[:500]}")

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]
    return df


def get_connection():
    """Compatibility shim — returns None.

    Callers that previously used ``get_connection()`` for ``pd.read_sql``
    should switch to ``run_query()`` instead.
    """
    logger.warning("get_connection() called — use run_query() instead (CLI-based auth)")
    return None


def check_connection():
    """Verify Snowflake access by running a trivial query."""
    df = run_query("SELECT 1 AS ok")
    return not df.empty
