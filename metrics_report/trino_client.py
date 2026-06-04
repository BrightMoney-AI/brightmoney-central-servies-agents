"""
trino_client.py — Shared async-compatible Trino/Iceberg query client.

Wraps trino.dbapi.connect with retry logic for queue-full and connection errors.
Blocking Trino I/O runs in a thread executor so the async scheduler is not blocked.

Usage:
    from .trino_client import execute_query
    rows = await execute_query("SELECT ...")   # list of dicts
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from trino.dbapi import connect
from trino.exceptions import TrinoConnectionError, TrinoQueryError

from .config import settings

log = logging.getLogger(__name__)


def _get_connection():
    return connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog="iceberg",
        schema="iceberg_db",
        http_scheme="https",
        source=settings.trino_source,
        request_timeout=300,
    )


def _execute_sync(query: str, max_retries: int = 3, backoff_seconds: int = 15) -> list[dict[str, Any]]:
    for attempt in range(max_retries):
        conn = None
        try:
            conn = _get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

        except TrinoQueryError as e:
            error = str(e)
            if "Too many queued queries" in error:
                if attempt < max_retries - 1:
                    wait = backoff_seconds * (attempt + 1)
                    log.warning("Trino queue full, retrying in %ds (attempt %d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"Trino query rejected after {max_retries} attempts: queue full") from e
            if "Access Denied" in error:
                raise PermissionError("Trino access denied — check TRINO_USER permissions") from e
            raise

        except TrinoConnectionError as e:
            if attempt < max_retries - 1:
                log.warning("Trino connection error, retrying in %ds (attempt %d/%d)", backoff_seconds, attempt + 1, max_retries)
                time.sleep(backoff_seconds)
                continue
            raise RuntimeError(f"Cannot connect to Trino at {settings.trino_host}:{settings.trino_port}") from e

        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    return []


async def execute_query(query: str, max_retries: int = 3, backoff_seconds: int = 15) -> list[dict[str, Any]]:
    """Run a Trino query and return rows as a list of dicts. Non-blocking."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _execute_sync, query, max_retries, backoff_seconds)
