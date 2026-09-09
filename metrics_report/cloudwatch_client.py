from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

_SEM = asyncio.Semaphore(4)


class CloudWatchClient:
    """Thin async wrapper around boto3 CloudWatch get_metric_data.

    Uses IAM role authentication (no explicit credentials needed).
    All blocking boto3 calls are dispatched to a thread pool via
    asyncio.to_thread() so they never block the event loop.
    """

    def __init__(self, region: str = "us-west-2") -> None:
        self._region = region
        self._client = None  # lazy init inside thread

    # ── internal ──────────────────────────────────────────────────────────────

    def _get_client(self):
        """Return (or create) the boto3 client.  Called inside a thread."""
        if self._client is None:
            import boto3
            self._client = boto3.client("cloudwatch", region_name=self._region)
        return self._client

    def _run_get_metric_data(self, queries: list[dict], start: datetime, end: datetime) -> list[dict]:
        """Blocking boto3 call — run in a thread."""
        client = self._get_client()
        results: list[dict] = []
        next_token = None
        while True:
            kwargs: dict = {
                "MetricDataQueries": queries,
                "StartTime": start,
                "EndTime": end,
            }
            if next_token:
                kwargs["NextToken"] = next_token
            resp = client.get_metric_data(**kwargs)
            results.extend(resp.get("MetricDataResults", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break
        return results

    # ── public API ────────────────────────────────────────────────────────────

    async def fetch(
        self,
        queries: list[dict],
        hours: int = 24,
    ) -> dict[str, list[float]]:
        """Run get_metric_data and return ``{id: [values oldest→newest]}``.

        Only queries with ReturnData=True (default) are included in the result.
        Empty / no-data metrics return an empty list.
        """
        end   = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        try:
            async with _SEM:
                raw_results = await asyncio.to_thread(
                    self._run_get_metric_data, queries, start, end
                )
        except Exception as exc:
            _log_boto_error(exc, "fetch")
            return {}

        out: dict[str, list[float]] = {}
        for r in raw_results:
            metric_id = r.get("Id", "")
            status    = r.get("StatusCode", "")
            if status == "InternalError":
                log.warning("CloudWatch InternalError for id=%r — skipping", metric_id)
                continue
            if status == "PartialData":
                log.warning("CloudWatch PartialData for id=%r — returning partial values", metric_id)
            timestamps = r.get("Timestamps", [])
            values     = r.get("Values", [])
            if timestamps:
                # Sort oldest → newest
                pairs = sorted(zip(timestamps, values), key=lambda p: p[0])
                out[metric_id] = [v for _, v in pairs]
            else:
                out[metric_id] = []
        return out

    async def fetch_multi(
        self,
        queries: list[dict],
        hours: int = 24,
    ) -> dict[str, dict[str, list[float]]]:
        """Same as fetch() but groups results by label for SEARCH()-based queries.

        Returns ``{query_id: {label: [values oldest→newest]}}``.
        This is used for per-slug / per-function breakdowns where SEARCH()
        returns multiple time series — each identified by its ``Label`` field.
        """
        end   = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        try:
            async with _SEM:
                raw_results = await asyncio.to_thread(
                    self._run_get_metric_data, queries, start, end
                )
        except Exception as exc:
            _log_boto_error(exc, "fetch_multi")
            return {}

        # CloudWatch SEARCH() returns one result-item per matched time series.
        # The query "Id" for math expressions is the expression id; for raw
        # SEARCH() metrics the id has a suffix appended by CloudWatch.
        # We group by the original query id prefix (before any "__") and use
        # the Label field as the sub-key.
        out: dict[str, dict[str, list[float]]] = {}
        for r in raw_results:
            metric_id = r.get("Id", "")
            label     = r.get("Label", metric_id)
            status    = r.get("StatusCode", "")
            if status == "InternalError":
                log.warning("CloudWatch InternalError for id=%r label=%r — skipping", metric_id, label)
                continue
            if status == "PartialData":
                log.warning("CloudWatch PartialData for id=%r label=%r", metric_id, label)
            timestamps = r.get("Timestamps", [])
            values     = r.get("Values", [])
            # Strip any numeric suffix CloudWatch appends to SEARCH ids
            base_id = metric_id.split("_0")[0] if "_0" in metric_id else metric_id
            if base_id not in out:
                out[base_id] = {}
            if timestamps:
                pairs = sorted(zip(timestamps, values), key=lambda p: p[0])
                out[base_id][label] = [v for _, v in pairs]
            else:
                out[base_id][label] = []
        return out

    @staticmethod
    def scalar(values: list[float], mode: str) -> Optional[float]:
        """Reduce a list of values to a single float.

        Modes:
            avg  — arithmetic mean
            sum  — total
            last — most-recent value
            max  — maximum value
        Returns None if values is empty.
        """
        if not values:
            return None
        if mode == "avg":
            return sum(values) / len(values)
        if mode == "sum":
            return sum(values)
        if mode == "last":
            return values[-1]
        if mode == "max":
            return max(values)
        raise ValueError(f"Unknown mode: {mode!r}")


# ── helpers ───────────────────────────────────────────────────────────────────

def _log_boto_error(exc: Exception, context: str) -> None:
    try:
        from botocore.exceptions import BotoCoreError, ClientError
        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            log.error("CloudWatch ClientError [%s]: %s %s", context, code, exc)
        elif isinstance(exc, BotoCoreError):
            log.error("CloudWatch BotoCoreError [%s]: %s", context, exc)
        else:
            log.error("CloudWatch unexpected error [%s]: %s", context, exc)
    except ImportError:
        log.error("CloudWatch error [%s]: %s", context, exc)
