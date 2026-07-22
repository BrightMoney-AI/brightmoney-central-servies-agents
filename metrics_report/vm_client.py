"""
Thin async wrapper around the VictoriaMetrics Prometheus-compatible HTTP API.
Supports instant queries (/api/v1/query) returning a single scalar or the
first result value from a vector.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional

import httpx

from .pagerduty import fire_alert

log = logging.getLogger(__name__)

_QUERY_PATH       = "/api/v1/query"
_QUERY_RANGE_PATH = "/api/v1/query_range"

# Retry configuration for HTTP 429 (Too Many Requests).
# When VictoriaMetrics rate-limits a query, back off briefly and try again.
_MAX_RETRIES   = 3
_RETRY_DELAY_S = 2.0  # seconds between attempts

# Track whether we've already sent a 429-rate-limit PD alert this process run so
# we don't re-fire on every concurrent retry storm.  Reset is intentionally
# not done — a new deploy/restart starts fresh.
_pagerduty_429_alerted = False



class VMClient:
    def __init__(self, base_url: str, headers: Optional[dict[str, str]] = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers  = headers or {}
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "VMClient":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=self._headers,
            timeout=10.0,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _get_with_retry(self, path: str, params: dict) -> httpx.Response:
        """GET with automatic retry on 429 (rate-limited) responses.

        PagerDuty alerting:
          • First 429 encountered → warning alert (dedup_key="vm-http-429").
            Fires immediately so on-call knows rate limiting is happening, even
            if subsequent retries recover.
          • Any other non-2xx on the FINAL attempt → critical/warning alert.
        """
        global _pagerduty_429_alerted
        assert self._client is not None, "VMClient must be used as async context manager"
        for attempt in range(_MAX_RETRIES + 1):
            resp = await self._client.get(path, params=params)

            if resp.status_code == 429:
                # Fire a PD warning on the very first 429 we see (process-wide
                # dedup so a burst of concurrent 429s creates exactly one incident).
                if not _pagerduty_429_alerted:
                    _pagerduty_429_alerted = True
                    asyncio.create_task(fire_alert(
                        summary="VictoriaMetrics rate-limited (HTTP 429) — queries are being retried",
                        severity="warning",
                        source=self._base_url + path,
                        component="vm_client",
                        details={
                            "path": path,
                            "attempt": attempt + 1,
                            "max_retries": _MAX_RETRIES,
                            "retry_delay_s": _RETRY_DELAY_S,
                        },
                        dedup_key="vm-http-429",
                    ))
                if attempt < _MAX_RETRIES:
                    log.warning(
                        "VictoriaMetrics 429 on attempt %d/%d — backing off %.1fs before retry",
                        attempt + 1, _MAX_RETRIES + 1, _RETRY_DELAY_S,
                    )
                    await asyncio.sleep(_RETRY_DELAY_S)
                    continue
                # All retries exhausted — escalate to critical
                asyncio.create_task(fire_alert(
                    summary=f"VictoriaMetrics persistent rate-limit — all {_MAX_RETRIES + 1} attempts returned 429",
                    severity="critical",
                    source=self._base_url + path,
                    component="vm_client",
                    details={"path": path, "attempts": _MAX_RETRIES + 1},
                    dedup_key="vm-http-429-exhausted",
                ))

            if not resp.is_success:
                # Non-429 failure (5xx, 4xx other) — always alert
                severity = "critical" if resp.status_code >= 500 else "warning"
                asyncio.create_task(fire_alert(
                    summary=f"VictoriaMetrics API error: HTTP {resp.status_code} on {path}",
                    severity=severity,
                    source=self._base_url + path,
                    component="vm_client",
                    details={
                        "status_code": resp.status_code,
                        "path": path,
                        "attempt": attempt + 1,
                        "body_preview": resp.text[:300],
                    },
                    dedup_key=f"vm-http-{resp.status_code}",
                ))
            resp.raise_for_status()
            return resp
        resp.raise_for_status()  # final attempt already raised; satisfy type checker
        return resp  # unreachable

    async def query(self, promql: str) -> Optional[float]:
        """Execute an instant PromQL query and return the first numeric value."""
        resp = await self._get_with_retry(_QUERY_PATH, {"query": promql})
        data = resp.json()

        result_type = data.get("data", {}).get("resultType")
        results = data.get("data", {}).get("result", [])

        if not results:
            log.debug("No results for query: %s", promql)
            return None

        if result_type == "scalar":
            return float(data["data"]["result"][1])

        if result_type in ("vector", "matrix"):
            first = results[0]
            value_field = first.get("value") or (first.get("values") or [[None, None]])[-1]
            return float(value_field[1])

        return None

    async def query_vector(self, promql: str, id_label: str = "name") -> list[tuple[str, float]]:
        """Execute a PromQL query and return [(server_id, value), ...] for every result series.

        Uses id_label (default: "name") as the server identifier, falling back to "instance".
        """
        resp = await self._get_with_retry(_QUERY_PATH, {"query": promql})
        data = resp.json()

        results = data.get("data", {}).get("result", [])
        if not results:
            log.debug("No per-server results for query: %s", promql)
            return []

        out: list[tuple[str, float]] = []
        for r in results:
            labels = r.get("metric", {})
            server = labels.get(id_label) or labels.get("instance", "unknown")
            value = float(r["value"][1])
            out.append((server, value))

        return sorted(out)

    async def query_range(self, promql: str, hours: int = 24, step: str = "30m") -> list[float]:
        """Fetch a time series over the past `hours` as one value per `step` interval.

        Returns bucket values oldest-first.  Used for spike analysis — each bucket
        is the aggregated value within that step window (e.g. rate([30m]) at 30m step
        gives non-overlapping 30-minute windows).
        """
        import time as _time
        end   = int(_time.time())
        start = end - hours * 3600
        resp  = await self._get_with_retry(
            _QUERY_RANGE_PATH,
            {"query": promql, "start": start, "end": end, "step": step},
        )
        data    = resp.json()
        results = data.get("data", {}).get("result", [])
        if not results:
            log.debug("No range results for query: %s", promql)
            return []
        return [
            float(v[1])
            for v in results[0].get("values", [])
            if v[1] not in ("NaN", "+Inf", "-Inf")
        ]
