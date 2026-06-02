"""
Thin async wrapper around the VictoriaMetrics Prometheus-compatible HTTP API.
Supports instant queries (/api/v1/query) returning a single scalar or the
first result value from a vector.
"""
import logging
from typing import Optional

import httpx

log = logging.getLogger(__name__)

_QUERY_PATH = "/api/v1/query"


class VMClient:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "VMClient":
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=10.0)
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def query(self, promql: str) -> Optional[float]:
        """Execute an instant PromQL query and return the first numeric value."""
        assert self._client is not None, "VMClient must be used as async context manager"
        resp = await self._client.get(_QUERY_PATH, params={"query": promql})
        resp.raise_for_status()
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
        assert self._client is not None, "VMClient must be used as async context manager"
        resp = await self._client.get(_QUERY_PATH, params={"query": promql})
        resp.raise_for_status()
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
