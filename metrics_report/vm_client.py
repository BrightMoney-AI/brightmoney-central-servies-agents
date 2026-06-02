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
            # scalar: [timestamp, "value"]
            return float(data["data"]["result"][1])

        if result_type in ("vector", "matrix"):
            first = results[0]
            value_field = first.get("value") or (first.get("values") or [[None, None]])[-1]
            return float(value_field[1])

        return None
