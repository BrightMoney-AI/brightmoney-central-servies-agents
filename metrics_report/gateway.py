"""
MetricsGateway — wraps VM queries with a per-query timeout and failure tracking.

The old serialising asyncio.Lock() has been removed.  Concurrency is now
managed exclusively by the two-layer rate-limiter + semaphore in vm_client.py:
  • _VMRateLimiter  — token-bucket, 10 req/s
  • _VM_GLOBAL_SEM  — semaphore, 10 concurrent in-flight requests

Removing the lock allows collect() to fire all of a service's queries in
parallel and allows multiple services to run concurrently, cutting wall-clock
time from O(N_services × N_queries × avg_query_time) to
O(max(N_queries_per_service) × avg_query_time) in the best case.
"""
from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class FailedQuery:
    name: str
    reason: str


class MetricsGateway:
    def __init__(self, timeout_secs: float = 5.0) -> None:
        self._timeout = timeout_secs
        self.failures: list[FailedQuery] = []

    async def fetch(self, name: str, coro_fn: Callable[[], Awaitable[T]]) -> Optional[T]:
        """Run coro_fn() with a hard timeout; return None and record on any error.

        No serialising lock — multiple calls may be in-flight concurrently.
        Concurrency is capped by the global semaphore in vm_client.py.
        Thread-safety note: .failures is written only from within the asyncio
        event loop so append() is safe without a lock.
        """
        try:
            return await asyncio.wait_for(coro_fn(), timeout=self._timeout)
        except asyncio.TimeoutError:
            reason = f"timed out after {self._timeout}s"
            log.warning("Gateway: %s — %s", name, reason)
            self.failures.append(FailedQuery(name=name, reason=reason))
            return None
        except Exception as exc:
            reason = str(exc)
            log.warning("Gateway: %s — %s", name, reason)
            self.failures.append(FailedQuery(name=name, reason=reason))
            return None

    def reset_failures(self) -> None:
        self.failures = []
