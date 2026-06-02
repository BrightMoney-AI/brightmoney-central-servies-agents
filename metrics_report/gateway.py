"""
MetricsGateway — serialises all VM queries (one at a time) with a hard 5 s timeout.
Failed queries are collected and surfaced in the final report rather than raising.
"""
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
        self._lock = asyncio.Lock()
        self._timeout = timeout_secs
        self.failures: list[FailedQuery] = []

    async def fetch(self, name: str, coro_fn: Callable[[], Awaitable[T]]) -> Optional[T]:
        """
        Acquire the single-request lock, run coro_fn() with a timeout,
        and return its result. On any error returns None and records the failure.
        """
        async with self._lock:
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
