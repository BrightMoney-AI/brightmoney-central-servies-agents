"""
ServiceDef — describes a service to report on, including the PromQL label selectors
used to scope system-health and API queries to that service.

services.json format (project root):
[
  {
    "display_name": "UAA Entity Manager",
    "name_patterns": ["p-uaa-em-.*", "p-uaa-entity-manager.*"],
    "system_job": "system_metrics",
    "api_job": "platform_statsd_metrics",
    "api_name_patterns": ["p.*-uaa-entity-manager-.*"],
    "api_exclude_endpoints": ["//api/account-meta/v0/get/"],
    "api_method": "POST"
  }
]

Multiple name_patterns are joined into a single PromQL regex with |:
  name=~"p-uaa-em-.*|p-uaa-entity-manager.*"

api_name_patterns:      if set, overrides name_patterns for API/endpoint queries.
api_exclude_endpoints:  endpoints to exclude from per-endpoint breakdown (noise filtering).
api_method:             if set, adds method="..." filter to hit/success/error queries.

If services.json is absent, one report is generated with no label filter (all services).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class ServiceDef:
    display_name: str
    name_patterns: list[str] = field(default_factory=list)   # system + fallback API filter
    system_job: Optional[str] = None
    api_job: Optional[str] = None
    api_name_patterns: list[str] = field(default_factory=list)      # overrides name_patterns for API queries
    api_exclude_endpoints: list[str] = field(default_factory=list) # endpoints to exclude from per-endpoint section
    api_method: Optional[str] = None                               # method filter for per-endpoint queries

    def _name_selector(self, patterns: Optional[list[str]] = None) -> Optional[str]:
        p = patterns if patterns is not None else self.name_patterns
        if not p:
            return None
        regex = "|".join(p)
        return f'name=~"{regex}"'

    @property
    def system_selector(self) -> str:
        parts = []
        name = self._name_selector()
        if name:
            parts.append(name)
        if self.system_job:
            parts.append(f'job="{self.system_job}"')
        return ", ".join(parts)

    @property
    def api_selector(self) -> str:
        parts = []
        patterns = self.api_name_patterns if self.api_name_patterns else self.name_patterns
        name = self._name_selector(patterns)
        if name:
            parts.append(name)
        if self.api_job:
            parts.append(f'job="{self.api_job}"')
        return ", ".join(parts)


_ALL_SERVICES = ServiceDef(display_name="All Services")


def load_services(path: str = "services.json") -> list[ServiceDef]:
    p = Path(path)
    if not p.exists():
        log.info("No services.json found — reporting on all services without label filter.")
        return [_ALL_SERVICES]
    data = json.loads(p.read_text())
    services = [ServiceDef(**entry) for entry in data]
    log.info("Loaded %d service(s) from %s", len(services), path)
    return services
