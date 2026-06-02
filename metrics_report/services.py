"""
ServiceDef — describes a service to report on, including the PromQL label selectors
used to scope system-health and API queries to that service.

services.json format (project root):
[
  {
    "display_name": "UAA Entity Manager",
    "name_pattern": "p-uaa-entity-manager.*",
    "system_job": "system_metrics",
    "api_job": null
  }
]

If services.json is absent, one report is generated with no label filter (all services).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class ServiceDef:
    display_name: str
    name_pattern: Optional[str] = None   # regex for `name` label; None = no filter
    system_job: Optional[str] = None     # job label for node_exporter / system metrics
    api_job: Optional[str] = None        # job label for app/API metrics; None = omit

    @property
    def system_selector(self) -> str:
        """PromQL label selector fragment for system-health queries."""
        parts = []
        if self.name_pattern:
            parts.append(f'name=~"{self.name_pattern}"')
        if self.system_job:
            parts.append(f'job="{self.system_job}"')
        return ", ".join(parts)

    @property
    def api_selector(self) -> str:
        """PromQL label selector fragment for API queries."""
        parts = []
        if self.name_pattern:
            parts.append(f'name=~"{self.name_pattern}"')
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
