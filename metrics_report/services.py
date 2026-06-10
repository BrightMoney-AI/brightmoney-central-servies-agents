"""
ServiceDef — describes a service to report on, including the PromQL label selectors
used to scope system-health and API queries to that service.

Configuration (project root):
  services.json — general / non-EMS services (e.g. UAA Entity Manager)
  ems.json      — Grafana EMS collective dashboard; row sections become services

services.json format:
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
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_NAME_PATTERN_RE = re.compile(r'name=~"([^"]+)"')

_DISPLAY_NAME_OVERRIDES: dict[str, str] = {
    "Firestore System Metrics": "Firestore",
    "Narada System Metrics": "Narada",
    "Mixpanel System Metrics": "Narada Mixpanel",
    "Email Management Service System Metrics": "Email Management Service",
    "singular forwader": "Singular Forwarder",
    "Facebook Forwader": "Facebook Forwarder",
    "Snap Forwader": "Snap Forwarder",
    "Google Forwader": "Google Forwarder",
}


@dataclass
class ServiceDef:
    display_name: str
    name_patterns: list[str] = field(default_factory=list)   # system + fallback API filter
    system_job: Optional[str] = None
    api_job: Optional[str] = None
    api_name_patterns: list[str] = field(default_factory=list)      # overrides name_patterns for API queries
    api_exclude_endpoints: list[str] = field(default_factory=list) # endpoints to exclude from per-endpoint section
    api_method: Optional[str] = None                               # method filter for per-endpoint queries
    api_request_metric: str = "django_request_count"               # override for services using a different counter metric
    api_response_metric: str = "django_http_responses_total_by_status"  # override for services using a different response metric
    report_group: str = "Central Services"                         # which canvas this service appears in
    rabbitmq_queues: list[str] = field(default_factory=list)       # RabbitMQ queue names to monitor
    kafka_cdc_sinks: list[dict] = field(default_factory=list)      # CDC sink reference: [{sink, debezium, heartbeat_topic}]
    kafka_sinks:     list[str]  = field(default_factory=list)      # plain Kafka sink connector names (no CDC heartbeat)

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



def _normalize_display_name(row_title: str) -> str:
    if row_title in _DISPLAY_NAME_OVERRIDES:
        return _DISPLAY_NAME_OVERRIDES[row_title]
    return row_title.removesuffix(" System Metrics").strip()


def _unique_patterns(patterns: list[str]) -> list[str]:
    return list(dict.fromkeys(patterns))


def parse_ems_dashboard(path: Path) -> list[ServiceDef]:
    """Extract one ServiceDef per row section from a Grafana dashboard export."""
    data = json.loads(path.read_text())
    panels = data.get("panels", [])
    services: list[ServiceDef] = []
    i = 0
    while i < len(panels):
        panel = panels[i]
        if panel.get("type") != "row":
            i += 1
            continue

        row_title = panel.get("title", "").strip()
        if not row_title:
            i += 1
            continue

        patterns: list[str] = []
        j = i + 1
        while j < len(panels) and panels[j].get("type") != "row":
            for target in panels[j].get("targets", []):
                patterns.extend(_NAME_PATTERN_RE.findall(target.get("expr", "")))
            j += 1

        if patterns:
            services.append(
                ServiceDef(
                    display_name=_normalize_display_name(row_title),
                    name_patterns=_unique_patterns(patterns),
                    system_job="system_metrics",
                )
            )
        i = j

    return services


def _merge_services(*groups: list[ServiceDef]) -> list[ServiceDef]:
    """Later groups override jobs on duplicate display_name; name_patterns are unioned."""
    merged: dict[str, ServiceDef] = {}
    for group in groups:
        for svc in group:
            existing = merged.get(svc.display_name)
            if existing is None:
                merged[svc.display_name] = svc
                continue
            patterns = _unique_patterns(existing.name_patterns + svc.name_patterns)
            merged[svc.display_name] = ServiceDef(
                display_name=svc.display_name,
                name_patterns=patterns,
                system_job=svc.system_job or existing.system_job,
                api_job=svc.api_job if svc.api_job is not None else existing.api_job,
                report_group=svc.report_group,
            )
    return list(merged.values())


def _load_json_services(path: Path) -> list[ServiceDef]:
    data = json.loads(path.read_text())
    return [ServiceDef(**entry) for entry in data]


def load_services(
    services_path: str | Path | None = None,
    ems_path: str | Path | None = None,
) -> list[ServiceDef]:
    services_path = Path(services_path) if services_path else _PROJECT_ROOT / "services.json"
    ems_path = Path(ems_path) if ems_path else _PROJECT_ROOT / "ems.json"

    general: list[ServiceDef] = []
    if services_path.exists():
        general = _load_json_services(services_path)
        log.info("Loaded %d general service(s) from %s", len(general), services_path)
    else:
        log.info("No %s — skipping general services.", services_path)

    ems: list[ServiceDef] = []
    if ems_path.exists():
        ems = parse_ems_dashboard(ems_path)
        log.info("Loaded %d EMS service(s) from %s", len(ems), ems_path)

    combined = _merge_services(ems, general)
    log.info("Reporting on %d service(s) total.", len(combined))
    return combined
