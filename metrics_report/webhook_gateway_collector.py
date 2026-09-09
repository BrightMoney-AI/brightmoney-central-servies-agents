from __future__ import annotations

"""webhook_gateway_collector.py — CloudWatch-based Webhook Gateway metrics collector.

Runs three get_metric_data batches:
  1. L0 aggregate (SEARCH/math expressions) — success rates, latency, DLQ, overflow, WAF
  2. Per-slug L1 — EB/MSK success rates per webhook provider
  3. Lambda + WAF rules L2 — MetricStat queries per function/rule

Returns a fully populated WebhookGatewayReport.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from .cloudwatch_client import CloudWatchClient
from .config import settings
from .models import (
    Status,
    WebhookDlqMetrics,
    WebhookGatewayReport,
    WebhookInfraMetrics,
    WebhookLambdaMetrics,
    WebhookPipelineMetrics,
    WebhookSlugMetrics,
    WebhookWafMetrics,
)

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# ── CloudWatch resource constants ─────────────────────────────────────────────

_ENV       = "prod"
_NAMESPACE = "Webhooks/Pipeline"
_PERIOD    = 300  # 5 min granularity — 24h = 288 data points

_LAMBDA_FUNCTIONS = [
    "webhook-size-check-prod",
    "webhook-msk-publisher-prod",
    "webhook-dlq-consumer-prod",
]

_WAF_RULES = [
    "AllowOnlyWebhookPath",
    "RateLimitPerIP",
    "RateLimitPerIPPerSlug",
    "AWSManagedCommonRuleSet",
]


# ── Query builders ────────────────────────────────────────────────────────────

def _build_l0_queries(api_name: str, event_bus: str, dlq_name: str, waf_name: str) -> list[dict]:
    """Return all L0 aggregate CloudWatch metric data queries."""
    ns_search  = f"{{{{Webhooks/Pipeline,Environment,Slug}}}}"   # escaped for f-string use
    apigw_dims = f"ApiName=\"{api_name}\" Stage=\"{_ENV}\""
    eb_bus     = event_bus

    return [
        # ── EB publish success % ──────────────────────────────────────────────
        {
            "Id": "eb_succ_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="EBPublishSuccess" Environment="{_ENV}"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "eb_fail_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="EBPublishFailure" Environment="{_ENV}"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "eb_succ_sum",
            "Expression": "SUM(eb_succ_raw)",
            "ReturnData": False,
        },
        {
            "Id": "eb_fail_sum",
            "Expression": "SUM(eb_fail_raw)",
            "ReturnData": False,
        },
        {
            "Id": "eb_success_pct",
            "Expression": "IF(eb_succ_sum+eb_fail_sum > 0, 100*eb_succ_sum/(eb_succ_sum+eb_fail_sum), 100)",
            "Label": "EB Publish Success %",
        },

        # ── MSK publish success % ────────────────────────────────────────────
        {
            "Id": "msk_succ_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="MskPublishSuccess" Environment="{_ENV}"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "msk_fail_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="MskPublishFailure" Environment="{_ENV}"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "msk_succ_sum",
            "Expression": "SUM(msk_succ_raw)",
            "ReturnData": False,
        },
        {
            "Id": "msk_fail_sum",
            "Expression": "SUM(msk_fail_raw)",
            "ReturnData": False,
        },
        {
            "Id": "msk_success_pct",
            "Expression": "IF(msk_succ_sum+msk_fail_sum > 0, 100*msk_succ_sum/(msk_succ_sum+msk_fail_sum), 100)",
            "Label": "MSK Publish Success %",
        },

        # ── API destination delivery % (non-kafka EventBridge rules) ─────────
        {
            "Id": "apidest_inv_raw",
            "Expression": (
                f'SEARCH(\'{{AWS/Events,EventBusName,RuleName}} MetricName="Invocations" EventBusName="{eb_bus}" NOT kafka\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "apidest_fail_raw",
            "Expression": (
                f'SEARCH(\'{{AWS/Events,EventBusName,RuleName}} MetricName="FailedInvocations" EventBusName="{eb_bus}" NOT kafka\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "apidest_inv_sum",
            "Expression": "SUM(apidest_inv_raw)",
            "ReturnData": False,
        },
        {
            "Id": "apidest_fail_sum",
            "Expression": "SUM(apidest_fail_raw)",
            "ReturnData": False,
        },
        {
            "Id": "api_dest_pct",
            "Expression": "IF(apidest_inv_sum > 0, 100*(apidest_inv_sum-apidest_fail_sum)/apidest_inv_sum, 100)",
            "Label": "API Dest Delivery %",
        },

        # ── API GW 5XX / 4XX / throughput ────────────────────────────────────
        {
            "Id": "apigw_count",
            "Expression": (
                f'SUM(SEARCH(\'{{AWS/ApiGateway,ApiName,Stage}} MetricName="Count" {apigw_dims}\', \'Sum\', {_PERIOD}))'
            ),
            "ReturnData": False,
        },
        {
            "Id": "apigw_5xx_raw",
            "Expression": (
                f'SUM(SEARCH(\'{{AWS/ApiGateway,ApiName,Stage}} MetricName="5XXError" {apigw_dims}\', \'Sum\', {_PERIOD}))'
            ),
            "ReturnData": False,
        },
        {
            "Id": "apigw_4xx_raw",
            "Expression": (
                f'SUM(SEARCH(\'{{AWS/ApiGateway,ApiName,Stage}} MetricName="4XXError" {apigw_dims}\', \'Sum\', {_PERIOD}))'
            ),
            "ReturnData": False,
        },
        {
            "Id": "apigw_5xx_pct",
            "Expression": "IF(apigw_count > 0, 100*apigw_5xx_raw/apigw_count, 0)",
            "Label": "API GW 5XX %",
        },
        {
            "Id": "apigw_4xx_pct",
            "Expression": "IF(apigw_count > 0, 100*apigw_4xx_raw/apigw_count, 0)",
            "Label": "API GW 4XX %",
        },
        {
            "Id": "apigw_throughput",
            "Expression": "apigw_count",
            "Label": "API GW Throughput",
        },

        # ── E2E latency (API GW p99 + EB ingestion p99) ──────────────────────
        {
            "Id": "apigw_lat_raw",
            "Expression": (
                f'MAX(SEARCH(\'{{AWS/ApiGateway,ApiName,Stage}} MetricName="Latency" {apigw_dims}\', \'p99\', {_PERIOD}))'
            ),
            "ReturnData": False,
        },
        {
            "Id": "eb_lat_raw",
            "Expression": (
                f'MAX(SEARCH(\'{{AWS/Events,EventBusName,RuleName}} MetricName="IngestionToInvocationCompleteLatency" EventBusName="{eb_bus}" NOT kafka\', \'p99\', {_PERIOD}))'
            ),
            "ReturnData": False,
        },
        {
            "Id": "e2e_latency",
            "Expression": "apigw_lat_raw + eb_lat_raw",
            "Label": "E2E Latency p99 ms",
        },
        {
            "Id": "apigw_latency_p99",
            "Expression": "apigw_lat_raw",
            "Label": "API GW Latency p99 ms",
        },

        # ── S3 overflow count ─────────────────────────────────────────────────
        {
            "Id": "overflow_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="OverflowToS3" Environment="{_ENV}"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "overflow_count",
            "Expression": "SUM(overflow_raw)",
            "Label": "S3 Overflow Count",
        },

        # ── WAF block % ───────────────────────────────────────────────────────
        {
            "Id": "waf_blocked_raw",
            "Expression": (
                f'SEARCH(\'{{AWS/WAFV2,WebACL,Rule,Region,Type}} MetricName="BlockedRequests" WebACL="{waf_name}" Rule="ALL"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "waf_allowed_raw",
            "Expression": (
                f'SEARCH(\'{{AWS/WAFV2,WebACL,Rule,Region,Type}} MetricName="AllowedRequests" WebACL="{waf_name}" Rule="ALL"\', \'Sum\', {_PERIOD})'
            ),
            "ReturnData": False,
        },
        {
            "Id": "waf_blocked_sum",
            "Expression": "SUM(waf_blocked_raw)",
            "ReturnData": False,
        },
        {
            "Id": "waf_allowed_sum",
            "Expression": "SUM(waf_allowed_raw)",
            "ReturnData": False,
        },
        {
            "Id": "waf_block_pct",
            "Expression": "IF(waf_blocked_sum+waf_allowed_sum > 0, 100*waf_blocked_sum/(waf_blocked_sum+waf_allowed_sum), 0)",
            "Label": "WAF Block %",
        },
        {
            "Id": "waf_blocked_total",
            "Expression": "waf_blocked_sum",
            "Label": "WAF Blocked Total",
        },

        # ── DLQ depth (SQS) ───────────────────────────────────────────────────
        {
            "Id": "dlq_depth",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/SQS",
                    "MetricName": "ApproximateNumberOfMessagesVisible",
                    "Dimensions": [{"Name": "QueueName", "Value": dlq_name}],
                },
                "Period": _PERIOD,
                "Stat": "Maximum",
            },
            "Label": "DLQ Depth",
        },
        {
            "Id": "dlq_age",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/SQS",
                    "MetricName": "ApproximateAgeOfOldestMessage",
                    "Dimensions": [{"Name": "QueueName", "Value": dlq_name}],
                },
                "Period": _PERIOD,
                "Stat": "Maximum",
            },
            "Label": "DLQ Age of Oldest Message",
        },
    ]


def _build_slug_queries() -> list[dict]:
    """Return per-slug L1 SEARCH queries."""
    return [
        {
            "Id": "slug_eb_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="EBPublishSuccessRate" Environment="{_ENV}"\', \'Average\', {_PERIOD})'
            ),
            "Label": "${PROP('Dim.Slug')}",
        },
        {
            "Id": "slug_msk_raw",
            "Expression": (
                f'SEARCH(\'{{{_NAMESPACE},Environment,Slug}} MetricName="MskPublishSuccessRate" Environment="{_ENV}"\', \'Average\', {_PERIOD})'
            ),
            "Label": "${PROP('Dim.Slug')}",
        },
    ]


def _build_lambda_waf_queries(waf_name: str) -> list[dict]:
    """Return L2 MetricStat queries for Lambda functions and WAF rules."""
    queries: list[dict] = []

    for fn_name in _LAMBDA_FUNCTIONS:
        safe_id = fn_name.replace("-", "_")
        for metric, stat in [("Errors", "Sum"), ("Throttles", "Sum"), ("Invocations", "Sum")]:
            queries.append({
                "Id": f"{safe_id}_{metric.lower()}",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/Lambda",
                        "MetricName": metric,
                        "Dimensions": [{"Name": "FunctionName", "Value": fn_name}],
                    },
                    "Period": _PERIOD,
                    "Stat": stat,
                },
                "Label": f"{fn_name} {metric}",
            })
        # Duration p99 — use ExtendedStatistics
        queries.append({
            "Id": f"{safe_id}_duration",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/Lambda",
                    "MetricName": "Duration",
                    "Dimensions": [{"Name": "FunctionName", "Value": fn_name}],
                },
                "Period": _PERIOD,
                "Stat": "p99",
            },
            "Label": f"{fn_name} Duration p99",
        })

    # WAF per-rule blocked counts
    for rule in _WAF_RULES:
        safe_rule = rule.replace("-", "_").lower()
        queries.append({
            "Id": f"waf_rule_{safe_rule}",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/WAFV2",
                    "MetricName": "BlockedRequests",
                    "Dimensions": [
                        {"Name": "WebACL", "Value": waf_name},
                        {"Name": "Rule", "Value": rule},
                        {"Name": "Region", "Value": settings.aws_region},
                        {"Name": "Type", "Value": "REGIONAL"},
                    ],
                },
                "Period": _PERIOD,
                "Stat": "Sum",
            },
            "Label": f"WAF Rule {rule}",
        })

    return queries


# ── Collector ─────────────────────────────────────────────────────────────────

async def collect_webhook_gateway() -> WebhookGatewayReport:
    """Collect all Webhook Gateway metrics from CloudWatch and return a report."""
    failures: list[str] = []
    cw = CloudWatchClient(region=settings.aws_region)

    api_name  = settings.webhook_gw_api_name
    event_bus = settings.webhook_gw_event_bus
    dlq_name  = settings.webhook_gw_dlq_name
    waf_name  = settings.webhook_gw_waf_name

    # ── Fire all three query batches concurrently ─────────────────────────────
    l0_queries     = _build_l0_queries(api_name, event_bus, dlq_name, waf_name)
    slug_queries   = _build_slug_queries()
    lam_waf_queries = _build_lambda_waf_queries(waf_name)

    l0_data, slug_raw, lam_waf_data = await asyncio.gather(
        cw.fetch(l0_queries),
        cw.fetch_multi(slug_queries),
        cw.fetch(lam_waf_queries),
        return_exceptions=True,
    )

    if isinstance(l0_data, Exception):
        log.error("L0 CloudWatch fetch failed: %s", l0_data)
        failures.append(f"L0 fetch: {l0_data}")
        l0_data = {}

    if isinstance(slug_raw, Exception):
        log.error("Slug CloudWatch fetch failed: %s", slug_raw)
        failures.append(f"Slug fetch: {slug_raw}")
        slug_raw = {}

    if isinstance(lam_waf_data, Exception):
        log.error("Lambda/WAF CloudWatch fetch failed: %s", lam_waf_data)
        failures.append(f"Lambda/WAF fetch: {lam_waf_data}")
        lam_waf_data = {}

    scalar = CloudWatchClient.scalar

    # ── L0 Pipeline metrics ───────────────────────────────────────────────────
    eb_success_pct  = scalar(l0_data.get("eb_success_pct",  []), "avg")
    msk_success_pct = scalar(l0_data.get("msk_success_pct", []), "avg")
    api_dest_pct    = scalar(l0_data.get("api_dest_pct",    []), "avg")
    overflow_vals   = l0_data.get("overflow_count", [])
    overflow_count  = int(scalar(overflow_vals, "sum") or 0)

    pipeline = WebhookPipelineMetrics(
        eb_success_pct=eb_success_pct,
        msk_success_pct=msk_success_pct,
        api_dest_pct=api_dest_pct,
        overflow_count=overflow_count,
        invalid_count=0,  # not tracked separately yet
    )

    # ── L0 Infra metrics ──────────────────────────────────────────────────────
    apigw_5xx_pct       = scalar(l0_data.get("apigw_5xx_pct", []),       "avg")
    apigw_4xx_pct       = scalar(l0_data.get("apigw_4xx_pct", []),       "avg")
    apigw_throughput    = int(scalar(l0_data.get("apigw_throughput", []), "sum") or 0)
    e2e_latency_p99     = scalar(l0_data.get("e2e_latency",    []),       "avg")
    apigw_latency_p99   = scalar(l0_data.get("apigw_latency_p99", []),   "avg")

    infra = WebhookInfraMetrics(
        apigw_5xx_pct=apigw_5xx_pct,
        apigw_4xx_pct=apigw_4xx_pct,
        apigw_throughput=apigw_throughput,
        e2e_latency_p99_ms=e2e_latency_p99,
        apigw_latency_p99_ms=apigw_latency_p99,
    )

    # ── DLQ ───────────────────────────────────────────────────────────────────
    dlq_depth_val = scalar(l0_data.get("dlq_depth", []), "last")
    dlq_age_val   = scalar(l0_data.get("dlq_age",   []), "last")

    dlq = WebhookDlqMetrics(
        depth_now=int(dlq_depth_val) if dlq_depth_val is not None else 0,
        age_oldest_s=int(dlq_age_val) if dlq_age_val is not None else None,
    )

    # ── WAF ───────────────────────────────────────────────────────────────────
    waf_block_pct    = scalar(l0_data.get("waf_block_pct",    []), "avg")
    waf_blocked_total = int(scalar(l0_data.get("waf_blocked_total", []), "sum") or 0)
    waf_rules: dict[str, int] = {}
    for rule in _WAF_RULES:
        safe_rule = rule.replace("-", "_").lower()
        rule_vals = lam_waf_data.get(f"waf_rule_{safe_rule}", [])
        waf_rules[rule] = int(scalar(rule_vals, "sum") or 0)

    waf = WebhookWafMetrics(
        block_pct=waf_block_pct,
        blocked_total=waf_blocked_total,
        rules=waf_rules,
    )

    # ── Per-slug L1 ───────────────────────────────────────────────────────────
    # slug_raw is {query_id: {label: [values]}}
    # We merge eb and msk by slug name
    eb_by_slug:  dict[str, list[float]] = slug_raw.get("slug_eb_raw",  {})
    msk_by_slug: dict[str, list[float]] = slug_raw.get("slug_msk_raw", {})
    all_slugs = sorted(set(eb_by_slug) | set(msk_by_slug))
    slugs: list[WebhookSlugMetrics] = []
    for slug in all_slugs:
        eb_pct  = scalar(eb_by_slug.get(slug,  []), "avg")
        msk_pct = scalar(msk_by_slug.get(slug, []), "avg")
        slugs.append(WebhookSlugMetrics(
            slug=slug,
            eb_success_pct=eb_pct,
            msk_success_pct=msk_pct,
        ))

    # ── Lambda L2 ─────────────────────────────────────────────────────────────
    lambdas: list[WebhookLambdaMetrics] = []
    for fn_name in _LAMBDA_FUNCTIONS:
        safe_id = fn_name.replace("-", "_")
        errors     = int(scalar(lam_waf_data.get(f"{safe_id}_errors",      []), "sum") or 0)
        throttles  = int(scalar(lam_waf_data.get(f"{safe_id}_throttles",   []), "sum") or 0)
        invocations = int(scalar(lam_waf_data.get(f"{safe_id}_invocations", []), "sum") or 0)
        duration   = scalar(lam_waf_data.get(f"{safe_id}_duration",        []), "avg")
        lambdas.append(WebhookLambdaMetrics(
            name=fn_name,
            errors=errors,
            throttles=throttles,
            duration_p99=duration,
            invocations=invocations,
        ))

    # ── Overall status ────────────────────────────────────────────────────────
    status = _compute_status(pipeline, infra, dlq, waf, failures)

    return WebhookGatewayReport(
        pipeline=pipeline,
        infra=infra,
        dlq=dlq,
        waf=waf,
        slugs=slugs,
        lambdas=lambdas,
        reported_at=datetime.now(IST),
        status=status,
        failures=failures,
    )


def _compute_status(
    pipeline: WebhookPipelineMetrics,
    infra: WebhookInfraMetrics,
    dlq: WebhookDlqMetrics,
    waf: WebhookWafMetrics,
    failures: list[str],
) -> Status:
    if failures:
        return Status.WARNING

    def _is_crit() -> bool:
        # Success % < 95
        for pct in [pipeline.eb_success_pct, pipeline.msk_success_pct, pipeline.api_dest_pct]:
            if pct is not None and pct < 95:
                return True
        # 5XX % >= 5
        if infra.apigw_5xx_pct is not None and infra.apigw_5xx_pct >= 5:
            return True
        # DLQ >= 10
        if dlq.depth_now >= 10:
            return True
        # E2E latency >= 10000ms
        if infra.e2e_latency_p99_ms is not None and infra.e2e_latency_p99_ms >= 10_000:
            return True
        # WAF block % >= 40
        if waf.block_pct is not None and waf.block_pct >= 40:
            return True
        return False

    def _is_warn() -> bool:
        # Success % < 99
        for pct in [pipeline.eb_success_pct, pipeline.msk_success_pct, pipeline.api_dest_pct]:
            if pct is not None and pct < 99:
                return True
        # 5XX % >= 1
        if infra.apigw_5xx_pct is not None and infra.apigw_5xx_pct >= 1:
            return True
        # DLQ >= 1
        if dlq.depth_now >= 1:
            return True
        # E2E latency >= 2000ms
        if infra.e2e_latency_p99_ms is not None and infra.e2e_latency_p99_ms >= 2_000:
            return True
        # WAF block % >= 10
        if waf.block_pct is not None and waf.block_pct >= 10:
            return True
        # S3 overflow any
        if pipeline.overflow_count >= 1:
            return True
        return False

    if _is_crit():
        return Status.CRITICAL
    if _is_warn():
        return Status.WARNING
    return Status.HEALTHY
