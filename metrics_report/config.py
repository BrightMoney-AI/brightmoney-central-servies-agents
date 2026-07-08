from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # VictoriaMetrics connection
    vm_instance_entrypoint: str = "http://vmselect-observability.brightmoney.net:8481"
    vm_instance_type: str = "cluster"   # "cluster" → /select/0/prometheus; "single" → bare entrypoint
    vm_auth_header: str = ""            # e.g. "Basic bWNwOlh6UjdYMGU..."

    gateway_timeout_secs: float = 5.0
    query_window: str = "24h"

    slack_bot_token: str
    slack_channel_id: str

    # Thresholds for status icons
    cpu_warn_pct: float = 70.0
    cpu_crit_pct: float = 90.0
    mem_warn_pct: float = 75.0
    mem_crit_pct: float = 90.0
    disk_warn_pct: float = 80.0
    disk_crit_pct: float = 90.0
    error_rate_warn_pct: float = 1.0
    error_rate_crit_pct: float = 5.0
    avg_latency_warn_ms: float = 500.0
    avg_latency_crit_ms: float = 1000.0

    # Trino / Iceberg connection
    trino_host: str   = "int-trino.brightmoney.co"
    trino_port: int   = 443
    trino_user: str   = "uaa_team_metrics"
    trino_source: str = "engg_team_code"

    # Airflow metadata DB (empty = disabled)
    airflow_db_url: str = ""

    # Airflow REST API (empty = disabled)
    airflow_api_url: str = ""
    airflow_api_username: str = ""
    airflow_api_password: str = ""

    # Kafka Connect REST API base URLs (empty = disabled)
    kafka_connect_kafka_sink_url: str = ""
    kafka_connect_cdc_sink_url: str = ""
    kafka_connect_debezium_url: str = ""

    @property
    def vm_base_url(self) -> str:
        base = self.vm_instance_entrypoint.rstrip("/")
        if self.vm_instance_type == "cluster":
            return f"{base}/select/0/prometheus"
        return base

    @property
    def vm_headers(self) -> dict:
        if self.vm_auth_header:
            return {"Authorization": self.vm_auth_header}
        return {}

    @property
    def kafka_connect_instances(self) -> dict:
        result = {}
        if self.kafka_connect_kafka_sink_url:
            result["Kafka Sink"] = self.kafka_connect_kafka_sink_url
        if self.kafka_connect_cdc_sink_url:
            result["CDC Sink"] = self.kafka_connect_cdc_sink_url
        if self.kafka_connect_debezium_url:
            result["Debezium"] = self.kafka_connect_debezium_url
        return result


settings = Settings()
