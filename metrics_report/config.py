from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    vm_base_url: str = "http://vmselect-observability.brightmoney.net:8481/select/0/prometheus"
    gateway_timeout_secs: float = 5.0
    query_window: str = "24h"

    slack_bot_token: str
    slack_channel_id: str

    # Thresholds for status icons
    cpu_warn_pct: float = 70.0
    cpu_crit_pct: float = 90.0
    mem_warn_pct: float = 75.0
    mem_crit_pct: float = 90.0
    disk_warn_pct: float = 75.0
    disk_crit_pct: float = 90.0
    error_rate_warn_pct: float = 1.0
    error_rate_crit_pct: float = 5.0
    avg_latency_warn_ms: float = 500.0
    avg_latency_crit_ms: float = 1000.0


settings = Settings()
