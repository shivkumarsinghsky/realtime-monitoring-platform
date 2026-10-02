from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration comes from environment variables (see .env.example)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "monitoring"
    log_level: str = "INFO"

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    #: Shared subscription lets several ingestion replicas split the MQTT load.
    mqtt_topic: str = "$share/ingestion/telemetry/#"

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://monitoring:monitoring-local-dev@localhost:5432/monitoring"

    #: Event bus implementation: "redis" (Redis Streams) or "kafka".
    bus: str = "redis"
    kafka_bootstrap: str = "localhost:9092"
    #: Number of stream partitions; readings are routed by hash(source_id) to keep per-source order.
    partitions: int = Field(default=4, ge=1, le=256)

    ingest_queue_size: int = Field(default=10_000, ge=100)
    ingest_batch_size: int = Field(default=500, ge=1)
    ingest_flush_ms: int = Field(default=200, ge=10)

    processor_batch_size: int = Field(default=500, ge=1)
    processor_group: str = "processors"
    consumer_name: str = "processor-1"

    #: A source is considered stale (heartbeat alert) after this many seconds without data.
    stale_after_seconds: int = Field(default=60, ge=5)
    raw_retention_days: int = Field(default=7, ge=1)

    http_port: int = 8000
    #: Comma-separated name=url pairs probed by the service prober, e.g. "api=http://api:8000/health/live"
    probe_targets: str = ""
    probe_interval_seconds: int = Field(default=15, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
