"""Telemetry and alert models shared by every component."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SourceId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]
Metric = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]


class SourceKind(StrEnum):
    ASSET = "asset"  # IoT sensors on physical equipment
    HOST = "host"  # server/VM agents
    SERVICE = "service"  # application health probes


class Reading(BaseModel):
    """One normalised measurement. The wire format on MQTT is validated into this model at ingestion."""

    model_config = ConfigDict(frozen=True)

    site: SourceId
    source_id: SourceId
    kind: SourceKind
    metric: Metric
    value: float
    unit: str = Field(default="", max_length=16)
    ts: datetime
    received_at: datetime | None = None

    @field_validator("value")
    @classmethod
    def finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be finite")
        return v

    @field_validator("ts")
    @classmethod
    def utc_and_not_in_future(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        v = v.astimezone(UTC)
        if v > datetime.now(UTC) + timedelta(minutes=5):
            raise ValueError("timestamp is too far in the future (clock skew)")
        return v


class MqttPayload(BaseModel):
    """Payload published by devices/agents on topic telemetry/{site}/{kind}/{sourceId}."""

    metrics: dict[Metric, float] = Field(min_length=1, max_length=100)
    units: dict[str, str] = Field(default_factory=dict)
    ts: datetime


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertState(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AlertEvent(BaseModel):
    """Emitted by the rule engine when an alert episode opens or resolves."""

    model_config = ConfigDict(frozen=True)

    rule_id: str
    site: str
    source_id: str
    metric: str
    severity: Severity
    transition: Literal["raised", "resolved"]
    value: float | None
    message: str
    at: datetime

    @property
    def episode_key(self) -> str:
        return f"{self.rule_id}:{self.source_id}"
