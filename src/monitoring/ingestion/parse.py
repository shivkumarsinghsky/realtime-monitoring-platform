"""MQTT wire format → validated Readings.

Topic:   telemetry/{site}/{kind}/{sourceId}        kind ∈ asset | host | service
Payload: {"ts": "2026-10-01T10:00:00Z", "metrics": {"vibration_mm_s": 3.2, "bearing_temp_c": 61.5},
          "units": {"vibration_mm_s": "mm/s"}}
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import ValidationError

from monitoring.models import MqttPayload, Reading, SourceKind


class InvalidMessage(ValueError):
    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


def parse_message(topic: str, payload: bytes, now: datetime | None = None) -> list[Reading]:
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != "telemetry":
        raise InvalidMessage("bad_topic", topic)
    _, site, kind, source_id = parts
    try:
        source_kind = SourceKind(kind)
    except ValueError as e:
        raise InvalidMessage("bad_kind", kind) from e
    if len(payload) > 64_000:
        raise InvalidMessage("too_large", f"{len(payload)} bytes")
    try:
        body = MqttPayload.model_validate_json(payload)
        received = now or datetime.now(UTC)
        return [
            Reading(
                site=site,
                source_id=source_id,
                kind=source_kind,
                metric=metric,
                value=value,
                unit=body.units.get(metric, ""),
                ts=body.ts,
                received_at=received,
            )
            for metric, value in body.metrics.items()
        ]
    except ValidationError as e:
        raise InvalidMessage("invalid_payload", e.errors()[0]["msg"]) from e
