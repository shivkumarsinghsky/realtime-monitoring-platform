import json
from datetime import UTC, datetime, timedelta

import pytest

from monitoring.ingestion.parse import InvalidMessage, parse_message
from monitoring.models import SourceKind

NOW = datetime.now(UTC)


def payload(**metrics: float) -> bytes:
    return json.dumps({"ts": NOW.isoformat(), "metrics": metrics, "units": {"vibration_mm_s": "mm/s"}}).encode()


def test_parses_one_reading_per_metric():
    readings = parse_message("telemetry/SITE01/asset/PUMP-101", payload(vibration_mm_s=3.2, bearing_temp_c=61.5))
    assert {r.metric for r in readings} == {"vibration_mm_s", "bearing_temp_c"}
    r = next(r for r in readings if r.metric == "vibration_mm_s")
    assert (r.site, r.source_id, r.kind, r.value, r.unit) == ("SITE01", "PUMP-101", SourceKind.ASSET, 3.2, "mm/s")
    assert r.received_at is not None


@pytest.mark.parametrize(
    "topic,reason",
    [
        ("telemetry/SITE01/PUMP-101", "bad_topic"),
        ("metrics/SITE01/asset/PUMP-101", "bad_topic"),
        ("telemetry/SITE01/robot/R1", "bad_kind"),
        ("telemetry/SITE 01/asset/P1", "invalid_payload"),
    ],
)
def test_rejects_bad_topics(topic, reason):
    with pytest.raises(InvalidMessage) as e:
        parse_message(topic, payload(x_value=1))
    assert e.value.reason == reason


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        json.dumps({"ts": NOW.isoformat(), "metrics": {}}).encode(),
        json.dumps({"ts": NOW.isoformat(), "metrics": {"Bad-Name": 1}}).encode(),
        json.dumps({"ts": "2026-10-01T10:00:00", "metrics": {"cpu_pct": 1}}).encode(),  # no timezone
        json.dumps({"ts": (NOW + timedelta(hours=1)).isoformat(), "metrics": {"cpu_pct": 1}}).encode(),
        b'{"ts": "2026-10-01T10:00:00Z", "metrics": {"cpu_pct": NaN}}',
    ],
)
def test_rejects_invalid_payloads(body):
    with pytest.raises(InvalidMessage) as e:
        parse_message("telemetry/DC1/host/host-01", body)
    assert e.value.reason == "invalid_payload"


def test_rejects_oversized_payloads():
    with pytest.raises(InvalidMessage) as e:
        parse_message("telemetry/DC1/host/host-01", b" " * 70_000)
    assert e.value.reason == "too_large"
