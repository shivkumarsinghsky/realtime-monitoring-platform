"""End-to-end: MQTT → ingestion → Redis Streams → processor → Redis/PostgreSQL → API."""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

import aiomqtt
import httpx
import pytest

from monitoring import db
from monitoring.api.app import create_app
from monitoring.bus import partition_for
from monitoring.bus.redis_streams import RedisStreamsBus
from monitoring.config import Settings
from monitoring.ingestion.service import INVALID_TOPIC, Ingestion
from monitoring.models import Reading, Severity, SourceKind
from monitoring.processing.rules import RuleEngine, ThresholdRule
from monitoring.processing.service import TELEMETRY_TOPIC, Processor
from monitoring.processing.state import LatestState

from .conftest import MQTT_HOST, requires_infra, requires_mqtt

pytestmark = requires_infra

VIB = ThresholdRule("vib-high", "vibration_mm_s", ">", 7.1, 4.5, 30, Severity.WARNING, frozenset({SourceKind.ASSET}))


def make_processor(redis, pool, stale=timedelta(minutes=5)):
    bus = RedisStreamsBus(redis, partitions=4, consumer="proc-test")
    return bus, Processor(bus, pool, LatestState(redis), RuleEngine([VIB], stale), batch_size=200)


async def drain(bus, processor, rounds=20):
    events = []
    for _ in range(rounds):
        msgs = await bus.read(TELEMETRY_TOPIC, 500, block_ms=100)
        if not msgs:
            break
        events += await processor.process_batch(msgs)
    return events


@requires_mqtt
async def test_mqtt_to_api(redis, pool):
    # Unique MQTT client id: the ingestion uses a persistent session, so reusing an id across test runs would
    # redeliver messages queued for an earlier run.
    settings = Settings(
        mqtt_host=MQTT_HOST, mqtt_topic="telemetry/#", ingest_flush_ms=50, service_name=f"it-{uuid.uuid4().hex[:8]}"
    )
    ingest_bus = RedisStreamsBus(redis, partitions=4)
    ingestion = Ingestion(settings, ingest_bus)
    task = asyncio.create_task(ingestion.run())
    bus, processor = make_processor(redis, pool)
    try:
        for _ in range(50):
            if ingestion.connected:
                break
            await asyncio.sleep(0.1)
        start = datetime.now(UTC) - timedelta(minutes=2)
        async with aiomqtt.Client(MQTT_HOST, identifier="it-publisher") as client:
            for i in range(8):  # vibration high for 70 s of device time → one alert
                ts = (start + timedelta(seconds=10 * i)).isoformat()
                body = {"ts": ts, "metrics": {"vibration_mm_s": 8.0 + i * 0.1, "bearing_temp_c": 60}}
                await client.publish("telemetry/SITE01/asset/PUMP-101", json.dumps(body), qos=1)
            await client.publish("telemetry/SITE01/asset/PUMP-101", b"{broken", qos=1)
        events = []
        for _ in range(50):
            events += await drain(bus, processor)
            if events:
                break
            await asyncio.sleep(0.1)
        assert [(e.rule_id, e.transition) for e in events] == [("vib-high", "raised")]
    finally:
        task.cancel()

    assert await pool.fetchval("SELECT count(*) FROM readings WHERE source_id = 'PUMP-101'") == 16
    invalid_stream = f"{INVALID_TOPIC}:{partition_for('telemetry/SITE01/asset/PUMP-101', 4)}"
    assert await redis.xlen(invalid_stream) == 1  # malformed message went to the dead-letter topic
    await db.compute_rollups(pool, start - timedelta(minutes=1), datetime.now(UTC))

    app = create_app(pool, redis)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as api:
        assert (await api.get("/health/ready")).json()["status"] == "ready"
        sources = (await api.get("/api/sources")).json()
        assert [s["sourceId"] for s in sources] == ["PUMP-101"]
        latest = (await api.get("/api/sources/PUMP-101/latest")).json()
        assert latest["kind"] == "asset" and latest["metrics"]["vibration_mm_s"]["value"] == pytest.approx(8.7)
        raw = (
            await api.get(
                "/api/sources/PUMP-101/series",
                params={
                    "metric": "vibration_mm_s",
                    "resolution": "raw",
                    "start": (start - timedelta(minutes=1)).isoformat(),
                },
            )
        ).json()
        assert len(raw) == 8
        rollup = (
            await api.get(
                "/api/sources/PUMP-101/series",
                params={"metric": "vibration_mm_s", "start": (start - timedelta(minutes=1)).isoformat()},
            )
        ).json()
        assert sum(b["count"] for b in rollup) == 8
        alerts = (await api.get("/api/alerts", params={"state": "open"})).json()
        assert len(alerts) == 1 and alerts[0]["rule_id"] == "vib-high"
        ack = await api.post(f"/api/alerts/{alerts[0]['id']}/ack", json={"by": "operator-1"})
        assert ack.status_code == 200 and ack.json()["state"] == "acknowledged"
        assert (await api.post(f"/api/alerts/{alerts[0]['id']}/ack", json={"by": "x"})).status_code == 409
        assert (await api.get("/api/sources/NOPE/latest")).status_code == 404
        assert (await api.get("/api/sources/PUMP-101/series", params={"metric": "Bad!"})).status_code == 422
        assert "telemetry_processed_total" in (await api.get("/metrics")).text


async def test_replayed_batches_are_idempotent(redis, pool):
    bus, processor = make_processor(redis, pool)
    t0 = datetime.now(UTC) - timedelta(minutes=5)
    readings = [
        Reading(
            site="S",
            source_id="P1",
            kind=SourceKind.ASSET,
            metric="vibration_mm_s",
            value=9.0,
            ts=t0 + timedelta(seconds=15 * i),
        )
        for i in range(4)
    ]
    await bus.publish(TELEMETRY_TOPIC, [(r.source_id, r.model_dump_json().encode()) for r in readings])
    batch = await bus.read(TELEMETRY_TOPIC, 100, block_ms=100)
    first = await processor.process_batch(batch)
    # Simulate redelivery after a crash: a fresh processor (empty rule state) sees the same messages again.
    _, replay_processor = make_processor(redis, pool)
    second = await replay_processor.process_batch(batch)
    assert [e.transition for e in first] == ["raised"]
    assert [e.transition for e in second] == ["raised"]  # evaluated again, but...
    assert await pool.fetchval("SELECT count(*) FROM readings") == 4
    assert await pool.fetchval("SELECT count(*) FROM alerts") == 1  # ...the episode is not duplicated


async def test_stale_service_alert_lifecycle(redis, pool):
    bus, processor = make_processor(redis, pool, stale=timedelta(seconds=30))
    now = datetime.now(UTC)
    up = Reading(site="platform", source_id="orders-api", kind=SourceKind.SERVICE, metric="up", value=1, ts=now)
    await bus.publish(TELEMETRY_TOPIC, [(up.source_id, up.model_dump_json().encode())])
    await drain(bus, processor)
    raised = await processor.check_stale(now + timedelta(seconds=45))
    assert [(e.rule_id, e.severity) for e in raised] == [("source-stale", Severity.CRITICAL)]
    assert await pool.fetchval("SELECT state FROM alerts WHERE rule_id = 'source-stale'") == "open"
    back = up.model_copy(update={"ts": now + timedelta(seconds=60)})
    await bus.publish(TELEMETRY_TOPIC, [(back.source_id, back.model_dump_json().encode())])
    await drain(bus, processor)
    assert await pool.fetchval("SELECT state FROM alerts WHERE rule_id = 'source-stale'") == "resolved"


async def test_retention_deletes_only_old_raw_data(redis, pool):
    bus, processor = make_processor(redis, pool)
    now = datetime.now(UTC)
    rs = [
        Reading(site="S", source_id="H1", kind=SourceKind.HOST, metric="cpu_pct", value=10, ts=now - timedelta(days=d))
        for d in (0, 1, 8, 9)
    ]
    await db.insert_readings(pool, rs)
    assert await db.delete_older_than(pool, now - timedelta(days=7), batch=1) == 2
    assert await pool.fetchval("SELECT count(*) FROM readings") == 2
