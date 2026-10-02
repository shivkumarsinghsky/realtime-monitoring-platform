import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest

from monitoring.bus import partition_for
from monitoring.bus.memory import MemoryBus
from monitoring.config import Settings
from monitoring.ingestion.service import INVALID_TOPIC, TELEMETRY_TOPIC, Ingestion
from monitoring.prober import parse_targets, probe_once


def body(**metrics: float) -> bytes:
    return json.dumps({"ts": datetime.now(UTC).isoformat(), "metrics": metrics}).encode()


async def test_valid_messages_are_batched_to_the_bus_and_invalid_ones_dead_lettered():
    bus = MemoryBus()
    ing = Ingestion(Settings(ingest_flush_ms=20, ingest_batch_size=100), bus)
    publisher = asyncio.create_task(ing.publisher())
    await ing.handle("telemetry/S1/asset/P1", body(vibration_mm_s=3, bearing_temp_c=60))
    await ing.handle("telemetry/S1/asset/P2", body(vibration_mm_s=4))
    await ing.handle("telemetry/S1/robot/R1", body(x=1))
    await asyncio.sleep(0.1)
    publisher.cancel()
    assert len(bus.all(TELEMETRY_TOPIC)) == 3
    assert {m.key for m in bus.all(TELEMETRY_TOPIC)} == {"P1", "P2"}
    invalid = bus.all(INVALID_TOPIC)
    assert len(invalid) == 1 and json.loads(invalid[0].value)["reason"].startswith("bad_kind")


async def test_queue_applies_back_pressure_instead_of_dropping():
    ing = Ingestion(Settings(ingest_queue_size=100), MemoryBus())
    for i in range(100):
        await ing.handle(f"telemetry/S1/asset/P{i}", body(v=1))
    blocked = asyncio.create_task(ing.handle("telemetry/S1/asset/P100", body(v=1)))
    await asyncio.sleep(0.05)
    assert not blocked.done()  # waits for space rather than discarding data
    ing.queue.get_nowait()
    await asyncio.wait_for(blocked, 1)


async def test_memory_bus_redelivery_semantics():
    bus = MemoryBus(partitions=4)
    await bus.publish("t", [("a", b"1"), ("b", b"2")])
    batch = await bus.read("t", 10, 0)
    assert [m.value for m in batch] == [b"1", b"2"]
    assert bus.unacked("t") == 2
    await bus.ack(batch[:1])
    assert bus.unacked("t") == 1


def test_partitioning_is_stable_per_key():
    assert partition_for("PUMP-101", 8) == partition_for("PUMP-101", 8)
    assert len({partition_for(f"src-{i}", 8) for i in range(200)}) == 8


def test_probe_target_parsing():
    assert parse_targets("api=http://api:8000/health/live, db=https://x/y") == {
        "api": "http://api:8000/health/live",
        "db": "https://x/y",
    }
    with pytest.raises(ValueError):
        parse_targets("broken")


async def test_probe_reports_down_on_5xx_and_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ok":
            return httpx.Response(200)
        if request.url.path == "/fail":
            return httpx.Response(503)
        raise httpx.ConnectError("refused")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert (await probe_once(client, "http://svc/ok"))[0] == 1.0
        assert (await probe_once(client, "http://svc/fail"))[0] == 0.0
        assert (await probe_once(client, "http://svc/down"))[0] == 0.0
