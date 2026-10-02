"""Ingestion service: MQTT → validate → batch → event bus.

Back-pressure: messages go into a bounded queue. When the bus is slow and the queue is full, `put` blocks, the
MQTT client stops reading, and the broker buffers (QoS 1, persistent session) instead of this process running
out of memory. Invalid messages never block the pipeline: they are counted and sent to a dead-letter topic.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time

import aiomqtt

from monitoring import metrics
from monitoring.bus import Bus
from monitoring.config import Settings
from monitoring.ingestion.parse import InvalidMessage, parse_message
from monitoring.logs import log_fields
from monitoring.models import Reading

log = logging.getLogger("monitoring.ingestion")

TELEMETRY_TOPIC = "telemetry"
INVALID_TOPIC = "telemetry.invalid"


class Ingestion:
    def __init__(self, settings: Settings, bus: Bus) -> None:
        self.settings = settings
        self.bus = bus
        self.queue: asyncio.Queue[Reading] = asyncio.Queue(maxsize=settings.ingest_queue_size)
        self.connected = False

    async def handle(self, topic: str, payload: bytes) -> None:
        try:
            readings = parse_message(topic, payload)
        except InvalidMessage as e:
            metrics.REJECTED.labels(reason=e.reason).inc()
            await self.bus.publish(
                INVALID_TOPIC,
                [
                    (
                        topic,
                        json.dumps(
                            {
                                "topic": topic,
                                "reason": str(e),
                                "payload": payload[:2000].decode("utf-8", "replace"),
                            }
                        ).encode(),
                    )
                ],
            )
            return
        for r in readings:
            await self.queue.put(r)  # blocks when full → back-pressure to the MQTT broker
        metrics.INGESTED.inc(len(readings))
        metrics.INGEST_QUEUE.set(self.queue.qsize())

    async def publisher(self) -> None:
        """Flush the queue to the bus every `ingest_flush_ms` or when a batch is full."""
        batch: list[Reading] = []
        deadline = time.monotonic() + self.settings.ingest_flush_ms / 1000
        while True:
            timeout = max(0.0, deadline - time.monotonic())
            with contextlib.suppress(TimeoutError):
                batch.append(await asyncio.wait_for(self.queue.get(), timeout))
            if batch and (len(batch) >= self.settings.ingest_batch_size or time.monotonic() >= deadline):
                await self._flush(batch)
                batch = []
            if time.monotonic() >= deadline:
                deadline = time.monotonic() + self.settings.ingest_flush_ms / 1000

    async def _flush(self, batch: list[Reading]) -> None:
        started = time.perf_counter()
        items = [(r.source_id, r.model_dump_json().encode()) for r in batch]
        for attempt in range(1, 6):
            try:
                await self.bus.publish(TELEMETRY_TOPIC, items)
                break
            except Exception:  # noqa: BLE001 - bus outage: retry with backoff, keep the batch
                log.warning("bus publish failed; retrying", extra=log_fields(attempt=attempt, size=len(batch)))
                await asyncio.sleep(min(0.2 * 2**attempt, 5))
        else:
            raise RuntimeError("event bus unavailable; exiting so the orchestrator restarts ingestion")
        metrics.PUBLISH_SECONDS.observe(time.perf_counter() - started)
        metrics.INGEST_QUEUE.set(self.queue.qsize())

    async def run(self) -> None:
        s = self.settings
        publisher = asyncio.create_task(self.publisher())
        try:
            while True:
                try:
                    async with aiomqtt.Client(
                        s.mqtt_host,
                        s.mqtt_port,
                        username=s.mqtt_username,
                        password=s.mqtt_password,
                        identifier=f"{s.service_name}-ingestion",
                        clean_session=False,
                    ) as client:
                        await client.subscribe(s.mqtt_topic, qos=1)
                        self.connected = True
                        log.info("subscribed", extra=log_fields(topic=s.mqtt_topic))
                        async for message in client.messages:
                            await self.handle(message.topic.value, bytes(message.payload))  # type: ignore[arg-type,unused-ignore]
                except aiomqtt.MqttError as e:
                    self.connected = False
                    log.warning("mqtt connection lost; reconnecting", extra=log_fields(error=str(e)))
                    await asyncio.sleep(2)
        finally:
            publisher.cancel()
