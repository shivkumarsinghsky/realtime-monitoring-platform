"""Processor: event bus → latest state (Redis) + storage (PostgreSQL) + rules → alerts.

Per batch, in order: update latest state, write raw readings (idempotent), evaluate rules, persist and publish
alert transitions, then ack. A crash before the ack replays the batch; every step tolerates replay.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

import asyncpg
from pydantic import ValidationError

from monitoring import db, metrics
from monitoring.bus import Bus, BusMessage
from monitoring.logs import log_fields
from monitoring.models import AlertEvent, Reading
from monitoring.processing.rules import RuleEngine
from monitoring.processing.state import LatestState

log = logging.getLogger("monitoring.processor")

TELEMETRY_TOPIC = "telemetry"
ALERTS_TOPIC = "alerts"


class Processor:
    def __init__(
        self,
        bus: Bus,
        pool: asyncpg.Pool,
        state: LatestState,
        engine: RuleEngine,
        batch_size: int = 500,
        retention: timedelta = timedelta(days=7),
    ) -> None:
        self.bus = bus
        self.pool = pool
        self.state = state
        self.engine = engine
        self.batch_size = batch_size
        self.retention = retention
        self.last_batch_at = time.monotonic()

    async def process_batch(self, messages: list[BusMessage]) -> list[AlertEvent]:
        started = time.perf_counter()
        readings: list[Reading] = []
        for m in messages:
            try:
                readings.append(Reading.model_validate_json(m.value))
            except ValidationError:
                log.error("undecodable bus message skipped", extra=log_fields(id=m.id, partition=m.partition))
        readings.sort(key=lambda r: r.ts)  # rules need per-source time order within a batch
        await self.state.update(readings)
        metrics.STORED.inc(await db.insert_readings(self.pool, readings))
        events: list[AlertEvent] = []
        for r in readings:
            events.extend(self.engine.evaluate(r))
        await self._emit(events)
        await self.bus.ack(messages)
        now = datetime.now(UTC)
        for r in readings:
            metrics.END_TO_END_SECONDS.observe(max(0.0, (now - r.ts).total_seconds()))
        metrics.PROCESSED.inc(len(readings))
        metrics.BATCH_SECONDS.observe(time.perf_counter() - started)
        self.last_batch_at = time.monotonic()
        return events

    async def check_stale(self, now: datetime | None = None) -> list[AlertEvent]:
        events = self.engine.check_stale(now or datetime.now(UTC))
        await self._emit(events)
        return events

    async def _emit(self, events: list[AlertEvent]) -> None:
        for e in events:
            changed = await db.apply_alert(self.pool, e)
            metrics.ALERTS.labels(rule=e.rule_id, transition=e.transition).inc()
            if changed:  # only notify on real state changes (dedup across replays)
                await self.bus.publish(ALERTS_TOPIC, [(e.source_id, e.model_dump_json().encode())])
                await self.state.publish_alert(e)
                log.info(
                    "alert %s",
                    e.transition,
                    extra=log_fields(rule=e.rule_id, source=e.source_id, severity=e.severity),
                )

    async def run(self) -> None:
        maintenance = asyncio.create_task(self._maintenance_loop())
        try:
            while True:
                messages = await self.bus.read(TELEMETRY_TOPIC, self.batch_size, block_ms=1000)
                if messages:
                    await self.process_batch(messages)
                else:
                    self.last_batch_at = time.monotonic()
        finally:
            maintenance.cancel()

    async def _maintenance_loop(self) -> None:
        """Every 10 s: staleness checks. Every 60 s: recompute recent rollups and apply retention."""
        tick = 0
        while True:
            await asyncio.sleep(10)
            tick += 1
            try:
                await self.check_stale()
                if tick % 6 == 0:
                    now = datetime.now(UTC)
                    await db.compute_rollups(self.pool, now - timedelta(minutes=10), now)
                    await db.delete_older_than(self.pool, now - self.retention)
            except Exception:
                log.exception("maintenance task failed")
