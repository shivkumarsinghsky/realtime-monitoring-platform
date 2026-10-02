"""Latest-value state in Redis, plus live fan-out to dashboards via Redis pub/sub."""

from __future__ import annotations

import json
from collections.abc import Sequence

from redis.asyncio import Redis

from monitoring.models import AlertEvent, Reading

LIVE_CHANNEL = "live:readings"
ALERT_CHANNEL = "live:alerts"
SOURCES_KEY = "sources"  # sorted set: source_id -> last seen epoch seconds


def latest_key(source_id: str) -> str:
    return f"latest:{source_id}"


class LatestState:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def update(self, readings: Sequence[Reading]) -> None:
        """Keep only the newest value per (source, metric); publish one live message per batch."""
        if not readings:
            return
        newest: dict[tuple[str, str], Reading] = {}
        for r in readings:
            k = (r.source_id, r.metric)
            if k not in newest or r.ts > newest[k].ts:
                newest[k] = r
        pipe = self.redis.pipeline(transaction=False)
        for r in newest.values():
            pipe.hset(
                latest_key(r.source_id),
                r.metric,
                json.dumps({"value": r.value, "unit": r.unit, "ts": r.ts.isoformat()}),
            )
            pipe.hset(latest_key(r.source_id), "_meta", json.dumps({"site": r.site, "kind": r.kind.value}))
            pipe.zadd(SOURCES_KEY, {r.source_id: r.ts.timestamp()}, gt=True)
        pipe.publish(
            LIVE_CHANNEL,
            json.dumps(
                [
                    {"source": r.source_id, "metric": r.metric, "value": r.value, "ts": r.ts.isoformat()}
                    for r in newest.values()
                ]
            ),
        )
        await pipe.execute()

    async def publish_alert(self, event: AlertEvent) -> None:
        await self.redis.publish(ALERT_CHANNEL, event.model_dump_json())

    async def latest(self, source_id: str) -> dict[str, object] | None:
        raw = await self.redis.hgetall(latest_key(source_id))
        if not raw:
            return None
        decoded = {_text(k): json.loads(v) for k, v in raw.items()}
        meta = decoded.pop("_meta", {})
        return {"sourceId": source_id, **meta, "metrics": decoded}

    async def sources(self) -> list[dict[str, object]]:
        rows = await self.redis.zrange(SOURCES_KEY, 0, -1, withscores=True)
        return [{"sourceId": _text(row[0]), "lastSeen": float(row[1])} for row in rows]


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
