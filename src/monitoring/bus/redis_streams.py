from __future__ import annotations

from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from . import BusMessage, partition_for


class RedisStreamsBus:
    """Redis Streams with one stream per partition: `{topic}:{partition}`.

    Partition ownership is static: consumer `index` of `count` owns partitions where p % count == index (like a
    StatefulSet ordinal). Within a stream a consumer group tracks delivery; on restart the consumer first re-reads
    its own pending (delivered but unacked) entries, then new ones. Streams are trimmed approximately to
    `max_len` entries for bounded memory.
    """

    def __init__(
        self,
        redis: Redis,
        partitions: int,
        group: str = "processors",
        consumer: str = "consumer-0",
        index: int = 0,
        count: int = 1,
        max_len: int = 1_000_000,
    ) -> None:
        if not 0 <= index < count:
            raise ValueError("index must be in [0, count)")
        self.redis = redis
        self.partitions = partitions
        self.group = group
        self.consumer = consumer
        self.owned = [p for p in range(partitions) if p % count == index]
        self.max_len = max_len
        self._groups_ready: set[str] = set()
        self._recovered: set[str] = set()

    @staticmethod
    def stream(topic: str, partition: int) -> str:
        return f"{topic}:{partition}"

    async def publish(self, topic: str, items: list[tuple[str, bytes]]) -> None:
        if not items:
            return
        pipe = self.redis.pipeline(transaction=False)
        for key, value in items:
            stream = self.stream(topic, partition_for(key, self.partitions))
            pipe.xadd(stream, {"k": key, "v": value}, maxlen=self.max_len, approximate=True)
        await pipe.execute()

    async def _ensure_groups(self, topic: str) -> None:
        if topic in self._groups_ready:
            return
        for p in self.owned:
            try:
                await self.redis.xgroup_create(self.stream(topic, p), self.group, id="0", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
        self._groups_ready.add(topic)

    async def read(self, topic: str, max_messages: int, block_ms: int) -> list[BusMessage]:
        await self._ensure_groups(topic)
        streams = {self.stream(topic, p): p for p in self.owned}
        # After (re)start, drain this consumer's pending entries first ("0"), then switch to new entries (">").
        start = ">" if topic in self._recovered else "0"
        response: Any = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {s: start for s in streams},
            count=max_messages,
            block=None if start == "0" else block_ms,
        )
        messages: list[BusMessage] = []
        for stream_name, entries in response or []:
            name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
            for entry_id, fields in entries:
                if not fields:  # pending entry that was trimmed away
                    continue
                messages.append(
                    BusMessage(
                        topic=topic,
                        partition=streams[name],
                        id=entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        key=fields[b"k"].decode(),
                        value=fields[b"v"],
                    )
                )
        if start == "0" and not messages:
            self._recovered.add(topic)
            return await self.read(topic, max_messages, block_ms)
        return messages

    async def ack(self, messages: list[BusMessage]) -> None:
        by_stream: dict[str, list[str]] = {}
        for m in messages:
            by_stream.setdefault(self.stream(m.topic, m.partition), []).append(m.id)
        if not by_stream:
            return
        pipe = self.redis.pipeline(transaction=False)
        for stream, ids in by_stream.items():
            pipe.xack(stream, self.group, *ids)
        await pipe.execute()

    async def close(self) -> None:
        return None
