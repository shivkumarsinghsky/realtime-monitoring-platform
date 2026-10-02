from __future__ import annotations

import asyncio
from collections import defaultdict

from . import BusMessage, partition_for


class MemoryBus:
    """In-process bus with the same semantics as the real ones (partitions, ack, redelivery of unacked)."""

    def __init__(self, partitions: int = 4) -> None:
        self.partitions = partitions
        self._log: dict[str, list[BusMessage]] = defaultdict(list)
        self._delivered: dict[str, int] = defaultdict(int)
        self._unacked: dict[str, dict[str, BusMessage]] = defaultdict(dict)
        self._event = asyncio.Event()

    async def publish(self, topic: str, items: list[tuple[str, bytes]]) -> None:
        for key, value in items:
            msg_id = str(len(self._log[topic]))
            self._log[topic].append(BusMessage(topic, partition_for(key, self.partitions), msg_id, key, value))
        self._event.set()

    async def read(self, topic: str, max_messages: int, block_ms: int) -> list[BusMessage]:
        start = self._delivered[topic]
        batch = self._log[topic][start : start + max_messages]
        if not batch and block_ms:
            self._event.clear()
            try:
                await asyncio.wait_for(self._event.wait(), block_ms / 1000)
            except TimeoutError:
                return []
            batch = self._log[topic][start : start + max_messages]
        self._delivered[topic] = start + len(batch)
        for m in batch:
            self._unacked[topic][m.id] = m
        return batch

    async def ack(self, messages: list[BusMessage]) -> None:
        for m in messages:
            self._unacked[m.topic].pop(m.id, None)

    def unacked(self, topic: str) -> int:
        return len(self._unacked[topic])

    def all(self, topic: str) -> list[BusMessage]:
        return list(self._log[topic])

    async def close(self) -> None:
        return None
