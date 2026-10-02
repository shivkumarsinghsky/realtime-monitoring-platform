"""Event bus abstraction: Redis Streams (default) or Kafka, plus an in-memory bus for tests.

Topics are split into partitions by hash(key); all readings of one source go to one partition, and each partition
is processed by exactly one consumer at a time, so per-source order and in-memory rule state are preserved
(ADR-002, ADR-003).
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class BusMessage:
    topic: str
    partition: int
    id: str
    key: str
    value: bytes


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


class Bus(Protocol):
    async def publish(self, topic: str, items: list[tuple[str, bytes]]) -> None:
        """Publish (key, value) pairs. Must not return before the broker accepted them."""

    async def read(self, topic: str, max_messages: int, block_ms: int) -> list[BusMessage]:
        """Read the next batch from the partitions owned by this consumer (pending first, then new)."""

    async def ack(self, messages: list[BusMessage]) -> None:
        """Mark messages processed. Unacked messages are redelivered after a restart (at-least-once)."""

    async def close(self) -> None: ...
