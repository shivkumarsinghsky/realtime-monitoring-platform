from __future__ import annotations

from typing import Any

from . import BusMessage


class KafkaBus:
    """Kafka adapter (optional `kafka` extra). Partitions are assigned by Kafka's consumer group protocol; keys
    route all readings of a source to one partition. Offsets are committed manually after processing
    (at-least-once). After a rebalance, rule state for newly assigned partitions warms up again (ADR-003)."""

    def __init__(self, bootstrap: str, group: str, topics: list[str]) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=5, enable_idempotence=True)
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._started = False
        self._consuming = bool(topics)

    async def _start(self) -> None:
        if not self._started:
            await self._producer.start()
            if self._consuming:
                await self._consumer.start()
            self._started = True

    async def publish(self, topic: str, items: list[tuple[str, bytes]]) -> None:
        await self._start()
        futures = [await self._producer.send(topic, value=v, key=k.encode()) for k, v in items]
        for f in futures:
            await f  # wait for broker acknowledgement (acks=all)

    async def read(self, topic: str, max_messages: int, block_ms: int) -> list[BusMessage]:
        await self._start()
        batches: dict[Any, list[Any]] = await self._consumer.getmany(timeout_ms=block_ms, max_records=max_messages)
        return [
            BusMessage(r.topic, r.partition, str(r.offset), (r.key or b"").decode(), r.value)
            for records in batches.values()
            for r in records
            if r.topic == topic
        ]

    async def ack(self, messages: list[BusMessage]) -> None:
        if not messages:
            return
        from aiokafka import TopicPartition
        from aiokafka.structs import OffsetAndMetadata

        offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        for m in messages:
            tp = TopicPartition(m.topic, m.partition)
            nxt = int(m.id) + 1
            if tp not in offsets or offsets[tp].offset < nxt:
                offsets[tp] = OffsetAndMetadata(nxt, "")
        await self._consumer.commit(offsets)

    async def close(self) -> None:
        if self._started:
            await self._producer.stop()
            if self._consuming:
                await self._consumer.stop()
