from monitoring.bus import partition_for
from monitoring.bus.redis_streams import RedisStreamsBus

from .conftest import requires_infra

pytestmark = requires_infra


async def test_publish_read_ack(redis):
    bus = RedisStreamsBus(redis, partitions=4, consumer="c0")
    await bus.publish("t", [("PUMP-1", b"a"), ("PUMP-2", b"b"), ("PUMP-1", b"c")])
    batch = await bus.read("t", 10, block_ms=100)
    assert sorted(m.value for m in batch) == [b"a", b"b", b"c"]
    # per-key order within a partition is preserved
    assert [m.value for m in batch if m.key == "PUMP-1"] == [b"a", b"c"]
    await bus.ack(batch)
    assert await bus.read("t", 10, block_ms=50) == []


async def test_unacked_messages_are_redelivered_to_the_same_consumer_after_restart(redis):
    first = RedisStreamsBus(redis, partitions=2, consumer="c0")
    await first.publish("t", [("k1", b"x"), ("k2", b"y")])
    delivered = await first.read("t", 10, block_ms=100)
    assert len(delivered) == 2  # "crash" before ack
    restarted = RedisStreamsBus(redis, partitions=2, consumer="c0")
    again = await restarted.read("t", 10, block_ms=100)
    assert sorted(m.value for m in again) == [b"x", b"y"]
    await restarted.ack(again)
    assert await RedisStreamsBus(redis, partitions=2, consumer="c0").read("t", 10, block_ms=50) == []


async def test_static_partition_ownership_splits_work_without_overlap(redis):
    keys = [f"src-{i}" for i in range(40)]
    producer = RedisStreamsBus(redis, partitions=4)
    await producer.publish("t", [(k, k.encode()) for k in keys])
    a = RedisStreamsBus(redis, partitions=4, consumer="p0", index=0, count=2)
    b = RedisStreamsBus(redis, partitions=4, consumer="p1", index=1, count=2)
    got_a = {m.key for m in await a.read("t", 100, block_ms=100)}
    got_b = {m.key for m in await b.read("t", 100, block_ms=100)}
    assert got_a | got_b == set(keys)
    assert not got_a & got_b
    assert {partition_for(k, 4) % 2 for k in got_a} == {0}
