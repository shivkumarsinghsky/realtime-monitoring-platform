import os

import pytest
from redis.asyncio import Redis

from monitoring import db

REDIS_URL = os.environ.get("TEST_REDIS_URL")
DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
MQTT_HOST = os.environ.get("TEST_MQTT_HOST")

requires_infra = pytest.mark.skipif(
    not (REDIS_URL and DATABASE_URL), reason="TEST_REDIS_URL and TEST_DATABASE_URL not set"
)
requires_mqtt = pytest.mark.skipif(not MQTT_HOST, reason="TEST_MQTT_HOST not set")


@pytest.fixture
async def redis():
    r = Redis.from_url(REDIS_URL)
    await r.flushdb()
    yield r
    await r.aclose()


@pytest.fixture
async def pool():
    p = await db.connect(DATABASE_URL)
    async with p.acquire() as c:
        await c.execute("DROP TABLE IF EXISTS readings, readings_1m, alerts, schema_migrations CASCADE")
    await db.migrate(p)
    yield p
    await p.close()
