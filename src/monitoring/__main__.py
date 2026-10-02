"""Entry point: python -m monitoring <ingestion|processor|api|prober|simulator|migrate>"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import timedelta

from monitoring.config import Settings, get_settings
from monitoring.logs import configure_logging


def build_bus(s: Settings, consuming: bool):  # type: ignore[no-untyped-def]
    if s.bus == "kafka":
        from monitoring.bus.kafka import KafkaBus

        return KafkaBus(s.kafka_bootstrap, s.processor_group, ["telemetry"] if consuming else [])
    from redis.asyncio import Redis

    from monitoring.bus.redis_streams import RedisStreamsBus

    index = int(os.environ.get("PROCESSOR_INDEX", "0"))
    count = int(os.environ.get("PROCESSOR_COUNT", "1"))
    return RedisStreamsBus(
        Redis.from_url(s.redis_url),
        s.partitions,
        s.processor_group,
        s.consumer_name,
        index=index,
        count=count,
    )


async def run_ingestion(s: Settings) -> None:
    from monitoring.ingestion.service import Ingestion

    await Ingestion(s, build_bus(s, consuming=False)).run()


async def run_processor(s: Settings) -> None:
    from redis.asyncio import Redis

    from monitoring import db
    from monitoring.processing.rules import RuleEngine, load_rules
    from monitoring.processing.service import Processor
    from monitoring.processing.state import LatestState

    pool = await db.connect(s.database_url)
    await db.migrate(pool)
    engine = RuleEngine(
        load_rules(os.environ.get("RULES_FILE", "config/rules.json")),
        timedelta(seconds=s.stale_after_seconds),
    )
    processor = Processor(
        build_bus(s, consuming=True),
        pool,
        LatestState(Redis.from_url(s.redis_url)),
        engine,
        s.processor_batch_size,
        timedelta(days=s.raw_retention_days),
    )
    await processor.run()


async def run_api(s: Settings) -> None:
    import uvicorn
    from redis.asyncio import Redis

    from monitoring import db
    from monitoring.api.app import create_app

    pool = await db.connect(s.database_url)
    app = create_app(pool, Redis.from_url(s.redis_url))
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=s.http_port, log_config=None))
    await server.serve()


async def run_migrate(s: Settings) -> None:
    from monitoring import db

    pool = await db.connect(s.database_url)
    print("applied:", await db.migrate(pool) or "nothing")
    await pool.close()


def main() -> None:
    component = sys.argv[1] if len(sys.argv) > 1 else ""
    if component == "simulator":
        from monitoring.simulator import main as sim

        sim(sys.argv[2:])
        return
    s = get_settings()
    configure_logging(f"{s.service_name}-{component}", s.log_level)
    runners = {
        "ingestion": run_ingestion,
        "processor": run_processor,
        "api": run_api,
        "migrate": run_migrate,
    }
    if component == "prober":
        from monitoring.prober import run as run_prober

        asyncio.run(run_prober(s))
        return
    if component not in runners:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    asyncio.run(runners[component](s))


if __name__ == "__main__":
    main()
