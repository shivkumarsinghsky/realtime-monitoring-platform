"""PostgreSQL access: schema migration, raw reading writes, rollups, alerts, retention."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

from monitoring.models import AlertEvent, AlertState, Reading

MIGRATIONS = Path(__file__).resolve().parent / "migrations"


async def connect(url: str, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(url, min_size=min_size, max_size=max_size, command_timeout=30)
    assert pool is not None
    return pool


async def migrate(pool: asyncpg.Pool) -> list[str]:
    applied: list[str] = []
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(hashtext('monitoring-migrations'))")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz DEFAULT now())"
        )
        done = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
        for f in sorted(MIGRATIONS.glob("*.sql")):
            if f.name not in done:
                await conn.execute(f.read_text())
                await conn.execute("INSERT INTO schema_migrations (name) VALUES ($1)", f.name)
                applied.append(f.name)
    return applied


async def insert_readings(pool: asyncpg.Pool, readings: Sequence[Reading]) -> int:
    """Idempotent batch insert: redelivered messages hit the primary key and are ignored."""
    if not readings:
        return 0
    result: str = await pool.execute(
        """
        INSERT INTO readings (source_id, metric, ts, site, kind, value, unit)
        SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::text[],
                             $6::double precision[], $7::text[])
        ON CONFLICT DO NOTHING
        """,
        [r.source_id for r in readings],
        [r.metric for r in readings],
        [r.ts for r in readings],
        [r.site for r in readings],
        [r.kind.value for r in readings],
        [r.value for r in readings],
        [r.unit for r in readings],
    )
    return int(result.split()[-1])


async def compute_rollups(pool: asyncpg.Pool, since: datetime, until: datetime) -> int:
    """(Re)compute 1-minute rollups for complete minutes in [since, until). Idempotent: recomputing overwrites."""
    async with pool.acquire() as conn, conn.transaction():
        if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(hashtext('rollup-1m'))"):
            return 0  # another processor is computing rollups right now
        result: str = await conn.execute(
            """
            INSERT INTO readings_1m (source_id, metric, bucket, min_value, max_value, avg_value, sample_count)
            SELECT source_id, metric, date_trunc('minute', ts), min(value), max(value), avg(value), count(*)
              FROM readings
             WHERE ts >= date_trunc('minute', $1::timestamptz) AND ts < date_trunc('minute', $2::timestamptz)
             GROUP BY 1, 2, 3
            ON CONFLICT (source_id, metric, bucket) DO UPDATE
               SET min_value = EXCLUDED.min_value, max_value = EXCLUDED.max_value,
                   avg_value = EXCLUDED.avg_value, sample_count = EXCLUDED.sample_count
            """,
            since,
            until,
        )
    return int(result.split()[-1])


async def apply_alert(pool: asyncpg.Pool, e: AlertEvent) -> bool:
    """Open or resolve an alert episode. A partial unique index guarantees at most one open episode per
    (rule, source), so duplicates from redelivery or concurrent processors are absorbed. Returns True if the
    database state changed."""
    if e.transition == "raised":
        status: str = await pool.execute(
            """
            INSERT INTO alerts (rule_id, site, source_id, metric, severity, state, message, last_value, opened_at)
            VALUES ($1, $2, $3, $4, $5, 'open', $6, $7, $8)
            ON CONFLICT (rule_id, source_id) WHERE state <> 'resolved' DO NOTHING
            """,
            e.rule_id,
            e.site,
            e.source_id,
            e.metric,
            e.severity.value,
            e.message,
            e.value,
            e.at,
        )
    else:
        status = await pool.execute(
            """
            UPDATE alerts SET state = 'resolved', resolved_at = $3, last_value = $4
             WHERE rule_id = $1 AND source_id = $2 AND state <> 'resolved'
            """,
            e.rule_id,
            e.source_id,
            e.at,
            e.value,
        )
    return status.split()[-1] != "0"


async def list_alerts(pool: asyncpg.Pool, state: AlertState | None, limit: int) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        """
        SELECT id, rule_id, site, source_id, metric, severity, state, message, last_value,
               opened_at, acknowledged_at, acknowledged_by, resolved_at
          FROM alerts WHERE ($1::text IS NULL OR state = $1) ORDER BY opened_at DESC LIMIT $2
        """,
        state.value if state else None,
        limit,
    )
    return [dict(r) for r in rows]


async def acknowledge_alert(pool: asyncpg.Pool, alert_id: int, by: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        """
        UPDATE alerts SET state = 'acknowledged', acknowledged_at = now(), acknowledged_by = $2
         WHERE id = $1 AND state = 'open'
        RETURNING id, state, acknowledged_at, acknowledged_by
        """,
        alert_id,
        by,
    )
    return dict(row) if row else None


async def series(
    pool: asyncpg.Pool, source_id: str, metric: str, start: datetime, end: datetime, resolution: str
) -> list[dict[str, Any]]:
    if resolution == "raw":
        rows = await pool.fetch(
            "SELECT ts, value FROM readings WHERE source_id = $1 AND metric = $2 AND ts >= $3 AND ts < $4"
            " ORDER BY ts LIMIT 10000",
            source_id,
            metric,
            start,
            end,
        )
        return [{"ts": r["ts"], "value": r["value"]} for r in rows]
    rows = await pool.fetch(
        "SELECT bucket, min_value, max_value, avg_value, sample_count FROM readings_1m"
        " WHERE source_id = $1 AND metric = $2 AND bucket >= $3 AND bucket < $4 ORDER BY bucket LIMIT 10000",
        source_id,
        metric,
        start,
        end,
    )
    return [
        {
            "ts": r["bucket"],
            "min": r["min_value"],
            "max": r["max_value"],
            "avg": r["avg_value"],
            "count": r["sample_count"],
        }
        for r in rows
    ]


async def delete_older_than(pool: asyncpg.Pool, cutoff: datetime, batch: int = 50_000) -> int:
    """Retention in bounded batches so a large delete never holds long locks. (Daily partitions + DROP
    PARTITION is the scale-out option; see docs/architecture.md.)"""
    total = 0
    while True:
        status: str = await pool.execute(
            "DELETE FROM readings WHERE ctid = ANY(ARRAY(SELECT ctid FROM readings WHERE ts < $1 LIMIT $2))",
            cutoff,
            batch,
        )
        n = int(status.split()[-1])
        total += n
        if n < batch:
            return total
