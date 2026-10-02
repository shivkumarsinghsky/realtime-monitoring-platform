# ADR-004: PostgreSQL for Raw Readings and 1-Minute Rollups

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

The platform needs recent raw data for troubleshooting, aggregated history for dashboards and trends, and
alert history — with idempotent writes under at-least-once delivery.

## Decision

- `readings` with primary key `(source_id, metric, ts)` and a BRIN index on `ts`; batch inserts via
  `INSERT … SELECT unnest(...) ON CONFLICT DO NOTHING`.
- `readings_1m` rollups **recomputed** from raw data for recent minutes (idempotent overwrite, safe to run from
  several processors; an advisory lock avoids duplicate work).
- Retention by batched deletes; daily partitions or TimescaleDB as the scale-out path.

## Alternatives Considered

- **TimescaleDB** — hypertables, compression and continuous aggregates; the natural next step, kept optional to
  run on plain PostgreSQL.
- **MongoDB time-series collections** — good write throughput and TTL; would add a second database next to the
  relational alert store.
- **InfluxDB / Prometheus** — purpose-built for metrics; less suited for alert workflows and relational queries.
- **Incremental rollups in the processor** — cheaper, but double-counts on redelivery.

## Trade-offs

Plain PostgreSQL needs partitioning before raw data reaches billions of rows; batched deletes create bloat that
vacuum must handle.

## Consequences

Every write path is replay-safe, which is what makes at-least-once delivery acceptable end-to-end.
