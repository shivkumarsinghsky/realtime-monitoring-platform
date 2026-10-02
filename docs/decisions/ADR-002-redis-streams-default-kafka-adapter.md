# ADR-002: Redis Streams by Default, Kafka Adapter for Scale

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

The bus between ingestion and processing must be durable enough for restarts, partitioned for per-source order,
and acknowledged after processing. Redis is already required for latest-value state and live fan-out.

## Decision

Define a small `Bus` interface (`publish`, `read`, `ack`) with three implementations:

- **Redis Streams** (default): one stream per partition, consumer groups, pending-entry redelivery, approximate
  length trimming.
- **Kafka** (`BUS=kafka`, `kafka` extra): partitions keyed by source id, manual offset commits after processing,
  idempotent producer with `acks=all`.
- **In-memory** for unit tests.

## Alternatives Considered

| Option | Strengths | Weaknesses |
|---|---|---|
| Redis Streams | Already deployed; low latency; simple operations | Memory-bound retention; no tiered storage |
| Kafka | Very high throughput, long retention, replay, ecosystem (Connect, ksqlDB) | Heavier to operate; partition rebalances move state |
| RabbitMQ | Flexible routing, per-message retry | Weaker fit for ordered, replayable high-volume streams |
| MQTT broker only | Simplest | Not a durable processing log |

## Trade-offs

Redis Streams retention is limited by memory (`max_len` trimming), so long replay windows require Kafka. Two
adapters must be kept behaviourally equivalent.

## Consequences

Small deployments run with three infrastructure services (MQTT, Redis, PostgreSQL). Moving to Kafka is a
configuration change. The Redis adapter has integration tests; the Kafka adapter is not yet covered by automated tests
(it can be run with the `kafka` compose profile and `BUS=kafka`).
