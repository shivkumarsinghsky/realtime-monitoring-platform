# ADR-003: Partition Ownership and In-Memory Rule State

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Alert rules are stateful per source (when did the breach start, is an episode open). Keeping that state in a
shared store adds a round-trip per reading; keeping it in memory is fast but only correct if all readings of a
source reach the same processor, in order.

## Decision

- Readings are keyed by source id, so a source always maps to one partition.
- Each partition has exactly one owning processor. With Redis Streams ownership is **static**: processor `i` of `N`
  owns partitions `p % N == i` (`PROCESSOR_INDEX`, `PROCESSOR_COUNT`), like a StatefulSet ordinal. With Kafka,
  the consumer group protocol assigns partitions.
- Rule state stays in memory; within a batch, readings are sorted by timestamp before evaluation.
- Alert episodes are persisted with a partial unique index, so a restarted processor that re-raises an alert does
  not create a duplicate.

## Alternatives Considered

- **Rule state in Redis** — survives restarts and rebalances; one extra round-trip per reading and per rule.
- **Redis consumer groups load-balancing across processors** — automatic scaling, but readings of one source are
  spread over processors and sustain/hysteresis logic breaks.

## Trade-offs

After a restart or rebalance, sustain timers start again: an alert may be delayed by up to one sustain window.
Static ownership requires changing `PROCESSOR_COUNT` together with the deployment size.

## Consequences

Rule evaluation needs no network calls; scaling processors is bounded by the partition count (default 4; raise it
before scaling out).
