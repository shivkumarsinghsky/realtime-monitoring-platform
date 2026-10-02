# ADR-001: MQTT Ingestion With Back-Pressure and a Separate Event Bus

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Devices and gateways speak MQTT: lightweight, works over poor networks, supports QoS and persistent sessions. An
MQTT broker, however, is not a durable, replayable buffer for downstream processing, and processing speed varies
(database latency, rule evaluation).

## Decision

- Devices publish to `telemetry/{site}/{kind}/{sourceId}` with QoS 1.
- A stateless **ingestion** service subscribes via a **shared subscription** (`$share/ingestion/...`), validates
  and normalises messages, and publishes batches to an **event bus** keyed by source id.
- Ingestion uses a **bounded queue**; when full it blocks, which stops reading from MQTT and lets the broker buffer
  (persistent session) instead of dropping data or exhausting memory.
- Invalid messages go to a dead-letter topic with a reason.

## Alternatives Considered

- **Processors subscribe to MQTT directly** — fewer hops, but no replay, no partition ownership, and slow
  processing directly pressures the broker.
- **Drop on overload** — protects the service but silently loses data; acceptable only for explicitly lossy metrics.
- **Device-to-HTTP ingestion** — simpler infrastructure, but heavier for constrained devices and no QoS semantics.

## Trade-offs

An extra hop adds a few milliseconds and another system to operate.

## Consequences

Ingestion and processing scale independently; the bus absorbs bursts; the dead-letter topic makes bad devices
visible.
