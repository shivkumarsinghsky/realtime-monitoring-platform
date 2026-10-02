# ADR-005: One Pipeline for Asset, Host and Service Monitoring

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Operations teams usually monitor equipment (IoT), infrastructure (hosts) and applications (services) with three
separate tools, so correlating "the pump alarm started when the gateway host ran out of disk" is manual.

## Decision

Treat everything as a **source** with a `kind` (`asset`, `host`, `service`) publishing readings on the same MQTT
topic scheme. Service health is turned into telemetry by a prober (`up`, `latency_ms`). Rules can target kinds;
staleness applies to all.

## Alternatives Considered

- **Separate stacks** (Prometheus for hosts/services, an IoT platform for assets) — best-of-breed features, no
  shared alerting or dashboard.
- **Pull-based scraping for hosts/services** — standard for Prometheus; would need a second ingestion path.

## Trade-offs

Rich APM features (traces, profiles) are out of scope; this covers health and key metrics.

## Consequences

One storage model, one rule engine, one alert workflow and one dashboard for all three kinds.
