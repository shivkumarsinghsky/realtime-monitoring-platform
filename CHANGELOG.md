# Changelog

All notable changes to this repository are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-10-02

### Added

- MQTT ingestion with validation, batching, back-pressure and a dead-letter topic.
- Event bus abstraction: Redis Streams (default), Kafka adapter, in-memory bus for tests.
- Processor: latest state in Redis, idempotent raw writes, 1-minute rollups, retention, rule engine with
  sustain/hysteresis, staleness detection, deduplicated alert episodes.
- FastAPI query API, alert acknowledgement, WebSocket live feed and a dependency-free dashboard.
- Service prober and telemetry simulator.
- Unit and integration tests (MQTT, Redis, PostgreSQL), Docker image, Compose environment, CI.
- Architecture document and ADR-001 to ADR-005.
