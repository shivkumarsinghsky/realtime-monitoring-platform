# Real-Time Monitoring Platform — MQTT, Event Streaming, Alerting and Live Dashboards

[![CI](https://github.com/shivkumarsinghsky/realtime-monitoring-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/shivkumarsinghsky/realtime-monitoring-platform/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![MQTT](https://img.shields.io/badge/MQTT-Mosquitto-660066)
![Redis](https://img.shields.io/badge/Redis-Streams-dc382d)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![License](https://img.shields.io/badge/license-MIT-green)

A **real-time monitoring** reference platform by **Shiv Kumar** for IoT assets, hosts and services. Telemetry
arrives over **MQTT** and is validated and batched onto an **event bus** (Redis Streams by default, Kafka
optional). Partitioned processors keep the **latest state in Redis**, store raw data and rollups in
**PostgreSQL**, and evaluate **alert rules**. Results are served through a **FastAPI** query API, a **WebSocket**
live feed and a small dashboard.

The focus is on what makes telemetry pipelines hard: back-pressure, per-source ordering, at-least-once delivery
with idempotent storage, alert deduplication, detecting sources that go silent, and keeping volume and
cardinality under control.

> Portfolio / reference implementation; the numbers in the docs are design targets and estimates, not benchmarks.

## Architecture

```mermaid
flowchart TB
    Assets["IoT assets<br/>vibration, temperature"] -->|"MQTT"| Broker["MQTT broker"]
    Hosts["Host agents<br/>cpu, memory, disk"] -->|"MQTT"| Broker
    Prober["Service prober<br/>up, latency"] -->|"MQTT"| Broker
    Broker --> Ingest["Ingestion service<br/>validate, normalise, batch"]
    Ingest --> Bus[["Event bus<br/>partitioned by source"]]
    Bus --> Proc["Processing<br/>rules, state, storage"]
    Proc --> Alerts["Alerts<br/>episodes, ack"]
    Proc --> Storage[("Storage<br/>PostgreSQL raw + rollups")]
    Proc --> Metrics[("Latest state<br/>Redis")]
    Alerts --> Dash["Dashboard<br/>REST + WebSocket"]
    Storage --> Dash
    Metrics --> Dash
```

Details — topics, processing sequence, delivery guarantees, back-pressure, failure handling, scaling to high
volume: [docs/architecture.md](docs/architecture.md).

## Key Capabilities

- **MQTT ingestion** with topic/payload validation, clock-skew and size checks, a **dead-letter topic**, batching,
  and **back-pressure** to the broker instead of dropping data.
- **Event bus abstraction**: Redis Streams with consumer groups and pending-entry recovery; Kafka adapter; in-memory
  bus for tests.
- **Partitioned processing**: per-source ordering and in-memory rule state via exclusive partition ownership.
- **Telemetry processing**: latest values in Redis, idempotent raw writes, 1-minute rollups, retention.
- **Alerting**: threshold rules with sustain windows and hysteresis, **staleness detection** for silent sources,
  one alert per episode (database-enforced), acknowledgement workflow.
- **Asset, host and service monitoring** in one pipeline; a prober turns health checks into telemetry.
- **Real-time dashboard**: WebSocket feed from Redis pub/sub, live tables with sparklines, active alerts with ack.
- **Health checks** and **Prometheus metrics** on the API.

## Technology Stack

| Area | Choice |
|---|---|
| Language | Python 3.11+ (asyncio), typed with `mypy --strict` |
| Device protocol | MQTT 3.1.1/5 via `aiomqtt`; Eclipse Mosquitto broker |
| Event bus | Redis Streams (default) or Apache Kafka (`aiokafka`) |
| Latest state / fan-out | Redis hashes, sorted set, pub/sub |
| Storage | PostgreSQL (`asyncpg`), BRIN index, rollup table |
| API | FastAPI, Uvicorn, WebSocket |
| Validation / config | pydantic v2, pydantic-settings |
| Observability | Prometheus client, JSON logs |
| Tests | pytest, pytest-asyncio, httpx (unit + integration against MQTT, Redis, PostgreSQL) |

## Repository Structure

```text
realtime-monitoring-platform/
├── src/monitoring/
│   ├── ingestion/        # MQTT topic/payload parsing, ingestion service (batching, back-pressure)
│   ├── bus/              # Bus interface + Redis Streams, Kafka, in-memory adapters
│   ├── processing/       # rule engine, latest state (Redis), processor service
│   ├── api/              # FastAPI app, WebSocket, static dashboard
│   ├── migrations/       # PostgreSQL schema
│   ├── db.py             # writes, rollups, alerts, retention, queries
│   ├── models.py         # Reading, AlertEvent, ...
│   ├── prober.py         # service health → telemetry
│   ├── simulator.py      # demo / load generator
│   └── __main__.py       # python -m monitoring <component>
├── config/rules.json     # alert rules
├── tests/unit/           # parsing, rules, ingestion, bus semantics, prober
├── tests/integration/    # Redis Streams bus, MQTT → API pipeline, replay idempotency, staleness, retention
├── docker/               # Dockerfile, mosquitto.conf
└── docs/                 # architecture, ADRs
```

## Getting Started

### Docker (everything)

```bash
git clone https://github.com/shivkumarsinghsky/realtime-monitoring-platform.git
cd realtime-monitoring-platform
docker compose up -d --build
open http://localhost:8000          # dashboard; PUMP-101 vibration rises and raises an alert after ~30 s
```

Compose runs Mosquitto, Redis, PostgreSQL, `ingestion`, `processor`, `api`, `prober` and a `simulator` that
injects an anomaly on `PUMP-101`.

### Components on the host

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
docker compose up -d mosquitto redis postgres
python -m monitoring migrate
python -m monitoring ingestion &
python -m monitoring processor &
python -m monitoring api &
python -m monitoring simulator --assets 5 --hosts 3 --interval 1 --anomaly PUMP-101
```

### Publishing your own telemetry

```bash
mosquitto_pub -h localhost -t telemetry/SITE01/asset/PUMP-201 -q 1 \
  -m '{"ts":"2026-10-01T10:00:00Z","metrics":{"vibration_mm_s":3.4,"bearing_temp_c":58.2},"units":{"vibration_mm_s":"mm/s"}}'
```

## Configuration

Environment variables (see [`.env.example`](.env.example)), validated by pydantic-settings:

| Variable | Purpose | Default |
|---|---|---|
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD` | Broker connection | `localhost:1883` |
| `MQTT_TOPIC` | Subscription (shared for scaling) | `$share/ingestion/telemetry/#` |
| `BUS` | `redis` or `kafka` | `redis` |
| `PARTITIONS` | Bus partitions (upper bound for processor count) | `4` |
| `PROCESSOR_INDEX`, `PROCESSOR_COUNT`, `CONSUMER_NAME` | Partition ownership for Redis Streams | `0`, `1`, `processor-1` |
| `REDIS_URL`, `DATABASE_URL`, `KAFKA_BOOTSTRAP` | Connections | local |
| `INGEST_QUEUE_SIZE`, `INGEST_BATCH_SIZE`, `INGEST_FLUSH_MS` | Back-pressure and batching | `10000`, `500`, `200` |
| `STALE_AFTER_SECONDS` | Silence before a staleness alert | `60` |
| `RAW_RETENTION_DAYS` | Raw data retention | `7` |
| `RULES_FILE` | Alert rules | `config/rules.json` |
| `PROBE_TARGETS` | `name=url,...` for the prober | — |

## API Examples

```http
GET  /api/sources                                         → [{ "sourceId": "PUMP-101", "lastSeen": 1790910400.7 }, ...]
GET  /api/sources/PUMP-101/latest                         → { "kind": "asset", "metrics": { "vibration_mm_s": { "value": 8.55, ... } } }
GET  /api/sources/PUMP-101/series?metric=vibration_mm_s&resolution=1m&start=2026-10-01T09:00:00Z
GET  /api/sources/PUMP-101/series?metric=vibration_mm_s&resolution=raw      (ranges up to 1 day)
GET  /api/alerts?state=open
POST /api/alerts/42/ack          { "by": "operator-1" }   → 200, or 409 if not open
WS   /ws/live                    → {"type":"readings","data":[...]} | {"type":"alert","data":{...}}
GET  /health/live | /health/ready | /metrics
```

## Testing

```bash
pytest tests/unit                       # no infrastructure required
ruff check . && ruff format --check . && mypy

docker compose up -d mosquitto redis postgres
TEST_DATABASE_URL=postgresql://monitoring:monitoring-local-dev@localhost:5432/monitoring \
TEST_REDIS_URL=redis://localhost:6379/15 TEST_MQTT_HOST=localhost \
pytest                                  # unit + integration
```

| Suite | Verifies |
|---|---|
| `unit/test_parse_and_models.py` | Topic parsing, payload validation (NaN, missing timezone, future timestamps, size) |
| `unit/test_rules.py` | Sustain, hysteresis, `<` rules, kind filters, per-source episodes, staleness, rule file |
| `unit/test_ingestion_and_bus.py` | Batching, dead-lettering, back-pressure, partitioning, prober |
| `integration/test_redis_streams_bus.py` | Publish/read/ack, redelivery after restart, non-overlapping partition ownership |
| `integration/test_pipeline.py` | MQTT → ingestion → bus → processor → Redis/PostgreSQL → API, alert raise and ack, replay idempotency, staleness lifecycle, retention |

Note: the Kafka adapter is not covered by automated tests yet.

## Docker

One image (`docker/Dockerfile`, non-root, multi-stage) runs every component: `ingestion`, `processor`, `api`,
`prober`, `simulator`, `migrate`. `docker compose --profile kafka up -d` adds a single-node Kafka (KRaft); set
`BUS=kafka` on ingestion and processor to use it.

## Architecture Decisions

| ADR | Decision |
|---|---|
| [ADR-001](docs/decisions/ADR-001-mqtt-ingestion-with-backpressure.md) | MQTT ingestion with back-pressure and a separate event bus |
| [ADR-002](docs/decisions/ADR-002-redis-streams-default-kafka-adapter.md) | Redis Streams by default, Kafka adapter for scale |
| [ADR-003](docs/decisions/ADR-003-partition-ownership-and-rule-state.md) | Partition ownership and in-memory rule state |
| [ADR-004](docs/decisions/ADR-004-postgresql-raw-and-rollups.md) | PostgreSQL raw readings and recomputed rollups |
| [ADR-005](docs/decisions/ADR-005-unified-asset-host-service-monitoring.md) | One pipeline for asset, host and service monitoring |

## Scalability Considerations

- **Horizontal scaling**: ingestion replicas share the MQTT load (shared subscription); processors scale up to the
  partition count; API replicas are stateless.
- **Partitioning** by source id preserves per-source order and keeps rule state local.
- **Batching** at every hop; one database statement and one pub/sub message per batch.
- **Storage tiers**: raw data for days, 1-minute rollups for long-term trends; partitioning or TimescaleDB
  beyond that.
- **Kafka** for higher throughput, longer retention and replay.

## Reliability

At-least-once delivery end-to-end, with idempotent storage, recomputed rollups and database-enforced alert
episodes. Back-pressure instead of data loss. A dead-letter topic for invalid input. Bus publish retries, then a
clean exit. MQTT reconnects with persistent sessions. Staleness alerts catch silent failures.

## Security

Strict payload validation (topic shape, metric names, finite values, timezone-aware timestamps within 5 minutes
of server time, size limits). The dashboard never injects telemetry as HTML. Containers run as non-root, and
configuration comes only from the environment. For production MQTT, use TLS, per-device credentials or client
certificates, and topic ACLs. **Not implemented here:** API authentication and authorization (see Future
improvements).

## Observability

JSON logs with correlation ids on API requests. Prometheus metrics for ingestion rate, rejections by reason,
queue depth, publish latency, processing time, end-to-end latency and alert transitions — with no per-source
labels, to bound cardinality. Readiness checks PostgreSQL and Redis.

## Future Improvements

Not implemented yet:

- API authentication (OIDC) and per-site authorization; MQTT TLS and ACLs in the default compose setup.
- Automated tests for the Kafka adapter; consumer-lag metrics.
- TimescaleDB hypertables with compression and continuous aggregates.
- Anomaly detection models alongside threshold rules; alert routing (email, Slack, on-call).
- Work request integration with EAM (see the EAM repository below).

## Related Projects

- [EAM Platform Architecture](https://github.com/shivkumarsinghsky/eam-platform-architecture) — condition-based maintenance: alerts to work requests
- [Event-Driven Platform](https://github.com/shivkumarsinghsky/event-driven-platform) — outbox, idempotent consumers, retries and DLQs
- [System Design Architecture](https://github.com/shivkumarsinghsky/system-design-architecture) — real-time monitoring system design and capacity model
- [Microservices Patterns](https://github.com/shivkumarsinghsky/microservices-patterns) — bulkhead, circuit breaker and observability patterns

## Author

**Shiv Kumar** — Senior Software Engineer / Software Architect
GitHub: [github.com/shivkumarsinghsky](https://github.com/shivkumarsinghsky)

## License

[MIT](LICENSE)
