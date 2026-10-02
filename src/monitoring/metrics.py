from prometheus_client import Counter, Gauge, Histogram

INGESTED = Counter("telemetry_ingested_total", "Readings accepted at ingestion")
REJECTED = Counter("telemetry_rejected_total", "Messages rejected at ingestion", ["reason"])
INGEST_QUEUE = Gauge("ingest_queue_depth", "Readings waiting to be published to the bus")
PUBLISH_SECONDS = Histogram("bus_publish_seconds", "Time to publish one batch to the bus")
PROCESSED = Counter("telemetry_processed_total", "Readings processed by the processor")
STORED = Counter("telemetry_stored_total", "Readings newly written to PostgreSQL (duplicates excluded)")
ALERTS = Counter("alerts_total", "Alert transitions", ["rule", "transition"])
BATCH_SECONDS = Histogram(
    "processor_batch_seconds",
    "Processing time per batch",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
END_TO_END_SECONDS = Histogram(
    "telemetry_end_to_end_seconds",
    "Device timestamp to processed (includes device clock skew)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
