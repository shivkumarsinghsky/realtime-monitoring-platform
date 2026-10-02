-- Raw readings. Primary key makes writes idempotent under at-least-once delivery.
-- BRIN on ts is tiny and effective for append-mostly time-series data.
CREATE TABLE readings (
  source_id text             NOT NULL,
  metric    text             NOT NULL,
  ts        timestamptz      NOT NULL,
  site      text             NOT NULL,
  kind      text             NOT NULL,
  value     double precision NOT NULL,
  unit      text             NOT NULL DEFAULT '',
  PRIMARY KEY (source_id, metric, ts)
);
CREATE INDEX readings_ts_brin ON readings USING brin (ts);

-- 1-minute rollups for dashboards and long retention.
CREATE TABLE readings_1m (
  source_id    text             NOT NULL,
  metric       text             NOT NULL,
  bucket       timestamptz      NOT NULL,
  min_value    double precision NOT NULL,
  max_value    double precision NOT NULL,
  avg_value    double precision NOT NULL,
  sample_count integer          NOT NULL,
  PRIMARY KEY (source_id, metric, bucket)
);

CREATE TABLE alerts (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rule_id         text        NOT NULL,
  site            text        NOT NULL,
  source_id       text        NOT NULL,
  metric          text        NOT NULL,
  severity        text        NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
  state           text        NOT NULL CHECK (state IN ('open', 'acknowledged', 'resolved')),
  message         text        NOT NULL,
  last_value      double precision,
  opened_at       timestamptz NOT NULL,
  acknowledged_at timestamptz,
  acknowledged_by text,
  resolved_at     timestamptz
);
-- At most one active episode per rule and source: deduplicates across processors and redeliveries.
CREATE UNIQUE INDEX alerts_one_active_episode ON alerts (rule_id, source_id) WHERE state <> 'resolved';
CREATE INDEX alerts_state_opened ON alerts (state, opened_at DESC);
