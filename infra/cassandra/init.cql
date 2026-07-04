-- ─────────────────────────────────────────────────────────────────────────────
-- StreamFlow Cassandra Schema Init
-- SPEC-17: matches Project Plan §8 exactly.
--
-- Usage:
--   cqlsh -f init.cql
--   (or wired into a one-shot Docker init container — see docker-compose.dev.yml)
--
-- Keyspace uses SimpleStrategy RF=1 for local dev.
-- For production, switch to NetworkTopologyStrategy:
--   WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
-- ─────────────────────────────────────────────────────────────────────────────

CREATE KEYSPACE IF NOT EXISTS streamflow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- ── Time-series viewer events ─────────────────────────────────────────────────
-- Partition key: (stream_id, date_bucket) — hourly buckets keep partitions bounded.
-- Clustering: timestamp DESC for latest-first reads.
-- TTL: 7 days (604800 seconds).
CREATE TABLE IF NOT EXISTS streamflow.viewer_events (
  stream_id     TEXT,
  date_bucket   TEXT,
  timestamp     TIMESTAMP,
  event_id      UUID,
  viewer_id     TEXT,
  event_type    TEXT,
  quality       TEXT,
  buffer_ms     INT,
  region        TEXT,
  PRIMARY KEY ((stream_id, date_bucket), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 604800;

-- ── Minute-level metric snapshots for historical replay ───────────────────────
-- Partition key: stream_id (one partition per stream; bounded by minute_bucket clustering).
-- Clustering: minute_bucket DESC for latest-first reads.
-- TTL: 30 days (2592000 seconds).
CREATE TABLE IF NOT EXISTS streamflow.metric_snapshots (
  stream_id          TEXT,
  minute_bucket      TIMESTAMP,
  live_viewer_count  BIGINT,
  buffer_rate_pct    DOUBLE,
  p95_latency_ms     INT,
  health_score       DOUBLE,
  quality_1080p_pct  DOUBLE,
  quality_720p_pct   DOUBLE,
  quality_480p_pct   DOUBLE,
  quality_360p_pct   DOUBLE,
  PRIMARY KEY (stream_id, minute_bucket)
) WITH CLUSTERING ORDER BY (minute_bucket DESC)
  AND default_time_to_live = 2592000;

-- ── Alert history ─────────────────────────────────────────────────────────────
-- Partition key: (stream_id, date_bucket) — daily buckets.
-- Clustering: timestamp DESC for latest-first reads.
-- TTL: 90 days (7776000 seconds).
CREATE TABLE IF NOT EXISTS streamflow.alerts (
  stream_id    TEXT,
  date_bucket  TEXT,
  timestamp    TIMESTAMP,
  alert_id     UUID,
  severity     TEXT,
  alert_type   TEXT,
  message      TEXT,
  actual_value DOUBLE,
  resolved_at  TIMESTAMP,
  PRIMARY KEY ((stream_id, date_bucket), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 7776000;
