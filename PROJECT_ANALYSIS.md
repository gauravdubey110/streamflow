# StreamFlow — Comprehensive Project Architecture & Technical Analysis

> **Document Version:** 1.0.0  
> **Target Project:** StreamFlow (Real-Time Live Streaming Analytics Platform)  
> **Tech Stack:** Java 17, Spring Boot 3.2.5, Apache Kafka, Redis 7.2, Apache Cassandra 4.1, React 19, TypeScript, Vite, Tailwind CSS, Zustand, Recharts, Resilience4j, Prometheus, Grafana, Docker.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [End-to-End System Architecture](#2-end-to-end-system-architecture)
3. [Module & Microservices Breakdown](#3-module--microservices-breakdown)
   - [3.1 `streamflow-common` (Domain Core & Contracts)](#31-streamflow-common-domain-core--contracts)
   - [3.2 `streamflow-producer` (Traffic & Telemetry Generator)](#32-streamflow-producer-traffic--telemetry-generator)
   - [3.3 `streamflow-processor` (Real-Time Aggregation & Alert Engine)](#33-streamflow-processor-real-time-aggregation--alert-engine)
   - [3.4 `streamflow-api` (Gateway & WebSocket Push Service)](#34-streamflow-api-gateway--websocket-push-service)
   - [3.5 `frontend` (Live React 19 Dashboard)](#35-frontend-live-react-19-dashboard)
4. [Data Storage & Persistence Architecture](#4-data-storage--persistence-architecture)
   - [4.1 Redis Hot-Tier In-Memory State](#41-redis-hot-tier-in-memory-state)
   - [4.2 Apache Cassandra Cold-Tier Time-Series Store](#42-apache-cassandra-cold-tier-time-series-store)
5. [Event Streaming & Messaging Topology](#5-event-streaming--messaging-topology)
6. [Resilience, Fault Tolerance & Chaos Engineering](#6-resilience-fault-tolerance--chaos-engineering)
   - [6.1 Resilience4j Circuit Breaker Integration](#61-resilience4j-circuit-breaker-integration)
   - [6.2 Chaos Injection Engine](#62-chaos-injection-engine)
7. [Observability & Telemetry](#7-observability--telemetry)
8. [Quality Assurance & Testing Strategy](#8-quality-assurance--testing-strategy)
9. [CI/CD & Containerization](#9-cicd--containerization)
10. [Developer Guide & Operational Commands](#10-developer-guide--operational-commands)

---

## 1. Executive Summary

**StreamFlow** is an enterprise-grade, distributed, real-time telemetry and streaming analytics platform designed to monitor live video broadcast performance at scale. The platform continuously ingests high-velocity viewer interactions (join, heartbeat, buffer, quality change, leave) alongside encoder health signals (bitrate, framerate, buffer rate, CPU load, dropped frames) at a rate exceeding **1,000+ Transactions Per Second (TPS)**.

### Key Capabilities:
- **Low-Latency Stream Aggregation:** Uses Redis sliding-window sorted sets and hashes to compute active concurrent viewers, video quality distributions, and composite stream health scores in real time (1-second cadence).
- **Automated Anomaly Detection & Alerting:** Evaluates rule-based degradation triggers (bitrate drops, buffer storms, sudden viewer churn) and dispatches real-time alerts.
- **Circuit Breaking & Fault Tolerance:** Employs Resilience4j to guard alert processing against cascading downstream failures with live state broadcast via WebSocket.
- **Controlled Chaos Engineering:** Simulates production failure modes (encoder crashes, packet loss/bitrate drops, buffer spikes, viewer dropouts) via REST APIs.
- **Dual-Tier Storage Architecture:** Combines low-latency in-memory state (Redis) with write-optimized, partition-bounded Cassandra time-series storage for long-term historical analytics and replay.
- **Real-Time Interactive Dashboard:** Built with React 19, TypeScript, Zustand, and STOMP-over-SockJS WebSockets, providing live updates, interactive charts, and historical scrubber replays.

---

## 2. End-to-End System Architecture

```
                                  ┌────────────────────────────────────────────────────────┐
                                  │                Synthetic Load Generation               │
                                  │       streamflow-producer (1,000+ TPS / Chaos)         │
                                  └───────────────┬────────────────────────┬───────────────┘
                                                  │                        │
                                   viewer-events  │                        │  stream-health
                                   (6 Partitions) │                        │  (3 Partitions)
                                                  ▼                        ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                   Kafka Event Bus (Broker / ZooKeeper Cluster)                           │
│   Topics: viewer-events (6) | stream-health (3) | metrics-aggregated (3) | alerts (3)   │
└─────────┬───────────────────────────────┬────────────────────────────────────────────────┘
          │                               │
          ▼                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                             streamflow-processor                                         │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────────────────┐  │
│  │ ViewerEventConsumer  │  │ StreamHealthConsumer │  │ AlertEngine (Rules + CB)       │  │
│  └──────────┬───────────┘  └──────────┬───────────┘  └───────────────┬────────────────┘  │
│             │                         │                              │                   │
│             ▼                         ▼                              ▼                   │
│      Redis (Hot State)         Redis (Hot State)            Kafka alerts / cb-events     │
│   • ZSET live viewers       • Hash stream health       • metrics-aggregated (1s tick)    │
│   • SMEMBERS active_streams • Snapshot caching         • Cassandra async persist         │
└───────────────────────────────────────┬──────────────────────────────────────────────────┘
                                        │
                                        │ (Kafka & Redis)
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                 streamflow-api                                           │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌──────────────────────────┐  │
│  │ REST Controllers        │  │ STOMP WebSocket Gateway │  │ Chaos Proxy Client       │  │
│  │ • /api/v1/streams       │  │ • /topic/metrics        │  │ • Proxies chaos requests │  │
│  │ • /api/v1/streams/..    │  │ • /topic/alerts         │  │   to producer:8081       │  │
│  │ • /history (Cassandra)  │  │ • /topic/cb-state       │  │                          │  │
│  └─────────────────────────┘  └─────────────────────────┘  └──────────────────────────┘  │
└───────────────────────────────────────┬──────────────────────────────────────────────────┘
                                        │
                                        │ HTTP REST & STOMP / WebSocket (SockJS)
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                          Frontend (React 19 + TypeScript + Vite)                         │
│  ┌──────────────────────────┐  ┌──────────────────────────┐  ┌────────────────────────┐  │
│  │ StreamGrid / StreamCard  │  │ AlertFeed / Badges       │  │ Chaos Control Panel    │  │
│  │ ViewerCountChart         │  │ CircuitBreakerIndicator  │  │ History Replay Modal   │  │
│  └──────────────────────────┘  └──────────────────────────┘  └────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Module & Microservices Breakdown

The backend is structured as a Maven multi-module project inheriting from `streamflow-parent` with Spring Boot 3.2.5 and Resilience4j managed via BOM imports.

```
streamflow/
├── backend/
│   ├── pom.xml                     # Parent POM (BOM dependency management, plugin config)
│   ├── streamflow-common/          # Shared domain records, enums, Kafka topic constants
│   ├── streamflow-producer/        # Ingestion load generator & internal chaos injector
│   ├── streamflow-processor/       # Redis sliding window, alert engine, snapshot scheduler
│   └── streamflow-api/             # REST API, STOMP WebSocket gateway, chaos proxy
├── frontend/                       # React 19 + TypeScript + Vite + Tailwind CSS + Zustand
├── infra/                          # Docker compose dev files, Kafka scripts, Cassandra CQL, Grafana
├── specs/                          # 21 Detailed engineering specifications (SPEC-01 to SPEC-21)
└── docker-compose.yml              # Production-grade full-stack orchestration
```

### 3.1 `streamflow-common` (Domain Core & Contracts)
- **Role:** Pure Java library shared across all backend services containing immutable contracts and constants.
- **Key Artifacts:**
  - **DTOs (Java 17 Records):**
    - `ViewerEventDTO`: `(eventId, streamId, viewerId, eventType, timestamp, quality, bufferDurationMs, region)`
    - `StreamHealthEventDTO`: `(streamId, timestamp, fps, bitrateKbps, bufferRatePct, cpuUsagePct, droppedFrames)`
    - `StreamMetricSnapshotDTO`: `(streamId, timestamp, liveViewerCount, healthScore, bufferRatePct, p95LatencyMs, qualityDistribution)`
    - `AlertEventDTO`: `(alertId, streamId, timestamp, severity, alertType, message, actualValue, resolvedAt)`
    - `CbStateEventDTO`: `(circuitBreakerName, state, timestamp, failureRate, slowCallRate)`
  - **Enums:** `EventType` (`JOIN`, `HEARTBEAT`, `BUFFER_START`, `BUFFER_END`, `QUALITY_CHANGE`, `LEAVE`), `VideoQuality` (`P1080`, `P720`, `P480`, `P360`), `AlertType` (`BITRATE_DROP`, `HIGH_BUFFER_RATE`, `VIEWER_DROP`, `ENCODER_CRASH`), `AlertSeverity` (`INFO`, `WARNING`, `CRITICAL`).
  - **Constants:** `KafkaTopics` centralizes topic names (`viewer-events`, `stream-health`, `metrics-aggregated`, `alerts`, `cb-events`).

### 3.2 `streamflow-producer` (Traffic & Telemetry Generator)
- **Port:** `8081`
- **Role:** Simulates high-throughput realistic viewer behaviour and stream encoder metrics across multiple concurrent live streams.
- **Architecture & Design Patterns:**
  - **Strategy Pattern:** `EventGenerationStrategy` with implementations:
    - `NormalLoadStrategy`: Emits realistic user flows (join, periodic 10s heartbeats, occasional buffering, resolution switching, leaves) targeting ~1,000 TPS total load.
    - `ChaosAwareStrategy`: Injects real-time behavioral anomalies when an active chaos scenario is configured.
  - **Simulators:** `StreamSimulator`, `ViewerEventProducer`, `StreamHealthProducer`.
  - **Internal Chaos Controller:** `POST /internal/chaos/start`, `DELETE /internal/chaos/{chaosId}`.

### 3.3 `streamflow-processor` (Real-Time Aggregation & Alert Engine)
- **Port:** `8082`
- **Role:** Consumes high-velocity raw telemetry from Kafka, executes sliding-window state aggregations in Redis, evaluates alert rules through a circuit breaker, and periodically broadcasts unified stream snapshots.
- **Key Components:**
  - **`ViewerEventConsumer`:**
    - Listens to `viewer-events` topic with manual acknowledgment (`AckMode.MANUAL_IMMEDIATE`).
    - Uses Redis Sorted Sets (`ZADD stream_viewers:{streamId} <timestamp> <viewerId>`) for sliding window computation.
    - Tracks active stream IDs via `SADD active_streams {streamId}`.
  - **`StreamHealthConsumer`:** Consumes encoder health events and updates Redis hash `stream_health:{streamId}`.
  - **`ViewerCountAggregator` & `QualityDistAggregator`:** Computes active viewer counts within a 60-second window and aggregates video resolution distributions.
  - **`HealthScoreCalculator`:** Computes a composite health index (0–100) using weighted metrics:
    $$\text{Health Score} = 100 - (\text{Bitrate Penalty} + \text{Buffer Rate Penalty} + \text{Frame Drop Penalty} + \text{CPU Load Penalty})$$
  - **`AlertEngine`:** Evaluates sliding window metrics against rules:
    - `BitrateDegradationRule`: Detects bitrate drops > 30% below baseline or < 2,500 Kbps.
    - `HighBufferRateRule`: Fires when stream-wide buffering exceeds 8% over a 30s window.
    - `ViewerDropRule`: Triggers when live viewer drop rate exceeds 20% within 15s.
  - **`SnapshotPublisher`:** Scheduled (`@Scheduled(fixedRate = 1000)`) hub that queries Redis state for all active streams, constructs `StreamMetricSnapshotDTO`, writes snapshot to Redis (`stream_snapshot:{streamId}`, TTL: 30s), publishes to Kafka `metrics-aggregated`, and rate-limits async persistence to Cassandra.
  - **`StaleViewerEvictionTask`:** Periodically purges viewer entries older than 60 seconds (`ZREMRANGEBYSCORE`).

### 3.4 `streamflow-api` (Gateway & WebSocket Push Service)
- **Port:** `8080`
- **Role:** Serves as the public gateway providing REST query endpoints and STOMP WebSocket push notifications.
- **REST Endpoints:**
  - `GET /api/v1/streams`: Fetches summary list of all active streams.
  - `GET /api/v1/streams/{streamId}`: Returns current metric snapshot for a stream (404 if inactive/expired).
  - `GET /api/v1/streams/{streamId}/history`: Queries minute- or hour-granularity historical metrics from Cassandra with weak ETag validation (`W/"..."`), `Cache-Control: max-age=30`, and max range restrictions.
  - `GET /api/v1/streams/{streamId}/alerts`: Queries historical alerts filtered by time range and severity.
  - `POST /api/v1/streams/{streamId}/chaos`: Injects chaos scenarios (proxied to `streamflow-producer`).
  - `DELETE /api/v1/streams/{streamId}/chaos/{chaosId}`: Cancels an active chaos scenario.
- **WebSocket Streaming (`WebSocketConfig`):**
  - Protocol: STOMP over SockJS at `/ws`.
  - Simple Broker Destinations:
    - `/topic/metrics`: Global broadcast of all stream snapshots.
    - `/topic/metrics/{streamId}`: Targeted per-stream snapshot feed.
    - `/topic/alerts`: Stream alert notifications.
    - `/topic/cb-state`: Real-time Circuit Breaker state transitions.
  - Transport limit: 64 KB buffer size to prevent message dropping under high throughput.

### 3.5 `frontend` (Live React 19 Dashboard)
- **Port:** `5173` (Vite Dev Server) / `3000` (Nginx Docker Production)
- **Tech Stack:** React 19, TypeScript (strict mode with `noUncheckedIndexedAccess`), Tailwind CSS, Zustand 5, Recharts 3, Axios, STOMPjs.
- **Key Modules & UI Components:**
  - **Layout & Stream Cards:** `StreamGrid`, `StreamCard`, `LiveDot`, `MetricCard`.
  - **Telemetry Visualizations:**
    - `ViewerCountChart`: Real-time streaming area chart showing viewer trend over 60s sliding window.
    - `HealthGauge`: Color-coded circular gauge displaying composite health score (Green > 85, Yellow 60-85, Red < 60).
    - `QualityDistBar`: Stacked resolution breakdown bar (1080p, 720p, 480p, 360p).
    - `BufferRateBadge`: Live buffer rate indicator with warning thresholds.
  - **Incident & Health Management:**
    - `AlertFeed` & `AlertBadge`: Real-time scrollable alert timeline with severity filtering.
    - `CircuitBreakerIndicator`: Dynamic badge showing circuit breaker state (`CLOSED`, `OPEN`, `HALF_OPEN`).
  - **Chaos Control Panel:**
    - `ChaosButton` & `StreamControls`: Interactive trigger for simulating faults (`VIEWER_DROP`, `BITRATE_SPIKE`, `HIGH_BUFFER`, `STREAM_DOWN`) with auto-revert countdown timer.
  - **Historical Replay:**
    - `HistoryModal` & `ReplayChart`: Interactive modal for querying Cassandra metrics up to 24 hours back with scrubbable timeline.
  - **State Management:** Zustand stores (`streamStore.ts`, `alertStore.ts`) updated via incoming STOMP frames and REST hydration.

---

## 4. Data Storage & Persistence Architecture

StreamFlow leverages a **hybrid polyglot storage architecture**: Redis for ultra-low latency sub-millisecond live state manipulation and Apache Cassandra for high-write-throughput time-series persistence.

```
                                      ┌──────────────────────────────────────────────┐
                                      │              StreamFlow Data Tier            │
                                      └───────┬──────────────────────────────┬───────┘
                                              │                              │
                                              ▼                              ▼
                             ┌─────────────────────────────────┐   ┌───────────────────────────────────┐
                             │       Redis 7.2 (Hot Tier)      │   │    Apache Cassandra 4.1 (Cold)    │
                             ├─────────────────────────────────┤   ├───────────────────────────────────┤
                             │ • active_streams (Set)          │   │ • viewer_events (Hourly Buckets)  │
                             │ • stream_viewers:* (Sorted Set) │   │ • metric_snapshots (Minute/Hour)  │
                             │ • stream_health:* (Hash)        │   │ • alerts (Daily Buckets)          │
                             │ • stream_snapshot:* (String JSON│   │                                   │
                             │ • cb_state:* (String)           │   │ Retention: 7 to 90 Days (TTL)     │
                             └─────────────────────────────────┘   └───────────────────────────────────┘
```

### 4.1 Redis Hot-Tier In-Memory State

| Key / Pattern | Data Structure | Purpose | Eviction / TTL |
|---|---|---|---|
| `active_streams` | **Set** (`SET`) | Registry of currently active stream IDs. | Managed dynamically by processor. |
| `stream_viewers:{streamId}` | **Sorted Set** (`ZSET`) | Score = Epoch Timestamp (ms), Value = `viewerId`. Used for exact concurrency calculation. | `ZREMRANGEBYSCORE` runs every 10s (window = 60s). |
| `stream_health:{streamId}` | **Hash** (`HSET`) | Latest encoder telemetry (fps, bitrate, dropped frames, CPU). | Overwritten on each health event. |
| `stream_snapshot:{streamId}`| **String** (`SET`) | Serialized `StreamMetricSnapshotDTO` JSON for instant REST lookup. | `TTL = 30 seconds`. |
| `cb_state:alert_processor`  | **String** | Mirror of current Resilience4j circuit breaker state (`CLOSED`, `OPEN`, `HALF_OPEN`). | Updated on state transition events. |

### 4.2 Apache Cassandra Cold-Tier Time-Series Store

Keyspace: `streamflow` (configured with `SimpleStrategy` RF=1 in dev, `NetworkTopologyStrategy` in prod).

#### Schema Definitions:
```sql
-- 1. Raw Time-Series Viewer Events (Partitioned by stream and hour to prevent unbounded partition growth)
CREATE TABLE IF NOT EXISTS streamflow.viewer_events (
  stream_id     TEXT,
  date_bucket   TEXT,          -- Format: 'YYYY-MM-DD-HH'
  timestamp     TIMESTAMP,
  event_id      UUID,
  viewer_id     TEXT,
  event_type    TEXT,
  quality       TEXT,
  buffer_ms     INT,
  region        TEXT,
  PRIMARY KEY ((stream_id, date_bucket), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 604800; -- 7 Days TTL

-- 2. Aggregated Metric Snapshots for Historical Replay
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
  AND default_time_to_live = 2592000; -- 30 Days TTL

-- 3. Historical Alert Log
CREATE TABLE IF NOT EXISTS streamflow.alerts (
  stream_id    TEXT,
  date_bucket  TEXT,           -- Format: 'YYYY-MM-DD'
  timestamp    TIMESTAMP,
  alert_id     UUID,
  severity     TEXT,
  alert_type   TEXT,
  message      TEXT,
  actual_value DOUBLE,
  resolved_at  TIMESTAMP,
  PRIMARY KEY ((stream_id, date_bucket), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 7776000; -- 90 Days TTL
```

---

## 5. Event Streaming & Messaging Topology

| Topic Name | Partitions | Replication Factor | Producer | Consumer(s) | Payload Contract |
|---|---|---|---|---|---|
| `viewer-events` | **6** | 1 (dev) / 3 (prod) | `streamflow-producer` | `streamflow-processor` (`ViewerEventConsumer`) | `ViewerEventDTO` (JSON) |
| `stream-health` | **3** | 1 (dev) / 3 (prod) | `streamflow-producer` | `streamflow-processor` (`StreamHealthConsumer`) | `StreamHealthEventDTO` (JSON) |
| `metrics-aggregated` | **3** | 1 (dev) / 3 (prod) | `streamflow-processor` | `streamflow-api` (`MetricsPushConsumer`) | `StreamMetricSnapshotDTO` (JSON) |
| `alerts` | **3** | 1 (dev) / 3 (prod) | `streamflow-processor` | `streamflow-api` (`AlertPushConsumer`), `streamflow-processor` (`AlertCassandraConsumer`) | `AlertEventDTO` (JSON) |
| `cb-events` | **3** | 1 (dev) / 3 (prod) | `streamflow-processor` | `streamflow-api` (`CircuitBreakerPushConsumer`) | `CbStateEventDTO` (JSON) |

### Reliability & Error Handling:
- **Manual Acknowledgment (`AckMode.MANUAL_IMMEDIATE`):** Kafka offsets are committed only after Redis/Cassandra updates successfully execute.
- **Dead-Letter-Topic (DLT) & Retry Backoff:** Failed messages undergo 3 retries with exponential backoff before being forwarded to `*.DLT` topics.

---

## 6. Resilience, Fault Tolerance & Chaos Engineering

### 6.1 Resilience4j Circuit Breaker Integration
- **Protected Target:** `AlertEngine` execution path.
- **Configuration:**
  - Sliding Window Type: Count-based (100 calls) or Time-based.
  - Failure Rate Threshold: `50%`.
  - Slow Call Rate Threshold: `50%` (Slow call duration > 500ms).
  - Wait Duration in Open State: `10 seconds`.
  - Permitted Number of Calls in Half-Open State: `10`.
- **State Synchronization:** State transitions emit `CircuitBreakerStateEvent`, which updates Redis key `cb_state:alert_processor` and publishes a `CbStateEventDTO` to Kafka `cb-events` for real-time frontend UI notification.

### 6.2 Chaos Injection Engine
StreamFlow allows engineers to test observability and alerting under adverse network and hardware conditions via the `ChaosInjector`:

| Chaos Scenario | Simulated Failure Mechanism | Observed System Reaction |
|---|---|---|
| `VIEWER_DROP` | Doubles the rate of `LEAVE` events in synthetic viewer streams. | `ViewerDropRule` alerts fire; UI charts show steep viewer drop. |
| `BITRATE_SPIKE` | Forces `bitrateKbps` to drop to 1,500 and frame drop rate to jump to 15%. | `BitrateDegradationRule` fires; HealthGauge drops to CRITICAL (<60). |
| `HIGH_BUFFER` | Increases `BUFFER_START` frequency from ~5% baseline to 12%+. | `HighBufferRateRule` fires; BufferRateBadge turns amber/red. |
| `STREAM_DOWN` | Completely halts event emission for the target stream. | Stream snapshot expires in Redis; stream moves to inactive state. |

---

## 7. Observability & Telemetry

StreamFlow features out-of-the-box observability configured via Micrometer, Spring Boot Actuator, Prometheus, and Grafana.

```
┌───────────────────────────────────────────────────────────┐
│              StreamFlow Observability Stack               │
└─────────────────────────────┬─────────────────────────────┘
                              │
         Scrapes /actuator/prometheus every 10s
                              ▼
┌───────────────────────────────────────────────────────────┐
│                       Prometheus                          │
│            (Scrapes: api:8080, processor:8082,            │
│                       producer:8081)                      │
└─────────────────────────────┬─────────────────────────────┘
                              │
                  PromQL Data Source Feed
                              ▼
┌───────────────────────────────────────────────────────────┐
│               Grafana (Port 3001: admin/admin)            │
│  • Pre-provisioned dashboards: StreamFlow Operations      │
│  • Panels: Ingestion TPS, Redis latency, Kafka lag,       │
│    Circuit Breaker state, JVM memory & GC pause times     │
└───────────────────────────────────────────────────────────┘
```

- **Actuator Endpoints:**
  - `/actuator/health`: Container health check with detailed component status (Kafka, Redis, Cassandra).
  - `/actuator/prometheus`: Micrometer-formatted metrics.
  - `/actuator/info`: Displays Git commit ID, build timestamp, and branch (via `git-commit-id-maven-plugin`).
- **Custom Micrometer Metrics:**
  - `streamflow.producer.events.generated`: Counter tagged by `streamId` and `eventType`.
  - `streamflow.processor.processing.time`: Timer for Redis sliding-window aggregations.
  - `streamflow.alerts.fired`: Counter tagged by `streamId`, `alertType`, and `severity`.
  - `streamflow.circuitbreaker.state`: Gauge tracking 0 (Closed), 1 (Half-Open), 2 (Open).

---

## 8. Quality Assurance & Testing Strategy

### Backend Test Matrix:
- **Unit Tests (`*Test.java`):** Fast, isolated tests mocking dependencies via Mockito (e.g. `ViewerCountAggregatorTest`, `AlertEngineTest`, `NormalLoadStrategyTest`).
- **Integration Tests (`*IT.java`):** Executed via `mvn verify` using **Testcontainers** to spin up ephemeral Kafka, Redis, and Cassandra instances in real Docker environments (e.g. `ViewerEventConsumerIT`, `SnapshotPublisherIT`, `HistoryServiceIT`).
- **Code Style Enforcement:** Checked and auto-formatted using **Spotless** and Google Java Format 1.22.0.

### Frontend Test Matrix:
- **Vitest & React Testing Library:** 104+ component and store tests covering state transitions, charts, WebSocket frame handling, and user interactions.
- **Strict TypeScript:** Compiled under strict mode with `noUncheckedIndexedAccess`.
- **Linting & Formatting:** ESLint v10 (flat config with `typescript-eslint` and `react-hooks`) and Prettier.

---

## 9. CI/CD & Containerization

### GitHub Actions Pipeline (`.github/workflows/ci.yml`):
1. **`backend` Job:** Runs `mvn spotless:check`, builds multi-module project, and executes full suite with Testcontainers (`mvn verify`).
2. **`frontend` Job:** Runs `npm run lint`, executes Vitest suite (`npm test`), and builds production bundle (`npm run build`).
3. **`docker` Job:** Triggered on main branch merges after backend/frontend jobs pass. Builds multi-stage Docker images and pushes to GitHub Container Registry (GHCR).

### Docker Compose Orchestration:
- **Full Stack (`docker-compose.yml`):**
  - Dependency order strictly enforced using health checks:  
    `zookeeper` ➔ `kafka` ➔ `kafka-init` ➔ `redis` ➔ `cassandra` ➔ `cassandra-init` ➔ `producer`/`processor`/`api` ➔ `frontend`/`prometheus`/`grafana`.
  - Automated database schema initialization (`init.cql` and Kafka topic creation).

---

## 10. Developer Guide & Operational Commands

### Local Development (Infra in Docker, Services Local):
```bash
# 1. Start backing services (Kafka, Redis, Cassandra, Prometheus, Grafana)
docker compose -f infra/docker-compose.dev.yml up -d

# 2. Initialize Kafka topics
./infra/kafka/init-topics.sh

# 3. Launch Backend Services
mvn -f backend/streamflow-producer/pom.xml spring-boot:run   # Port 8081
mvn -f backend/streamflow-processor/pom.xml spring-boot:run  # Port 8082
mvn -f backend/streamflow-api/pom.xml spring-boot:run        # Port 8080

# 4. Launch Frontend
cd frontend
npm install
npm run dev                                                  # Port 5173
```

### Full-Stack Single-Command Run:
```bash
# Build and run the entire platform in Docker
docker compose up --build

# Teardown and delete data volumes
docker compose down -v
```

### Verification & Testing Commands:
```bash
# Backend test suite (Unit + Testcontainers)
mvn -f backend/pom.xml verify

# Backend code formatting check / apply
mvn -f backend/pom.xml spotless:check
mvn -f backend/pom.xml spotless:apply

# Frontend test and lint
cd frontend
npm test
npm run lint
npm run build
```

### Port Map Summary:
| Service | Host Port | Protocol / Path | Description |
|---|---|---|---|
| **Frontend (Nginx / Vite)** | `3000` / `5173` | HTTP `/` | React Analytics Dashboard |
| **API Gateway** | `8080` | HTTP / REST `/api/v1/*`, STOMP `/ws` | REST API & WebSocket Push Gateway |
| **Producer Service** | `8081` | HTTP `/actuator`, `/internal/chaos/*` | Load Simulator & Chaos Endpoint |
| **Processor Service** | `8082` | HTTP `/actuator` | Real-Time Aggregator & Alert Engine |
| **Kafka Broker** | `9092` (host) / `29092` | PLAINTEXT | Event Bus |
| **Redis** | `6379` | RESP | Hot-Tier In-Memory State |
| **Cassandra** | `9042` | CQL | Cold-Tier Historical Storage |
| **Prometheus** | `9090` | HTTP `/` | Metrics Scraper & TSDB |
| **Grafana** | `3001` | HTTP `/` (admin/admin) | Operations & Monitoring Dashboard |

---

*Authored by Antigravity AI — Advanced Agentic Coding.*
