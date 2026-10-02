# StreamFlow — Local Setup & Execution Guide

This guide provides step-by-step instructions to configure, initialize, run, and verify the **StreamFlow** real-time streaming analytics platform on your local machine.

---

## Quick Reference & Port Allocation

| Component | Port | Description | Health / Entry URL |
|---|---|---|---|
| **Frontend Dashboard (Dev)** | `5173` | React 19 + Vite HMR Dev Server | [http://localhost:5173](http://localhost:5173) |
| **Frontend Dashboard (Docker)** | `3000` | Production Nginx container | [http://localhost:3000](http://localhost:3000) |
| **API Gateway** | `8080` | Spring Boot REST API & STOMP WebSocket | [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) |
| **Stream Processor** | `8082` | Kafka consumer, Redis aggregation, Alert Engine | [http://localhost:8082/actuator/health](http://localhost:8082/actuator/health) |
| **Telemetry Producer** | `8081` | 1,000+ TPS traffic simulator & chaos engine | [http://localhost:8081/actuator/health](http://localhost:8081/actuator/health) |
| **Apache Kafka** | `9092` (host) / `29092` | Event Bus Broker | `localhost:9092` |
| **Redis** | `6379` | In-memory hot state (Sorted sets, Hashes) | `redis-cli -p 6379 ping` |
| **Apache Cassandra** | `9042` | Time-series historical database | `cqlsh localhost 9042` |
| **Prometheus** | `9090` | Metrics scraper & TSDB | [http://localhost:9090](http://localhost:9090) |
| **Grafana** | `3001` | Operational telemetry dashboard | [http://localhost:3001](http://localhost:3001) (`admin` / `admin`) |

---

## 1. Prerequisites & Environment Setup

Ensure you have the following installed on your machine:

1. **Java Development Kit (JDK 17):**
   ```bash
   java -version
   # OpenJDK 17 or Oracle JDK 17
   ```
2. **Apache Maven 3.8+:**
   ```bash
   mvn -version
   ```
3. **Node.js (v20+ or v22+) & npm:**
   ```bash
   node -v
   npm -v
   ```
4. **Docker & Docker Compose (v2.20+):**
   - Ensure Docker Desktop (or Docker daemon) is running with at least **4 GB to 6 GB of RAM allocated** (Cassandra and Kafka require sufficient memory).
   ```bash
   docker --version
   docker compose version
   ```

---

## 2. Choosing Your Execution Mode

StreamFlow supports two workflows:

- **Mode A: Hybrid Local Development (Recommended for developers)**
  - Backing databases (Kafka, Zookeeper, Redis, Cassandra, Prometheus, Grafana) run in Docker.
  - Microservices (`producer`, `processor`, `api`) and the React frontend run natively on your machine for rapid development and debugging.
- **Mode B: Single-Command Full-Stack Docker Deployment**
  - The entire ecosystem (all 4 databases/tools + all 3 Spring Boot microservices + Nginx frontend) runs in Docker containers with dependency healthcheck ordering.

---

## 3. Mode A: Step-by-Step Local Development Setup

### Step 1: Start Infrastructure Containers (Kafka, Redis, Cassandra, Prometheus, Grafana)

From the project root directory, launch the dev infrastructure:

```bash
docker compose -f infra/docker-compose.dev.yml up -d
```

#### What happens behind the scenes:
1. **Zookeeper & Kafka (7.5.3)** start up and expose Kafka on `localhost:9092`.
2. **Redis 7.2** starts on `localhost:6379`.
3. **Cassandra 4.1** starts on `localhost:9042`.
4. **`cassandra-init` container** waits for Cassandra to become healthy, then automatically executes `infra/cassandra/init.cql` to create the `streamflow` keyspace and tables (`viewer_events`, `metric_snapshots`, `alerts`).
5. **Prometheus & Grafana** start and pre-load datasources and dashboards.

#### Verify Backing Services:
```bash
docker compose -f infra/docker-compose.dev.yml ps
```
Ensure all containers show `Up` or `Up (healthy)`.

---

### Step 2: Initialize Kafka Topics

Kafka requires 5 dedicated topics with specific partition configurations. Run the topic initialization script:

#### On Linux / macOS / Git Bash:
```bash
chmod +x infra/kafka/init-topics.sh
./infra/kafka/init-topics.sh
```

#### On Windows (PowerShell):
If you do not have Git Bash, run the topic creation commands directly via Docker:
```powershell
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic viewer-events --partitions 6 --replication-factor 1
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic stream-health --partitions 3 --replication-factor 1
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic alerts --partitions 3 --replication-factor 1
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic metrics-aggregated --partitions 3 --replication-factor 1
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic cb-events --partitions 3 --replication-factor 1
```

#### Verify Topics:
```bash
docker compose -f infra/docker-compose.dev.yml exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
*Expected Output:*
```
alerts
cb-events
metrics-aggregated
stream-health
viewer-events
```

---

### Step 3: Build Shared Libraries & Backend Services

Compile and install the `streamflow-common` contract library so the microservices can resolve all shared DTO records and enums:

```bash
mvn -f backend/pom.xml clean install -DskipTests
```

---

### Step 4: Run Backend Microservices

Open **3 separate terminal windows** (or use your IDE run configurations) and start each service:

#### Terminal 1 — `streamflow-producer` (Port 8081)
Simulates ~1,000 TPS of realistic viewer events and health telemetry:
```bash
mvn -f backend/streamflow-producer/pom.xml spring-boot:run
```
*Logs to look for:* `Started StreamProducerApplication in ... seconds` and periodic simulation event batch logs.

#### Terminal 2 — `streamflow-processor` (Port 8082)
Consumes Kafka events, aggregates metrics in Redis, runs the alert engine:
```bash
mvn -f backend/streamflow-processor/pom.xml spring-boot:run
```
*Logs to look for:* `Started StreamProcessorApplication in ... seconds` and periodic `SnapshotPublisher` execution logs.

#### Terminal 3 — `streamflow-api` (Port 8080)
Provides REST endpoints and WebSocket/STOMP gateway:
```bash
mvn -f backend/streamflow-api/pom.xml spring-boot:run
```
*Logs to look for:* `Started StreamApiApplication in ... seconds` and STOMP message broker initialization.

---

### Step 5: Start Frontend Dashboard

Open **Terminal 4** and launch the React 19 development server:

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server will start at **`http://localhost:5173`**.

> **Note on Vite Proxy:** The frontend is configured with a built-in proxy in `vite.config.ts`. All calls to `/api/*` and WebSocket connections to `/ws` are automatically forwarded to `http://localhost:8080`.

---

## 4. Mode B: Single-Command Full-Stack Docker Deployment

If you prefer to run everything inside Docker without local Java/Maven/Node installations:

### Step 1: Launch the Full Stack
```bash
docker compose up --build
```

### Step 2: Automated Healthcheck Startup Sequence
The root `docker-compose.yml` automatically handles service orchestration:
1. `zookeeper` starts ➔ `kafka` starts ➔ `kafka-init` creates all topics and exits `0`.
2. `redis` starts and passes `PING` health check.
3. `cassandra` starts ➔ `cassandra-init` executes `init.cql` schema creation.
4. `producer`, `processor`, and `api` start in parallel once databases and topics are healthy.
5. `frontend` (Nginx on port 3000), `prometheus` (port 9090), and `grafana` (port 3001) start once the API service is UP.

### Access URLs:
- **Live React Dashboard:** [http://localhost:3000](http://localhost:3000)
- **API Swagger Documentation:** [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)
- **Grafana Monitoring:** [http://localhost:3001](http://localhost:3001) (User: `admin`, Pass: `admin`)
- **Prometheus Metrics Engine:** [http://localhost:9090](http://localhost:9090)

### Teardown:
To stop all containers and remove persistent volumes:
```bash
docker compose down -v
```

---

## 5. Verifying the End-to-End Pipeline

### 1. Verify Active Streams via REST API
Check that the API gateway returns live stream summaries:
```bash
curl -s http://localhost:8080/api/v1/streams | jq .
```
*Expected response:* JSON array containing `stream-001`, `stream-002`, and `stream-003` with viewer counts and health scores.

### 2. Verify Redis Hot State
Connect to the Redis container and inspect cached stream state:
```bash
docker exec -it streamflow-redis redis-cli

# Check active streams set
SMEMBERS active_streams

# Check live viewer count for stream-001 (ZSET cardinality)
ZCARD stream_viewers:stream-001

# Inspect the 1-second cached snapshot JSON
GET stream_snapshot:stream-001

# Check circuit breaker state
GET cb_state:alert_processor
```

### 3. Verify Cassandra Historical Snapshots
Connect to Cassandra and verify that 1-minute aggregation snapshots are being written:
```bash
docker exec -it streamflow-cassandra cqlsh -e "SELECT stream_id, minute_bucket, live_viewer_count, health_score, buffer_rate_pct FROM streamflow.metric_snapshots LIMIT 5;"
```

---

## 6. Interactive Feature Walkthrough & Chaos Testing

### 1. Real-Time Telemetry Dashboard
Open **`http://localhost:5173`** (or `http://localhost:3000` for Docker). You will observe:
- **Live Stream Cards:** Real-time viewer counts updating every second.
- **Interactive Area Charts:** Rolling 60-second sliding-window viewer volume.
- **Health Gauges:** Dynamic health score (Green: Healthy, Amber: Degraded, Red: Critical).
- **Resolution Distribution:** Percentage breakdown of 1080p, 720p, 480p, and 360p playback.
- **Circuit Breaker Indicator:** Real-time badge showing `CLOSED` under normal operations.

### 2. Injecting Chaos Scenarios
You can trigger intentional system degradations directly from the UI or via REST cURL:

#### Scenario 1: Inject High Buffering (`HIGH_BUFFER`)
Simulates CDN congestion by boosting client buffering rates to 12%+:
```bash
curl -X POST http://localhost:8080/api/v1/streams/stream-001/chaos \
  -H "Content-Type: application/json" \
  -d '{"scenario": "HIGH_BUFFER", "durationSeconds": 30}'
```
*Expected UI Behavior:*
- Buffer Rate badge turns amber/red (> 8%).
- `HighBufferRateRule` alert fires and pops up in the `AlertFeed`.
- Stream health gauge drops.
- Automatically reverts back after 30 seconds.

#### Scenario 2: Inject Bitrate Degradation (`BITRATE_SPIKE`)
Drops stream bitrate to 1,500 Kbps with 15% dropped frames:
```bash
curl -X POST http://localhost:8080/api/v1/streams/stream-002/chaos \
  -H "Content-Type: application/json" \
  -d '{"scenario": "BITRATE_SPIKE", "durationSeconds": 30}'
```

#### Scenario 3: Cancel Active Chaos
```bash
curl -X DELETE http://localhost:8080/api/v1/streams/stream-001/chaos/<chaosId>
```

### 3. Historical Replay & Time-Window Scrubbing
1. On any Stream Card, click the **"History"** or **"Replay"** button.
2. Select a time window (e.g. Last 1 Hour, Last 6 Hours).
3. The modal queries `GET /api/v1/streams/{streamId}/history` from **Cassandra**.
4. Use the interactive time scrubber to replay viewer numbers and health metrics at historical timestamps.

---

## 7. Running Tests & Code Quality Checks

### Run All Backend Tests (Unit + Integration):
```bash
mvn -f backend/pom.xml verify
```

### Run Unit Tests Only (Excluding Testcontainers ITs):
```bash
mvn -f backend/pom.xml test "-Dtest=!*IT"
```

### Check and Apply Java Code Formatting (Spotless + Google Java Format):
```bash
# Check formatting
mvn -f backend/pom.xml spotless:check

# Auto-format Java files
mvn -f backend/pom.xml spotless:apply
```

### Run Frontend Tests & Linting:
```bash
cd frontend

# Run Vitest test suite (104+ tests)
npm test

# Run ESLint check
npm run lint

# Verify production build bundle
npm run build
```

---

## 8. Troubleshooting & Common Pitfalls

### 1. Cassandra Container Fails to Start or Times Out
- **Symptom:** `Connection refused on port 9042` or `cassandra-init` fails with timeout.
- **Cause:** Cassandra requires at least 1 GB to 2 GB of free RAM to initialize its JVM heap.
- **Fix:** In Docker Desktop settings, increase memory limit to **4 GB or 6 GB**. Wait 60 seconds for the first startup.

### 2. Kafka Topic Connection Error (`UnknownTopicOrPartitionException`)
- **Symptom:** `streamflow-processor` logs show `Topic viewer-events not present in metadata`.
- **Fix:** Make sure you ran `./infra/kafka/init-topics.sh` (or executed the `kafka-topics` creation commands in Step 2).

### 3. Port Already in Use (e.g. Port 8080, 6379, or 9092)
- **Symptom:** `BindException: Address already in use`.
- **Fix:** Check for existing local instances of Redis, PostgreSQL, or other Java servers running on host ports:
  ```bash
  # Windows PowerShell
  netstat -ano | findstr :8080
  netstat -ano | findstr :6379
  
  # Linux / macOS
  lsof -i :8080
  lsof -i :6379
  ```
  Stop the colliding process or alter port assignments in `application.properties`.

### 4. WebSocket Fails to Connect in Frontend
- **Symptom:** Browser console shows `WebSocket connection to 'ws://localhost:5173/ws' failed`.
- **Fix:** Ensure `streamflow-api` is running on port `8080`. Vite dev proxy forwards `/ws` to `localhost:8080`.

---

## 9. Environment Variables Reference

You can customize runtime behavior by exporting environment variables or setting them in `.env`:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker host & port |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `CASSANDRA_CONTACT_POINTS` | `localhost` | Cassandra contact point |
| `CASSANDRA_PORT` | `9042` | Cassandra native port |
| `CASSANDRA_DATACENTER` | `datacenter1` | Cassandra local datacenter name |
| `STREAMFLOW_SIMULATION_ENABLED` | `true` | Enable/disable synthetic traffic producer |
| `STREAMFLOW_SIMULATION_TPS` | `1000` | Target transactions/events per second |
| `STREAMFLOW_PRODUCER_BASE_URL` | `http://localhost:8081` | Base URL used by API gateway to proxy chaos commands |

---

*Enjoy exploring StreamFlow! For architectural details and spec documentation, refer to [`PROJECT_ANALYSIS.md`](PROJECT_ANALYSIS.md) and [`specs/`](specs/).*
