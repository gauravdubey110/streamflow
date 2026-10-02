# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

StreamFlow is a real-time live-streaming analytics platform: a Kafka-driven pipeline simulates
viewer/encoder telemetry, a Spring Boot processor aggregates it through Redis (with Cassandra for
history), and a React dashboard consumes live updates over STOMP/WebSocket.

```
Producer (simulates 1K events/s + chaos injection)
  → viewer-events / stream-health (Kafka topics)
  → Processor: Redis sliding-window aggregation + alert engine + circuit breaker
  → metrics-aggregated / alerts / cb-events (Kafka topics)
  → API Gateway: REST + STOMP/SockJS WebSocket push (1 s interval)
  → React Dashboard (live charts, alert feed, chaos controls)

Persistence: Cassandra (time-series, historical replay)
Observability: Prometheus + Grafana
```

| Module | Port | Role |
|---|---|---|
| `backend/streamflow-producer` | 8081 | Simulates viewer/health events; exposes internal chaos-injection REST API |
| `backend/streamflow-processor` | 8082 | Kafka consumers; Redis aggregation; alert engine; Resilience4j circuit breaker; Cassandra writes |
| `backend/streamflow-api` | 8080 | REST API + STOMP/SockJS WebSocket gateway; proxies chaos requests to producer |
| `backend/streamflow-common` | — | Shared library: DTOs (Java records), enums, Kafka topic constants — no runnable app |
| `frontend` | 5173 (dev) / 3000 (docker) | React 19 + Vite dashboard |

The project was built as a sequence of specs (`specs/SPEC-NN-*.md`), each with a corresponding
commit log entry in `commits/SPEC-NN.md`. Code comments frequently reference `SPEC-NN R<n>` —
these point back to the requirement in that spec file, which is the authoritative source for *why*
a piece of code exists (deviations, open-question resolutions, trade-offs are documented there).
When making non-trivial changes to existing logic, check the referenced spec first.

## Commands

### Backend (Maven multi-module, Java 17, from `backend/`)

```bash
# Build + run all tests (unit + Testcontainers integration tests) for every module
mvn -f backend/pom.xml verify

# Single module
mvn -f backend/streamflow-processor/pom.xml verify

# Single test class
mvn -f backend/pom.xml test -Dtest=ViewerCountAggregatorTest

# Single test method
mvn -f backend/pom.xml test -Dtest=ViewerCountAggregatorTest#recordJoin_addsViewerToSortedSet

# Format check / auto-format (Spotless + Google Java Format; enforced in CI)
mvn -f backend/pom.xml spotless:check
mvn -f backend/pom.xml spotless:apply

# Run a service locally (needs infra up — see below)
mvn -f backend/streamflow-producer/pom.xml spring-boot:run
mvn -f backend/streamflow-processor/pom.xml spring-boot:run
mvn -f backend/streamflow-api/pom.xml spring-boot:run
```

See `backend/CLAUDE.md` for module boundaries and the Testcontainers/IT.java testing contract.

### Frontend (from `frontend/`)

Standard npm scripts (`npm run dev|build|lint|test|test:watch`) — see `frontend/package.json`.
See `frontend/CLAUDE.md` for architecture conventions.

### Infrastructure

```bash
# Dev: infra only (Kafka, Redis, Cassandra, Prometheus, Grafana) — run backend/frontend locally
docker compose -f infra/docker-compose.dev.yml up -d
./infra/kafka/init-topics.sh

# Full stack in Docker (build + run everything, healthcheck-ordered startup)
docker compose up --build
# Dashboard: http://localhost:3000  API: http://localhost:8080  Grafana: http://localhost:3001 (admin/admin)
docker compose down -v   # teardown + remove volumes
```

Kafka topics (partitions, RF=1): `viewer-events` (6), `stream-health` (3), `alerts` (3),
`metrics-aggregated` (3), `cb-events` (3).

Env var overrides (all default to `localhost` for local dev): `KAFKA_BOOTSTRAP_SERVERS`,
`REDIS_HOST`, `REDIS_PORT`, `CASSANDRA_CONTACT_POINTS`, `CASSANDRA_PORT`,
`STREAMFLOW_SIMULATION_TPS`, `STREAMFLOW_PRODUCER_BASE_URL`.

## Architecture

Per-module detail lives in `backend/CLAUDE.md` (module responsibilities, Redis key conventions,
Kafka ack/retry/DLT behavior) and `frontend/CLAUDE.md` (store/hook/STOMP layering) — read the
relevant one before editing. There is no `mvnw` wrapper; a system `mvn` (3.9+) is required.
Integration tests need a working Docker daemon (Testcontainers); to skip them locally run
`mvn -f backend/pom.xml test "-Dtest=!*IT"`.

### CI (`.github/workflows/ci.yml`)

Three jobs: `backend` (Spotless check + `mvn verify` with Testcontainers, Ryuk disabled),
`frontend` (lint + test + build), `docker` (build/push all four images to GHCR, main-branch pushes
only, gated on the other two jobs passing).
