# StreamFlow

Real-time live streaming analytics dashboard: Kafka → Spring Boot → Redis/Cassandra → React (WebSocket).

[![CI](https://github.com/YOUR_ORG/StreamFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_ORG/StreamFlow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Replace `YOUR_ORG/StreamFlow` in the badge URL with your actual GitHub repository path.

---

## Architecture

```
Producer (1K TPS)
  → viewer-events / stream-health (Kafka topics)
  → Processor: Redis sliding-window aggregation + alert engine + circuit breaker
  → metrics-aggregated / alerts / cb-events (Kafka topics)
  → API Gateway: REST + STOMP/SockJS WebSocket push (1 s interval)
  → React Dashboard (live charts, alert feed, chaos controls)

Persistence: Cassandra (time-series, historical replay)
Observability: Prometheus + Grafana
```

| Service | Port | Role |
|---|---|---|
| `streamflow-producer` | 8081 | Simulates 1K viewer events/s; chaos injection |
| `streamflow-processor` | 8082 | Kafka consumer; Redis aggregation; Resilience4j CB |
| `streamflow-api` | 8080 | REST API + STOMP WebSocket gateway |
| Frontend (Nginx) | 3000 | React live dashboard |
| Prometheus | 9090 | Metrics scraping |
| Grafana | 3001 | Pre-provisioned dashboard (admin/admin) |

---

## Quick start — infrastructure only (dev)

```bash
# Start Kafka, Redis, Cassandra, Prometheus, Grafana
docker compose -f infra/docker-compose.dev.yml up -d

# Create Kafka topics
./infra/kafka/init-topics.sh
```

Then run each service locally:

```bash
# Terminal 1
mvn -f backend/streamflow-producer/pom.xml spring-boot:run

# Terminal 2
mvn -f backend/streamflow-processor/pom.xml spring-boot:run

# Terminal 3
mvn -f backend/streamflow-api/pom.xml spring-boot:run

# Terminal 4
cd frontend && npm install && npm run dev
# Open http://localhost:5173
```

---

## Full-stack (single command)

```bash
docker compose up --build
# Dashboard: http://localhost:3000
# Grafana:   http://localhost:3001  (admin / admin)
```

Startup order is enforced via healthchecks and `depends_on` conditions — no manual waiting required.

To tear down and remove volumes:

```bash
docker compose down -v
```

---

## Running tests

```bash
# Backend (JUnit 5 + Testcontainers)
mvn -f backend/pom.xml verify

# Frontend (Vitest + React Testing Library)
cd frontend && npm test

# Backend code format check (Spotless / Google Java Format)
mvn -f backend/pom.xml spotless:check
```

---

## Pre-commit hooks (optional)

**Backend:** `mvn spotless:apply` auto-formats Java source with Google Java Format.

**Frontend:** Husky + lint-staged runs ESLint + Prettier on staged files.

```bash
# Install husky (one-time, from frontend/ directory)
cd frontend
npm install --save-dev husky lint-staged
npx husky install
```

---

## Environment variables

All connection strings default to `localhost` for local development and accept overrides for Docker/Kubernetes:

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | producer, processor, api |
| `REDIS_HOST` | `localhost` | processor, api |
| `REDIS_PORT` | `6379` | processor, api |
| `CASSANDRA_CONTACT_POINTS` | `localhost` | processor, api |
| `CASSANDRA_PORT` | `9042` | processor, api |
| `STREAMFLOW_SIMULATION_TPS` | `1000` | producer |
| `STREAMFLOW_PRODUCER_BASE_URL` | `http://localhost:8081` | api (chaos proxy) |

---

## References

- [Project Plan](StreamFlow_Project_Plan.md) — architecture, data models, API contracts
- [Spec Index](specs/README.md) — 22-spec, 4-week implementation schedule
