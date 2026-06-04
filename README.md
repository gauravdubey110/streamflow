# StreamFlow

Real-time live streaming analytics dashboard: Kafka → Spring Boot → Redis/Cassandra → React (WebSocket).

- [Project Plan](StreamFlow_Project_Plan.md) — architecture, data models, API contracts
- [Spec Index](specs/README.md) — 22-spec, 4-week implementation schedule

## Quick start (infra only)

```bash
docker compose -f infra/docker-compose.dev.yml up -d
./infra/kafka/init-topics.sh
```

## Full stack

```bash
docker compose up --build
# → http://localhost:3000
```
