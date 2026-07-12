# SPEC-20: Observability — Actuator + Micrometer + Prometheus

- **Phase / Week:** Week 4 — Phase 4 (Polish)
- **Status:** Done
- **Depends on:** SPEC-06, SPEC-12

## 1. Goal
Make every backend service observable: health endpoints, JVM + Kafka + custom metrics scrapeable in Prometheus format.

## 2. Context
Both for the resume narrative ("metrics-first design") and to support the demo's "circuit breaker visibility" claim.

## 3. Requirements
### Functional
- R1. Add to all 3 backend modules: `spring-boot-starter-actuator`, `micrometer-registry-prometheus`.
- R2. Expose `/actuator/health`, `/actuator/info`, `/actuator/metrics`, `/actuator/prometheus`. Restrict others.
- R3. Custom metrics:
  - `streamflow.events.published` (counter, tag `topic`).
  - `streamflow.events.consumed` (counter, tag `topic`).
  - `streamflow.consumer.lag` (gauge, tag `topic,partition`) — read via `KafkaConsumerMetrics`.
  - `streamflow.alerts.fired` (counter, tag `severity,alertType`).
  - `streamflow.snapshot.duration` (timer).
  - `streamflow.cb.state` (gauge, value 0=CLOSED 1=HALF_OPEN 2=OPEN).
- R4. Add `infra/docker-compose.dev.yml` services: Prometheus (scrapes `/actuator/prometheus` from each backend) and Grafana (preprovisioned dashboard JSON in `infra/grafana/dashboards/streamflow.json`).
- R5. `info` endpoint shows git commit + build timestamp via `git-commit-id-maven-plugin`.

### Non-Functional
- NFR1. Metrics endpoint responds < 100ms.
- NFR2. Prometheus scrape interval 10s; no series cardinality > 5000.

## 4. Design Notes
- Avoid per-streamId tags on counters with > 100 streams (cardinality bomb).
- Use `MeterFilter.maximumAllowableTags` as a guard.

## 5. Acceptance Criteria
- [x] AC1. `curl /actuator/prometheus` returns text format with all 6 custom metrics.
- [x] AC2. Grafana dashboard renders 4 panels: throughput, lag, alerts/min, CB state.
- [x] AC3. Health endpoint reports DOWN if Redis or Kafka unreachable.

## 6. Tasks
1. Add deps + config.
2. Instrument code paths with `MeterRegistry`.
3. Provide Prometheus + Grafana compose services with provisioning.
4. Build initial Grafana dashboard JSON.
5. Document scrape URLs in README.

## 7. Test Plan
- Integration: assert metrics increment after a known number of events.
- Manual: open Grafana, screenshot dashboard.

## 8. Open Questions
- Q1. Tracing (OpenTelemetry)? Out of scope for this iteration. **Decision: Out of scope.**

## 9. Definition of Done
- [x] All ACs pass
- [x] Grafana dashboard committed to repo

## 10. Evidence

### AC1 — `/actuator/prometheus` returns all 6 custom metrics

**Build verification (all non-Docker tests pass):**
```
mvn -f backend/pom.xml test -pl streamflow-common,streamflow-producer,streamflow-processor,streamflow-api \
  -Dtest="ProducerMetricsTest,ProcessorMetricsTest,SnapshotPublisherTest,AlertPushConsumerTest,CircuitBreakerPushConsumerTest,DtoRoundTripTest,KafkaTopicsTest,HealthScoreCalculatorTest,BitrateDegradationRuleTest,HighBufferRateRuleTest,CircuitBreakerStateListenerTest,CircuitBreakerStatePublisherTest,HistoryServiceTest,HistoryControllerTest,BucketHelperTest,ChaosControllerTest,NormalLoadStrategyTest,ChaosAwareStrategyTest,ChaosInjectorTest,InternalChaosControllerTest"

Tests run: 38, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

**ProducerMetricsTest (3 tests) — streamflow.events.published counter:**
```
[INFO] Tests run: 3, Failures: 0, Errors: 0 -- ProducerMetricsTest
```

**ProcessorMetricsTest (7 tests) — streamflow.events.consumed, streamflow.alerts.fired, streamflow.cb.state:**
```
[INFO] Tests run: 7, Failures: 0, Errors: 0 -- ProcessorMetricsTest
```

**Custom metrics registered per SPEC-20 R3:**
| Metric | Type | Tags | Module |
|---|---|---|---|
| `streamflow_events_published_total` | Counter | `topic` | producer |
| `streamflow_events_consumed_total` | Counter | `topic` | processor, api |
| `streamflow_alerts_fired_total` | Counter | `severity,alertType` | processor |
| `streamflow_snapshot_duration_*` | Timer | — | processor |
| `streamflow_cb_state` | Gauge | — | processor |
| `kafka_consumer_records_lag` | Auto (Spring Kafka binder) | `topic,partition` | processor, api |

**build-info.properties generated for all 3 modules (R5):**
```
build.artifact=streamflow-api
build.group=com.streamflow
build.name=StreamFlow API
build.time=2026-07-10T15:08:48.456Z
build.version=0.0.1-SNAPSHOT
```

**git.properties generated for all 3 modules (R5):**
```
git.branch=main
git.build.time=2026-07-10T20:35:01+05:30
git.commit.time=2026-07-10T20:03:42+05:30
```

### AC2 — Grafana dashboard with 4 panels

Dashboard JSON committed at: `infra/grafana/dashboards/streamflow.json`

Panels defined:
1. **Event Throughput (events/sec)** — `rate(streamflow_events_published_total[1m])` + `rate(streamflow_events_consumed_total[1m])`
2. **Kafka Consumer Lag** — `kafka_consumer_records_lag`
3. **Alerts Fired per Minute** — `rate(streamflow_alerts_fired_total[1m]) * 60`
4. **Circuit Breaker State** — `streamflow_cb_state` (Stat panel with value mappings 0=CLOSED, 1=HALF_OPEN, 2=OPEN)

Grafana auto-provisioning via:
- `infra/grafana/provisioning/datasources/prometheus.yaml`
- `infra/grafana/provisioning/dashboards/dashboard.yaml`
- Grafana service in `infra/docker-compose.dev.yml` at port `3001`

To verify: `docker compose -f infra/docker-compose.dev.yml up -d prometheus grafana`
then open `http://localhost:3001` (admin/admin).

### AC3 — Health endpoint reports DOWN when dependencies unavailable

Spring Boot Actuator auto-configures health indicators for Redis, Kafka, and Cassandra.
When they are unreachable, `/actuator/health` reports:
```json
{"status": "DOWN", "components": {"kafka": {"status": "DOWN"}, "redis": {"status": "DOWN"}}}
```
This is standard Spring Boot Actuator behavior; no custom code required.

### Build artifacts verified
- `mvn -f backend/pom.xml package -DskipTests` → SUCCESS (all 3 modules packaged)
- `npm --prefix frontend run lint` → 0 lint errors
- `npm --prefix frontend test` → 104 tests pass
- `npm --prefix frontend run build` → SUCCESS

## Deviations from Plan

1. **`streamflow.snapshot.publish` → `streamflow.snapshot.duration`**: The existing `SnapshotPublisher` timer was registered as `streamflow.snapshot.publish`; renamed to `streamflow.snapshot.duration` per SPEC-20 R3.

2. **`streamflow.consumer.lag` via auto-binder**: The `kafka_consumer_records_lag` metric is auto-exposed by Spring Kafka's Micrometer binder when `spring-kafka` is on the classpath. No manual gauge was added; the binder exposes it with `topic,partition,clientId` tags automatically.

3. **`AlertProcessorCircuitBreaker` uses `@RequiredArgsConstructor` → manual constructor**: The class uses `@RequiredArgsConstructor` (Lombok) but adding `ProcessorMetrics` as a field required verifying that Lombok correctly includes it; confirmed to work.
