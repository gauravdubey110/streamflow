# Commit Plan — SPEC-20: Observability — Actuator + Micrometer + Prometheus

Suggested branch: feat/spec-20-observability

---

## Commit 1 — Add Prometheus dep and actuator config to all 3 modules

**Message:**
```
SPEC-20: add micrometer-prometheus dep + expose actuator endpoints

- Add micrometer-registry-prometheus to streamflow-producer POM
  (processor and api already had it from prior specs)
- Expose health,info,metrics,prometheus in all 3 application configs
- Add management.metrics.tags.application tag to each service
- Add management.info.git/build config for /actuator/info

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/streamflow-producer/pom.xml
- backend/streamflow-producer/src/main/resources/application.yml
- backend/streamflow-processor/src/main/resources/application.properties
- backend/streamflow-api/src/main/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-producer/pom.xml \
        backend/streamflow-producer/src/main/resources/application.yml \
        backend/streamflow-processor/src/main/resources/application.properties \
        backend/streamflow-api/src/main/resources/application.properties
```

---

## Commit 2 — Add git-commit-id plugin to parent POM and all runnable modules

**Message:**
```
SPEC-20: add git-commit-id-maven-plugin and build-info goal

- Add plugin to parent POM pluginManagement (version 8.0.2)
- Activate in producer, processor, and api runnable modules
- Add spring-boot build-info goal so /actuator/info shows build
  timestamp alongside git commit hash and branch

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/pom.xml
- backend/streamflow-producer/pom.xml
- backend/streamflow-processor/pom.xml
- backend/streamflow-api/pom.xml

**Stage command:**
```bash
git add backend/pom.xml \
        backend/streamflow-producer/pom.xml \
        backend/streamflow-processor/pom.xml \
        backend/streamflow-api/pom.xml
```

---

## Commit 3 — Add ProducerMetrics and instrument producer module

**Message:**
```
SPEC-20: add ProducerMetrics and instrument ViewerEventProducer + StreamHealthProducer

- Create ProducerMetrics Spring component with two counters:
  streamflow.events.published[topic=viewer-events] and
  streamflow.events.published[topic=stream-health]
- Inject ProducerMetrics into StreamSimulator and pass to ViewerEventProducer
- Increment counter on successful Kafka send in ViewerEventProducer
- Increment counter on successful Kafka send in StreamHealthProducer
- Add MetricsConfig with MeterFilter cardinality guard (NFR2)

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/streamflow-producer/src/main/java/com/streamflow/producer/metrics/ProducerMetrics.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/config/MetricsConfig.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamSimulator.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java
- backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java

**Stage command:**
```bash
git add backend/streamflow-producer/src/main/java/com/streamflow/producer/metrics/ProducerMetrics.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/config/MetricsConfig.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamSimulator.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/ViewerEventProducer.java \
        backend/streamflow-producer/src/main/java/com/streamflow/producer/simulator/StreamHealthProducer.java
```

---

## Commit 4 — Add ProcessorMetrics and instrument processor module

**Message:**
```
SPEC-20: add ProcessorMetrics and instrument processor module

- Create ProcessorMetrics component with:
  - streamflow.events.consumed[topic=viewer-events|stream-health] counters
  - streamflow.alerts.fired[severity,alertType] counter (per-type on demand)
  - streamflow.cb.state gauge (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
- Rename SnapshotPublisher timer from streamflow.snapshot.publish to
  streamflow.snapshot.duration (SPEC-20 R3)
- Inject ProcessorMetrics into ViewerEventConsumer, StreamHealthConsumer,
  AlertEngine, and AlertProcessorCircuitBreaker
- Increment consumed counters on successful Kafka ack
- Increment alerts.fired counter when AlertEngine publishes an alert
- Update cb.state gauge on every CB state transition (including startup)
- Add MetricsConfig with MeterFilter cardinality guard (NFR2)

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/metrics/ProcessorMetrics.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/MetricsConfig.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/StreamHealthConsumer.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertEngine.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/AlertProcessorCircuitBreaker.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/metrics/ProcessorMetrics.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/config/MetricsConfig.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/ViewerEventConsumer.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/consumer/StreamHealthConsumer.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/alert/AlertEngine.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/AlertProcessorCircuitBreaker.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/snapshot/SnapshotPublisher.java
```

---

## Commit 5 — Add ApiMetrics and instrument API gateway module

**Message:**
```
SPEC-20: add ApiMetrics and instrument API gateway push consumers

- Create ApiMetrics component with two counters:
  streamflow.events.consumed[topic=metrics-aggregated] and
  streamflow.events.consumed[topic=alerts]
- Inject ApiMetrics into MetricsPushConsumer and AlertPushConsumer
- Increment counters on each successful Kafka consume + STOMP push
- Add MetricsConfig with MeterFilter cardinality guard (NFR2)

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/metrics/ApiMetrics.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/MetricsConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/MetricsPushConsumer.java
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertPushConsumer.java

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/metrics/ApiMetrics.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/config/MetricsConfig.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/websocket/MetricsPushConsumer.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertPushConsumer.java
```

---

## Commit 6 — Add Prometheus and Grafana Docker Compose services with dashboard

**Message:**
```
SPEC-20: add Prometheus + Grafana services and pre-provisioned dashboard

- Add prometheus service (prom/prometheus:v2.51.2) at port 9090 to
  infra/docker-compose.dev.yml; config at infra/prometheus/prometheus.yml
  scraping all 3 backends every 10s
- Add grafana service (grafana/grafana:10.4.2) at port 3001 with
  auto-provisioned Prometheus datasource and StreamFlow dashboard
- Add dashboard JSON (infra/grafana/dashboards/streamflow.json) with
  4 panels: throughput, consumer lag, alerts/min, CB state gauge

Refs: specs/SPEC-20-observability.md
```

**Files:**
- infra/docker-compose.dev.yml
- infra/prometheus/prometheus.yml
- infra/grafana/provisioning/datasources/prometheus.yaml
- infra/grafana/provisioning/dashboards/dashboard.yaml
- infra/grafana/dashboards/streamflow.json

**Stage command:**
```bash
git add infra/docker-compose.dev.yml \
        infra/prometheus/prometheus.yml \
        infra/grafana/provisioning/datasources/prometheus.yaml \
        infra/grafana/provisioning/dashboards/dashboard.yaml \
        infra/grafana/dashboards/streamflow.json
```

---

## Commit 7 — Add observability unit tests and fix test breakage from constructor changes

**Message:**
```
SPEC-20: add metrics unit tests and fix constructors in existing tests

- Add ProducerMetricsTest (3 tests) verifying viewer-events and
  stream-health counters increment independently
- Add ProcessorMetricsTest (7 tests) verifying events.consumed,
  alerts.fired (per severity+alertType), and cb.state gauge transitions
- Fix CircuitBreakerStateListenerTest: add ProcessorMetrics mock to
  match new AlertProcessorCircuitBreaker 4-arg constructor
- Fix AlertPushConsumerTest: add ApiMetrics mock to match new
  AlertPushConsumer field injected by Lombok @RequiredArgsConstructor

Refs: specs/SPEC-20-observability.md
```

**Files:**
- backend/streamflow-producer/src/test/java/com/streamflow/producer/metrics/ProducerMetricsTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/metrics/ProcessorMetricsTest.java
- backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateListenerTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertPushConsumerTest.java

**Stage command:**
```bash
git add backend/streamflow-producer/src/test/java/com/streamflow/producer/metrics/ProducerMetricsTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/metrics/ProcessorMetricsTest.java \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStateListenerTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertPushConsumerTest.java
```

---

## Commit 8 — Mark SPEC-20 Done with evidence and commit plan

**Message:**
```
SPEC-20: mark spec Done, add evidence section and commit plan

Refs: specs/SPEC-20-observability.md
```

**Files:**
- specs/SPEC-20-observability.md
- commits/SPEC-20.md

**Stage command:**
```bash
git add specs/SPEC-20-observability.md commits/SPEC-20.md
```

---

## Verification before pushing
- [ ] `mvn -f backend/pom.xml test -Dtest="ProducerMetricsTest,ProcessorMetricsTest,SnapshotPublisherTest,AlertPushConsumerTest,CircuitBreakerPushConsumerTest,DtoRoundTripTest,KafkaTopicsTest,HealthScoreCalculatorTest,BitrateDegradationRuleTest,HighBufferRateRuleTest,CircuitBreakerStateListenerTest,CircuitBreakerStatePublisherTest,HistoryServiceTest,HistoryControllerTest,BucketHelperTest,ChaosControllerTest,NormalLoadStrategyTest,ChaosAwareStrategyTest,ChaosInjectorTest,InternalChaosControllerTest"` → 38 tests pass
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` → lint clean, 104 tests pass, build succeeds
- [ ] `mvn -f backend/pom.xml package -DskipTests` → build-info.properties and git.properties generated in target/classes for all 3 modules
- [ ] Demo evidence in spec matches reality
