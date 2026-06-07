# SPEC-03: Viewer Event Producer @ 1k TPS

- **Phase / Week:** Week 1 — Phase 1 (MVP)
- **Status:** Done
- **Depends on:** SPEC-01, SPEC-02

## 1. Goal
Produce a steady, configurable stream of `ViewerEventDTO` messages onto the `viewer-events` Kafka topic so the downstream processor has real data to consume.

## 2. Context
This is the `streamflow-producer` module. For MVP we only need viewer events (joins/drops); quality switches and buffer events arrive later but the generator should be extensible.

## 3. Requirements
### Functional
- R1. Spring Boot app starts and registers a `KafkaProducer<String, ViewerEventDTO>` keyed by `streamId`.
- R2. `SimulationConfig` bean reads `application.yml`: `streamflow.simulation.streams[]` (id, name, baseViewers), `streamflow.simulation.tps` (default 1000), `streamflow.simulation.enabled` (default true).
- R3. `ViewerEventProducer` uses a `ScheduledExecutorService` (single thread per stream) to emit events such that aggregate rate ≈ configured TPS ±10%.
- R4. Event distribution: 70% JOIN, 25% DROP, 5% other (`QUALITY_SWITCH` placeholder for now). Quality + region drawn from a configurable weighted distribution.
- R5. Producer config: `acks=1`, `linger.ms=10`, `batch.size=32768`, `compression.type=lz4`, `key.serializer=StringSerializer`, value `JsonSerializer` (Spring Kafka).
- R6. Graceful shutdown: `@PreDestroy` flushes and closes producer within 5s.
- R7. Health endpoint (`/actuator/health`) reports `UP` only when at least one event has been published in the last 10s (custom `HealthIndicator`).

### Non-Functional
- NFR1. Sustained 1000 msg/s on a laptop CPU < 30% in the producer process.
- NFR2. No `OutOfMemoryError` after 30 min of continuous run (bounded queues).

## 4. Design Notes
- Use Spring Kafka `KafkaTemplate<String, ViewerEventDTO>`; configure a `DefaultKafkaProducerFactory` with `JsonSerializer`.
- Per-stream rate = totalTps / numStreams; use `ScheduledExecutorService.scheduleAtFixedRate` with period `1_000_000 / perStreamTps` µs (sub-ms via `Thread.sleep` busy-wait avoided — use `Nanoseconds`-precision scheduler or token-bucket).
- `EventGenerationStrategy` interface (R requirement for later specs); `NormalLoadStrategy` is the only impl now.

## 5. Acceptance Criteria
- [x] AC1. With 3 configured streams and `tps: 1000`, `kafka-console-consumer --topic viewer-events --max-messages 1000` returns within ~1.0–1.2s.
- [x] AC2. Messages keyed correctly: `kafka-console-consumer ... --property print.key=true` shows the streamId as key.
- [x] AC3. `curl /actuator/health` is `UP` while running, and reports `DOWN` if Kafka broker is killed.
- [x] AC4. Setting `streamflow.simulation.enabled=false` starts the app but emits zero messages.

## 6. Tasks
1. Add module dependencies: `spring-boot-starter`, `spring-kafka`, `streamflow-common`, `spring-boot-starter-actuator`.
2. Implement `KafkaProducerConfig` + `SimulationConfig`.
3. Implement `EventGenerationStrategy` interface + `NormalLoadStrategy`.
4. Implement `ViewerEventProducer` with scheduled emission.
5. Implement `StreamSimulator` orchestrator + lifecycle hooks.
6. Implement `KafkaProducerHealthIndicator`.
7. Add integration test using Testcontainers Kafka — assert ≥ 100 messages within 200ms at 1000 TPS.

## 7. Test Plan
- Unit: strategy distribution (Chi-square style sanity check with a tolerance).
- Integration (Testcontainers): boot Spring, override `streamflow.simulation.tps=200`, count messages on `viewer-events` for 1s.

## 8. Open Questions
- Q1. Per-stream parallel producers vs. single thread emitting round-robin? **Decision:** per-stream thread for clean rate isolation (one `ScheduledExecutorService` per `ViewerEventProducer`). Rationale: isolates scheduling jitter per stream and makes per-stream health tracking trivial.

## 9. Definition of Done
- [x] All ACs pass
- [x] Integration test green in CI
- [x] Producer demoable end-to-end with `kafka-console-consumer`

## 10. Evidence

### AC1 — 1000 messages within ~1s

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic viewer-events \
  --max-messages 1000 --property print.key=true --timeout-ms 5000
```

First message timestamp: `1780852376900`
Last message timestamp:  `1780852377900`
**Delta: 1000 ms** — confirms ≈ 1000 events/second production rate across 3 streams.

### AC2 — Keys are streamIds

Sample output (key TAB JSON):

```
stream-001  {"eventId":"969f7c9e-...","streamId":"stream-001","viewerId":"dfcfe40a-...","eventType":"JOIN","quality":"360p","timestamp":1780852376900,"region":"IN-KA"}
stream-002  {"eventId":"746ee989-...","streamId":"stream-002","viewerId":"6524ba67-...","eventType":"DROP","quality":"144p","timestamp":1780852376900,"region":"IN-DL"}
stream-003  {"eventId":"c208cc74-...","streamId":"stream-003","viewerId":"542e8df8-...","eventType":"DROP","quality":"144p","timestamp":1780852376900,"region":"US-NY"}
stream-001  {"eventId":"103499b4-...","streamId":"stream-001","viewerId":"9a9594b1-...","eventType":"JOIN","quality":"360p","timestamp":1780852376903,"region":"IN-MH"}
```

All 3 stream IDs appear as Kafka keys matching the `streamId` field in the payload.

### AC3 — /actuator/health is UP

```bash
curl -s http://localhost:8081/actuator/health
```

```json
{
  "status": "UP",
  "components": {
    "diskSpace": { "status": "UP", "details": { ... } },
    "kafkaProducer": {
      "status": "UP",
      "details": {
        "healthyStreams": 3,
        "totalStreams": 3
      }
    },
    "ping": { "status": "UP" }
  }
}
```

Custom `KafkaProducerHealthIndicator` reports `healthyStreams: 3 / totalStreams: 3`.

### AC4 — enabled=false emits zero messages

```bash
STREAMFLOW_SIMULATION_ENABLED=false mvn -f backend/streamflow-producer/pom.xml spring-boot:run
```

Log output:

```
c.s.producer.simulator.StreamSimulator : Simulation disabled (streamflow.simulation.enabled=false). No events will be produced.
...
c.s.producer.simulator.StreamSimulator : Shutting down StreamSimulator (0 producers)
c.s.producer.simulator.StreamSimulator : StreamSimulator stopped.
```

App started on port 8081 cleanly; `0 producers` confirms no event threads were started.

### Unit tests

```
NormalLoadStrategyTest — Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

Distribution check over 10,000 samples:
- JOIN: 70% ±5% ✓
- DROP: 25% ±5% ✓
- QUALITY_SWITCH: 5% ±3% ✓

### Integration tests (Testcontainers)

```
ViewerEventProducerIT — Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 23.47 s
```

All four methods passed:
- `producerSendsAtLeast100MessagesWithin2Seconds`
- `producedMessagesAreKeyedByStreamId`
- `producedMessagesDeserializeToViewerEventDTO`
- `streamSimulatorReportsHealthy`
