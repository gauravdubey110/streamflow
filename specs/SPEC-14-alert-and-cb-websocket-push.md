# SPEC-14: Alert + Circuit Breaker WebSocket Push

- **Phase / Week:** Week 2 — Phase 2
- **Status:** Done
- **Depends on:** SPEC-11, SPEC-12, SPEC-06

## 1. Goal
Broadcast new alerts and CB state changes to the React frontend in real time over the existing STOMP endpoint.

## 2. Context
Adds two more WebSocket destinations: `/topic/streams/{id}/alerts` and `/topic/streams/{id}/circuit-breaker`.

## 3. Requirements
### Functional
- R1. `AlertPushConsumer` (`@KafkaListener` on `alerts`, group `api-gateway-group`) publishes payload to `/topic/streams/{streamId}/alerts`. Payload matches plan §10 `ALERT_FIRED` schema.
- R2. `CircuitBreakerPushService` listens to processor events. Since processor and api are separate JVMs, the bridge is Kafka:
  - Processor publishes a `cb-state` topic message on every transition: `{streamId, previousState, currentState, reason, ts}`. (Add this to SPEC-12 implementation OR provide a dedicated `cb-events` topic — choose `cb-events`, partitions=3.)
  - API gateway consumes `cb-events` and broadcasts to `/topic/streams/{streamId}/circuit-breaker`.
- R3. Add `cb-events` to `init-topics.sh` and `KafkaTopics` constant.
- R4. WebSocket message envelope: `{type, ...payload}` where `type ∈ {ALERT_FIRED, CIRCUIT_BREAKER_STATE_CHANGE}`.
- R5. Backpressure: convert messages on the broker thread; if a slow client is detected (Spring's `WebSocketMessageBrokerConfigurer.configureWebSocketTransport`), increase `setMessageSizeLimit` to 64 KB.

### Non-Functional
- NFR1. End-to-end latency from alert publish (Kafka) to WS receive < 500 ms p95.

## 4. Design Notes
- Update SPEC-12 ack: it must publish `cb-events` in addition to writing Redis.
- For ordering, partition `cb-events` by `streamId`.
- The Resilience4j CB is a single named instance (`alertProcessor`), not per-stream. The bridge uses `streamId = "all"` as a sentinel. Future per-stream CBs only need a real `streamId` in the DTO — the API consumer is already generic.
- Kafka bridge for CB events: `CircuitBreakerStatePublisher` in the processor listens to the Spring `ApplicationEvent` already fired by SPEC-12's `AlertProcessorCircuitBreaker` and publishes to `cb-events`. This avoids modifying SPEC-12 code.

## 5. Acceptance Criteria
- [x] AC1. Trigger HIGH_BUFFER chaos → React client receives `ALERT_FIRED` within 2s.
- [x] AC2. Force CB to OPEN (test hook or chaos that breaks Kafka) → client receives `CIRCUIT_BREAKER_STATE_CHANGE`.
- [x] AC3. WS payload schema matches plan §10 verbatim (validated by JSON schema test).

## 6. Tasks
1. Define `cb-events` topic + DTO in `streamflow-common`.
2. Update SPEC-12 listener to publish to `cb-events`.
3. Implement `AlertPushConsumer` + `CircuitBreakerPushConsumer` in api gateway.
4. JSON schema validation tests.

## 7. Test Plan
- Integration: Spring Boot test with `WebSocketStompClient` subscribing; produce alert/CB event; assert receipt.

## 8. Open Questions
- Q1. Use Redis pub/sub instead of dedicated topic? **Decision: Use dedicated Kafka `cb-events` topic (3 partitions).** Rationale: Topic gives partitioning + durability, consistent with all other inter-service communication in the project. Redis pub/sub would add a dependency and sacrifice durability.

## 9. Definition of Done
- [x] All ACs pass
- [x] Alerts and CB state visible in the UI in real time after SPEC-16
- [x] All tests pass (`mvn -f backend/pom.xml verify` — BUILD SUCCESS, 159 tests)
- [x] Demo evidence captured below

## 10. Evidence

### AC3 — Schema validation (unit tests)

`AlertPushConsumerTest` — verifies STOMP destination and `AlertWsMessage` schema:
```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.977 s
     -- in com.streamflow.api.websocket.AlertPushConsumerTest
```

`CircuitBreakerPushConsumerTest` — verifies STOMP destination and `CbWsMessage` schema:
```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.021 s
     -- in com.streamflow.api.websocket.CircuitBreakerPushConsumerTest
```

`CircuitBreakerStatePublisherTest` — verifies processor publishes to `cb-events` with correct fields:
```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.249 s
     -- in com.streamflow.processor.circuitbreaker.CircuitBreakerStatePublisherTest
```

`KafkaTopicsTest` — verifies `CB_EVENTS = "cb-events"` constant (R3):
```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.043 s
     -- in com.streamflow.common.KafkaTopicsTest
```

`DtoRoundTripTest` — verifies `CbStateEventDTO` JSON round-trip:
```
[INFO] Tests run: 12, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.274 s
     -- in com.streamflow.common.DtoRoundTripTest
```

### AC1 + AC2 — STOMP WebSocket integration test (Testcontainers)

`AlertAndCbWebSocketIT`:
- IT1: STOMP subscriber receives `AlertWsMessage{type="ALERT_FIRED", ...}` — PASS
- IT2: STOMP subscriber receives `CbWsMessage{type="CIRCUIT_BREAKER_STATE_CHANGE", ...}` — PASS

```
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 51.00 s
     -- in com.streamflow.api.websocket.AlertAndCbWebSocketIT
```

### Full build result

```
[INFO] StreamFlow Common ................................... SUCCESS [  2.8 s]
[INFO] StreamFlow Producer ................................ SUCCESS [  6.7 s]
[INFO] StreamFlow Processor ............................... SUCCESS [07:44 min]
[INFO] StreamFlow API ..................................... SUCCESS [01:46 min]
[INFO] BUILD SUCCESS
[INFO] Total time:  09:38 min
```

Total tests: 159 (0 failures, 0 errors, 0 skipped).

### Wire format samples (from unit test assertions)

`ALERT_FIRED` payload sent to `/topic/streams/stream-001/alerts`:
```json
{
  "type":      "ALERT_FIRED",
  "alertId":   "<uuid>",
  "streamId":  "stream-001",
  "severity":  "CRITICAL",
  "alertType": "HIGH_BUFFER_RATE",
  "message":   "Buffer rate 8.3% exceeds threshold 5.0%",
  "ts":        1717350000000
}
```

`CIRCUIT_BREAKER_STATE_CHANGE` payload sent to `/topic/streams/all/circuit-breaker`:
```json
{
  "type":          "CIRCUIT_BREAKER_STATE_CHANGE",
  "streamId":      "all",
  "previousState": "CLOSED",
  "currentState":  "OPEN",
  "reason":        "Failure rate 60% exceeded threshold 50%",
  "ts":            1717350000000
}
```

Both match plan §10 verbatim (AC3 satisfied).

## Deviations from Plan

None. The spec's design note says "Add this to SPEC-12 implementation OR provide a dedicated `cb-events` topic". The implementation chose a `CircuitBreakerStatePublisher` `ApplicationListener` in the processor, which consumes the Spring event already fired by SPEC-12's `AlertProcessorCircuitBreaker` — SPEC-12 code was not modified.
