# Commit Plan — SPEC-14: Alert + Circuit Breaker WebSocket Push

Suggested branch: feat/spec-14-alert-cb-websocket-push

---

## Commit 1 — Add cb-events topic constant and CbStateEventDTO to common

**Message:**
```
SPEC-14: add CB_EVENTS topic constant and CbStateEventDTO to common

Add KafkaTopics.CB_EVENTS = "cb-events" (R3) and a new
CbStateEventDTO record carrying {streamId, previousState,
currentState, reason, ts} — the wire format for the cb-events
Kafka topic that bridges CB state transitions from the processor
JVM to the API gateway JVM.

Also extend KafkaTopicsTest (new cb-events assertion) and
DtoRoundTripTest (CbStateEventDTO JSON round-trip).

Refs: specs/SPEC-14-alert-and-cb-websocket-push.md
```

**Files:**
- backend/streamflow-common/src/main/java/com/streamflow/common/constants/KafkaTopics.java
- backend/streamflow-common/src/main/java/com/streamflow/common/dto/CbStateEventDTO.java
- backend/streamflow-common/src/test/java/com/streamflow/common/KafkaTopicsTest.java
- backend/streamflow-common/src/test/java/com/streamflow/common/DtoRoundTripTest.java

**Stage command:**
```bash
git add backend/streamflow-common/src/main/java/com/streamflow/common/constants/KafkaTopics.java \
        backend/streamflow-common/src/main/java/com/streamflow/common/dto/CbStateEventDTO.java \
        backend/streamflow-common/src/test/java/com/streamflow/common/KafkaTopicsTest.java \
        backend/streamflow-common/src/test/java/com/streamflow/common/DtoRoundTripTest.java
```

---

## Commit 2 — Add CircuitBreakerStatePublisher to processor

**Message:**
```
SPEC-14: publish CB state transitions to cb-events Kafka topic

Add CbKafkaConfig (producer factory + KafkaTemplate for
CbStateEventDTO) and CircuitBreakerStatePublisher (ApplicationListener
that converts the Spring CircuitBreakerStateEvent already fired by
SPEC-12's AlertProcessorCircuitBreaker into a CbStateEventDTO and
sends it to the cb-events topic keyed by streamId="all").

Failure policy: Kafka send errors are logged and swallowed; the CB
state is already in Redis from SPEC-12.

Also add cb-events topic name to processor application.properties.

Refs: specs/SPEC-14-alert-and-cb-websocket-push.md
```

**Files:**
- backend/streamflow-processor/src/main/java/com/streamflow/processor/config/CbKafkaConfig.java
- backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStatePublisher.java
- backend/streamflow-processor/src/main/resources/application.properties
- backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStatePublisherTest.java

**Stage command:**
```bash
git add backend/streamflow-processor/src/main/java/com/streamflow/processor/config/CbKafkaConfig.java \
        backend/streamflow-processor/src/main/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStatePublisher.java \
        backend/streamflow-processor/src/main/resources/application.properties \
        backend/streamflow-processor/src/test/java/com/streamflow/processor/circuitbreaker/CircuitBreakerStatePublisherTest.java
```

---

## Commit 3 — Add AlertPushConsumer and CircuitBreakerPushConsumer to API gateway

**Message:**
```
SPEC-14: add alert + CB WebSocket push consumers to API gateway

Add three files to streamflow-api:
- AlertWsMessage record: ALERT_FIRED envelope (plan §10 schema)
- CbWsMessage record: CIRCUIT_BREAKER_STATE_CHANGE envelope
- AlertPushConsumer: @KafkaListener on alerts topic →
  /topic/streams/{streamId}/alerts (R1)
- CircuitBreakerPushConsumer: @KafkaListener on cb-events topic →
  /topic/streams/{streamId}/circuit-breaker (R2)

Update KafkaConsumerConfig to add alertListenerContainerFactory and
cbListenerContainerFactory beans (one ConsumerFactory per message
type to avoid generic-type ambiguity). Extract shared props into
baseConsumerProps() helper.

Update WebSocketConfig to override configureWebSocketTransport and
set message/send buffer size to 64 KB (R5).

Add alerts and cb-events topic properties to application.properties.

Refs: specs/SPEC-14-alert-and-cb-websocket-push.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertWsMessage.java
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/CbWsMessage.java
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertPushConsumer.java
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/CircuitBreakerPushConsumer.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/KafkaConsumerConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/WebSocketConfig.java
- backend/streamflow-api/src/main/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertWsMessage.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/websocket/CbWsMessage.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/websocket/AlertPushConsumer.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/websocket/CircuitBreakerPushConsumer.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/config/KafkaConsumerConfig.java \
        backend/streamflow-api/src/main/java/com/streamflow/api/config/WebSocketConfig.java \
        backend/streamflow-api/src/main/resources/application.properties
```

---

## Commit 4 — Add tests for alert + CB WebSocket push

**Message:**
```
SPEC-14: add unit and integration tests for alert/CB WebSocket push

Unit tests (Mockito, no containers):
- AlertPushConsumerTest: verifies correct STOMP destination and
  AlertWsMessage schema (type=ALERT_FIRED, all fields mapped)
- CircuitBreakerPushConsumerTest: verifies correct STOMP destination
  and CbWsMessage schema (type=CIRCUIT_BREAKER_STATE_CHANGE)

Integration test (Testcontainers Kafka + Redis):
- AlertAndCbWebSocketIT: STOMP client subscribes to both topics;
  SimpMessagingTemplate injects payloads directly (same approach as
  StreamApiIT to avoid partition-assignment race); asserts receipt
  and correct schema fields.

Refs: specs/SPEC-14-alert-and-cb-websocket-push.md
```

**Files:**
- backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertPushConsumerTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/websocket/CircuitBreakerPushConsumerTest.java
- backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertAndCbWebSocketIT.java

**Stage command:**
```bash
git add backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertPushConsumerTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/websocket/CircuitBreakerPushConsumerTest.java \
        backend/streamflow-api/src/test/java/com/streamflow/api/websocket/AlertAndCbWebSocketIT.java
```

---

## Commit 5 — Add cb-events to init-topics.sh and update spec

**Message:**
```
SPEC-14: add cb-events topic to init script and mark spec Done

Add create_topic "cb-events" 3 to infra/kafka/init-topics.sh (R3).
Update SPEC-14 spec: status → Done, tick all ACs and DoD, add
evidence section with test output and wire format samples.
Add commit plan to commits/SPEC-14.md.

Refs: specs/SPEC-14-alert-and-cb-websocket-push.md
```

**Files:**
- infra/kafka/init-topics.sh
- specs/SPEC-14-alert-and-cb-websocket-push.md
- commits/SPEC-14.md

**Stage command:**
```bash
git add infra/kafka/init-topics.sh \
        specs/SPEC-14-alert-and-cb-websocket-push.md \
        commits/SPEC-14.md
```

---

## Verification before pushing
- [ ] `mvn -f backend/pom.xml verify` — all 159 tests pass, BUILD SUCCESS
- [ ] `npm --prefix frontend run lint && npm --prefix frontend test && npm --prefix frontend run build` (frontend unchanged in this spec)
- [ ] Demo evidence in spec matches reality
