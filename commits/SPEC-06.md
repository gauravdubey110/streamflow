# Commit Plan — SPEC-06: API Gateway REST + WebSocket + Metrics Push

Suggested branch: feat/spec-06-api-websocket-metrics-push

---

## Commit 1 — Add springdoc + Testcontainers deps to streamflow-api pom

**Message:**
```
SPEC-06: add springdoc-openapi and Testcontainers deps to streamflow-api

Add springdoc-openapi-starter-webmvc-ui 2.3.0 for OpenAPI docs at
/v3/api-docs (DoD requirement). Add testcontainers/junit-jupiter and
testcontainers/kafka for integration tests. Configure maven-surefire-plugin
to pick up *IT.java test classes and pass DOCKER_HOST + Ryuk-disabled
env vars (mirrors the processor module pattern from SPEC-05).

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/pom.xml

**Stage command:**
```bash
git add backend/streamflow-api/pom.xml
```

---

## Commit 2 — Add application.properties for streamflow-api

**Message:**
```
SPEC-06: configure application.properties for streamflow-api

Exclude Cassandra autoconfiguration (not needed until SPEC-17) to
prevent startup failure when Cassandra is not running. Add
streamflow.kafka.topics.metrics-aggregated property, cors.allowed-origins
for Vite dev server, and springdoc paths.

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/src/main/resources/application.properties

**Stage command:**
```bash
git add backend/streamflow-api/src/main/resources/application.properties
```

---

## Commit 3 — Implement config beans (WebSocket, Kafka, Redis, CORS, Jackson)

**Message:**
```
SPEC-06: add WebSocket, Kafka consumer, Redis, CORS, and Jackson config

WebSocketConfig: STOMP at /ws with SockJS, in-memory broker on /topic.
KafkaConsumerConfig: ConsumerFactory<String, StreamMetricSnapshotDTO>
  for api-gateway-group; concurrency=1.
RedisConfig: RedisTemplate<String, String> with @Primary to override
  Spring Boot's auto-configured stringRedisTemplate.
CorsConfig: WebMvcConfigurer allowing http://localhost:5173 on /api/**.
JacksonConfig: documentation/extension-point class (no bean declared;
  ObjectMapper provided by JacksonAutoConfiguration).

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/config/WebSocketConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/KafkaConsumerConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/RedisConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/CorsConfig.java
- backend/streamflow-api/src/main/java/com/streamflow/api/config/JacksonConfig.java

**Stage command:**
```bash
git add \
  backend/streamflow-api/src/main/java/com/streamflow/api/config/WebSocketConfig.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/config/KafkaConsumerConfig.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/config/RedisConfig.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/config/CorsConfig.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/config/JacksonConfig.java
```

---

## Commit 4 — Implement REST layer (DTO, service, controller, exception handler)

**Message:**
```
SPEC-06: add StreamSummaryDTO, StreamService, StreamController, exceptions

StreamSummaryDTO: thin projection record (streamId, streamName,
  liveViewerCount, healthScore, activeAlerts, circuitBreakerState).
StreamService: reads active_streams SMEMBERS from Redis, deserialises
  stream_snapshot:{id} JSON, builds summaries and full snapshots.
StreamController: GET /api/v1/streams and GET /api/v1/streams/{streamId}
  with @PathVariable("streamId") explicit name (Spring Boot 3.2+).
StreamNotFoundException: maps to HTTP 404.
GlobalExceptionHandler: @RestControllerAdvice returning RFC-7807
  ProblemDetail for StreamNotFoundException.

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/dto/StreamSummaryDTO.java
- backend/streamflow-api/src/main/java/com/streamflow/api/service/StreamService.java
- backend/streamflow-api/src/main/java/com/streamflow/api/controller/StreamController.java
- backend/streamflow-api/src/main/java/com/streamflow/api/exception/StreamNotFoundException.java
- backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java

**Stage command:**
```bash
git add \
  backend/streamflow-api/src/main/java/com/streamflow/api/dto/StreamSummaryDTO.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/service/StreamService.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/controller/StreamController.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/exception/StreamNotFoundException.java \
  backend/streamflow-api/src/main/java/com/streamflow/api/exception/GlobalExceptionHandler.java
```

---

## Commit 5 — Implement MetricsPushConsumer (Kafka → WebSocket)

**Message:**
```
SPEC-06: add MetricsPushConsumer — Kafka metrics-aggregated to STOMP

Consumes StreamMetricSnapshotDTO from metrics-aggregated topic under
api-gateway-group and broadcasts each snapshot to the STOMP destination
/topic/streams/{streamId}/metrics via SimpMessagingTemplate. This is
the core real-time push path for the React frontend.

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/src/main/java/com/streamflow/api/websocket/MetricsPushConsumer.java

**Stage command:**
```bash
git add backend/streamflow-api/src/main/java/com/streamflow/api/websocket/MetricsPushConsumer.java
```

---

## Commit 6 — Add StreamApiIT integration test

**Message:**
```
SPEC-06: add StreamApiIT covering REST, CORS, and STOMP WebSocket

@SpringBootTest(RANDOM_PORT) with Testcontainers Kafka + Redis.
Tests cover:
- AC1: GET /api/v1/streams returns seeded stream summaries
- AC1 edge: empty list when no active streams
- R4: GET /api/v1/streams/{id} returns full snapshot or 404
- AC4: CORS preflight from http://localhost:5173 succeeds
- AC2: STOMP subscriber receives snapshot pushed via SimpMessagingTemplate
       (direct broker push — avoids Kafka consumer race condition in tests)

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- backend/streamflow-api/src/test/java/com/streamflow/api/StreamApiIT.java

**Stage command:**
```bash
git add backend/streamflow-api/src/test/java/com/streamflow/api/StreamApiIT.java
```

---

## Commit 7 — Update SPEC-06 to Done and add evidence

**Message:**
```
SPEC-06: mark Done, add evidence and commit plan

All acceptance criteria verified:
- AC1: curl output showing 3 active streams
- AC2: STOMP subscriber test passes
- AC3/R4: 404 RFC-7807 response for unknown stream
- AC4: CORS preflight returns Access-Control-Allow-Origin header
- DoD: /v3/api-docs returns OpenAPI JSON

Refs: specs/SPEC-06-api-websocket-and-metrics-push.md
```

**Files:**
- specs/SPEC-06-api-websocket-and-metrics-push.md
- commits/SPEC-06.md

**Stage command:**
```bash
git add specs/SPEC-06-api-websocket-and-metrics-push.md commits/SPEC-06.md
```

---

## Verification before pushing

- [ ] `mvn -f backend/pom.xml verify` — all 6 StreamApiIT tests pass, full build green
- [ ] `curl http://localhost:8080/api/v1/streams` — returns JSON array
- [ ] `curl http://localhost:8080/api/v1/streams/nonexistent` — returns 404 RFC-7807
- [ ] `curl http://localhost:8080/v3/api-docs` — returns OpenAPI JSON
- [ ] CORS preflight OPTIONS to `/api/v1/streams` with `Origin: http://localhost:5173` returns `Access-Control-Allow-Origin: http://localhost:5173`
- [ ] Demo evidence in spec matches reality
