# Backend — module boundaries and conventions

Integration tests use Testcontainers (Kafka, Redis, Cassandra) and require Docker. Files ending in
`IT.java` are integration tests; plain `Test.java` are unit tests — both run under `mvn verify`
(Surefire) since no separate failsafe binding is configured.

### Module boundaries

- **streamflow-common**: pure library, no Spring app. Holds cross-module contracts: `dto/*DTO`
  (Java records, e.g. `ViewerEventDTO`, `StreamMetricSnapshotDTO`, `AlertEventDTO`,
  `CbStateEventDTO`), `enums/*` (`EventType`, `AlertType`, `AlertSeverity`, `VideoQuality`), and
  `constants/KafkaTopics`. Every runnable module depends on it (declared once in the parent POM's
  `dependencyManagement`). Changing a DTO here has cross-cutting impact — check producer/processor
  serialization and API deserialization together.
- **streamflow-producer**: generates synthetic viewer/health traffic (`simulator/*`) via a
  strategy pattern (`strategy/EventGenerationStrategy` → `NormalLoadStrategy` /
  `ChaosAwareStrategy`). Chaos scenarios (`chaos/ChaosInjector`, `ChaosScenario`, `ChaosState`) are
  toggled through an internal REST endpoint (`rest/InternalChaosController`) that the API gateway
  proxies to.
- **streamflow-processor**: the core aggregation engine. Kafka `@KafkaListener`s
  (`consumer/ViewerEventConsumer`, `StreamHealthConsumer`) update Redis structures
  (sorted sets for live viewers, hashes for health telemetry), feed `aggregator/*`
  (`ViewerCountAggregator`, `QualityDistAggregator`, `HealthScoreCalculator`), and drive
  `alert/AlertEngine` (rule-based: `BitrateDegradationRule`, `HighBufferRateRule`,
  `ViewerDropRule`) and `circuitbreaker/AlertProcessorCircuitBreaker` (Resilience4j, state mirrored
  to Redis + published as a Spring `ApplicationEvent`). `snapshot/SnapshotPublisher` is the hub: a
  `@Scheduled(fixedRate = 1000)` job that reads all the above per active stream, builds a
  `StreamMetricSnapshotDTO`, writes it to Redis (`stream_snapshot:{streamId}`, 30 s TTL), publishes
  it to `metrics-aggregated`, and (optionally, rate-limited) persists it to Cassandra via
  `persistence/*` repositories. Active-stream discovery is Redis-set-based
  (`SADD`/`SMEMBERS active_streams`), not a static registry.
- **streamflow-api**: stateless gateway. `controller/StreamController` and `HistoryController`
  serve REST reads (live snapshot from Redis, history from Cassandra — the latter conditional on a
  `CassandraOperations` bean so it can be disabled in tests). `websocket/*PushConsumer` classes are
  Kafka listeners that re-publish `metrics-aggregated`/`alerts`/`cb-events` onto STOMP
  `/topic/**` destinations (`config/WebSocketConfig`, endpoint `/ws`, SockJS, 64 KB message limit).
  `chaos/ProducerChaosClient` proxies chaos-toggle requests to the producer's internal endpoint.

Cross-cutting patterns to preserve when extending this code:
- DTOs are immutable Java records; Optional-wrapped beans (e.g.
  `Optional<CassandraViewerEventRepository>`) are the convention for making Cassandra persistence
  toggle-able without conditional-bean plumbing at every call site.
- Redis is the source of truth for "live" state (sorted sets, hashes, short-TTL snapshot strings);
  Cassandra is for historical replay only and is written at a reduced rate, not on every event.
  Redis key prefixes/patterns are declared as constants near their owning class
  (`stream_snapshot:`, `stream_health:`, `cb_state:alert_processor`, `active_streams`).
  Grep for a prefix before renaming — several classes across processor/api share the same key.
- Manual Kafka acknowledgment (`Acknowledgment.acknowledge()`) is only called after the
  corresponding Redis/Cassandra write succeeds; exceptions propagate to trigger the configured
  retry + dead-letter-topic behavior in `config/KafkaConsumerConfig`.
- History endpoints (`HistoryController`) use weak ETags (`computeEtag`) + `Cache-Control` for
  conditional 304 responses, and enforce a max query-range via
  `streamflow.history.max-range-hours` → `HistoryRangeException` → 400.
