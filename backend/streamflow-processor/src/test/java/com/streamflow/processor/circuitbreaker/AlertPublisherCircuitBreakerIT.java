package com.streamflow.processor.circuitbreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import com.streamflow.processor.alert.AlertPublisher;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for the Resilience4j circuit breaker on {@link AlertPublisher}.
 *
 * <p>SPEC-12 Test Plan — Integration:
 *
 * <ul>
 *   <li>Stub the {@code alertKafkaTemplate} to throw a {@link RuntimeException} on every {@code
 *       send(...)} call, simulating a broker brownout.
 *   <li>Call {@link AlertPublisher#publish(AlertEventDTO)} enough times to exceed the {@code
 *       minimumNumberOfCalls} threshold with a 100% failure rate → CB trips to OPEN.
 *   <li>Verify the subsequent call is routed to the fallback (SPEC-12 R3/R4): the alert ID appears
 *       in {@code dropped_alerts:{streamId}} in Redis.
 *   <li>Verify {@code cb_state:alert_processor} in Redis is set to {@code "OPEN"} (SPEC-12 R5).
 * </ul>
 *
 * <p>The test uses a short CB config (sliding-window-size=10, min-calls=5) injected via {@link
 * DynamicPropertySource} to keep the test fast. Redis is a real Testcontainers instance. Kafka is
 * also Testcontainers (needed for context startup), but its template is mocked.
 *
 * <p>Cassandra auto-config is excluded in {@code application.properties}.
 */
@Testcontainers
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AlertPublisherCircuitBreakerIT {

  @SuppressWarnings("resource")
  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

  @SuppressWarnings("resource")
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379).toString());
    // Override CB thresholds to trip faster in tests (SPEC-12 IT)
    registry.add(
        "resilience4j.circuitbreaker.instances.alertProcessor.sliding-window-size", () -> "10");
    registry.add(
        "resilience4j.circuitbreaker.instances.alertProcessor.minimum-number-of-calls", () -> "5");
    registry.add(
        "resilience4j.circuitbreaker.instances.alertProcessor.failure-rate-threshold", () -> "50");
    registry.add(
        "resilience4j.circuitbreaker.instances.alertProcessor.wait-duration-in-open-state",
        () -> "30s");
  }

  /**
   * Replace the real alertKafkaTemplate with a mock that always fails. This is the "broken Kafka"
   * scenario for the circuit breaker test.
   *
   * <p>Note: we mock at the bean level because the CB annotation proxies the {@link
   * AlertPublisher#publish} method — the mock failure must be thrown synchronously (or the
   * CompletableFuture must complete exceptionally) to be counted as a CB failure. We make the
   * send() method throw synchronously.
   */
  @MockBean(name = "alertKafkaTemplate")
  private KafkaTemplate<String, AlertEventDTO> alertKafkaTemplate;

  /** Suppress the SnapshotPublisher scheduler so it does not interfere. */
  @MockBean
  @SuppressWarnings("unused")
  private com.streamflow.processor.snapshot.SnapshotPublisher snapshotPublisher;

  @Autowired private AlertPublisher alertPublisher;

  @Autowired private RedisTemplate<String, String> redisTemplate;

  @Autowired private AlertProcessorCircuitBreaker alertProcessorCircuitBreaker;

  @BeforeEach
  void setUp() {
    // Flush Redis state between tests
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

    // Make every KafkaTemplate.send() throw a RuntimeException synchronously
    // so Resilience4j counts it as a failure within the decorated method.
    when(alertKafkaTemplate.send(anyString(), anyString(), any(AlertEventDTO.class)))
        .thenThrow(new RuntimeException("Simulated Kafka broker failure"));
  }

  /**
   * AC1 (adapted for IT): After enough failing calls, the CB transitions to OPEN and subsequent
   * calls are routed to the fallback which stores to dropped_alerts.
   *
   * <p>Also covers:
   *
   * <ul>
   *   <li>AC3: while OPEN, dropped alerts accumulate in {@code dropped_alerts:{streamId}}.
   *   <li>R5: {@code cb_state:alert_processor} = {@code "OPEN"} in Redis.
   * </ul>
   */
  @Test
  void whenKafkaFails_cbOpens_fallbackStoresDroppedAlerts() {
    String streamId = "it-cb-" + UUID.randomUUID();

    // Call publish() enough times to exceed minimumNumberOfCalls (5) with 100% failure rate
    // → CB should trip to OPEN after 5 failures
    for (int i = 0; i < 12; i++) {
      AlertEventDTO alert = buildAlert(streamId, "alert-" + i);
      alertPublisher.publish(alert);
    }

    // SPEC-12 R5: CB state key in Redis should be OPEN
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              String redisState =
                  redisTemplate.opsForValue().get(AlertProcessorCircuitBreaker.CB_STATE_KEY);
              assertThat(redisState)
                  .as("cb_state:alert_processor should be OPEN after failures")
                  .isEqualTo("OPEN");
            });

    // SPEC-12 AC3 / R4: dropped_alerts:{streamId} list should have entries
    List<String> dropped =
        redisTemplate.opsForList().range(AlertPublisher.DROPPED_KEY_PREFIX + streamId, 0, -1);
    assertThat(dropped)
        .as("dropped_alerts:{streamId} should contain alert IDs after CB opens")
        .isNotEmpty();
  }

  /**
   * SPEC-12 R4: dropped alerts list is capped at 100 entries (LTRIM).
   *
   * <p>This test verifies the cap by calling fallback directly (bypassing the CB proxy) since
   * waiting for 100+ real failures would be too slow for a unit/IT test.
   */
  @Test
  @SuppressWarnings("unchecked")
  void fallback_droppedAlertsList_cappedAt100() {
    String streamId = "it-cap-" + UUID.randomUUID();
    String droppedKey = AlertPublisher.DROPPED_KEY_PREFIX + streamId;

    // Call fallback directly 110 times
    for (int i = 0; i < 110; i++) {
      AlertEventDTO alert = buildAlert(streamId, "alert-cap-" + i);
      alertPublisher.publishFallback(alert, new RuntimeException("test"));
    }

    Long listSize = redisTemplate.opsForList().size(droppedKey);
    assertThat(listSize)
        .as(
            "dropped_alerts list should be capped at %d (SPEC-12 R4)",
            AlertPublisher.DROPPED_ALERTS_MAX_SIZE)
        .isLessThanOrEqualTo((long) AlertPublisher.DROPPED_ALERTS_MAX_SIZE);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static AlertEventDTO buildAlert(String streamId, String alertId) {
    return new AlertEventDTO(
        alertId,
        streamId,
        AlertSeverity.CRITICAL,
        AlertType.HIGH_BUFFER_RATE,
        10.0,
        12.5,
        "Test alert for CB IT",
        System.currentTimeMillis());
  }
}
