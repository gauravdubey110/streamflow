package com.streamflow.api.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for {@link HistoryController} — SPEC-18 AC1–AC4.
 *
 * <p>Seeds Cassandra via a raw {@link CqlSession} before the Spring context starts, then verifies
 * the REST endpoints using {@link RestClient}.
 *
 * <p>Tests:
 *
 * <ul>
 *   <li>AC1 — metric history returns seeded snapshots with non-zero viewer counts, ordered ASC.
 *   <li>AC2 — range exceeding 24h returns HTTP 400 with problem-detail JSON.
 *   <li>AC3 — severity filter on alerts excludes non-matching rows.
 *   <li>AC4 — ETag returned and re-fetch with {@code If-None-Match} returns 304.
 * </ul>
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      // Re-enable Cassandra auto-configuration (excluded in default test profile)
      "spring.autoconfigure.exclude=",
      "spring.cassandra.schema-action=none"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HistoryControllerIT {

  @SuppressWarnings("resource")
  @Container
  static final CassandraContainer<?> CASSANDRA =
      new CassandraContainer<>(DockerImageName.parse("cassandra:4.1"))
          .withStartupTimeout(Duration.ofMinutes(3));

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
    registry.add("spring.cassandra.contact-points", CASSANDRA::getHost);
    registry.add("spring.cassandra.port", () -> CASSANDRA.getMappedPort(9042).toString());
    registry.add("spring.cassandra.local-datacenter", () -> "datacenter1");
    registry.add("spring.cassandra.keyspace-name", () -> "streamflow");
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379).toString());
  }

  /** Creates keyspace + tables and seeds test data before the Spring context starts. */
  @BeforeAll
  static void initSchemaAndSeedData() {
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CASSANDRA.getHost(), CASSANDRA.getMappedPort(9042)))
            .withLocalDatacenter("datacenter1")
            .build()) {

      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS streamflow "
              + "WITH replication = {'class':'SimpleStrategy','replication_factor':1}");

      session.execute(
          "CREATE TABLE IF NOT EXISTS streamflow.metric_snapshots ("
              + "  stream_id TEXT, minute_bucket TIMESTAMP,"
              + "  live_viewer_count BIGINT, buffer_rate_pct DOUBLE,"
              + "  p95_latency_ms INT, health_score DOUBLE,"
              + "  quality_1080p_pct DOUBLE, quality_720p_pct DOUBLE,"
              + "  quality_480p_pct DOUBLE, quality_360p_pct DOUBLE,"
              + "  PRIMARY KEY (stream_id, minute_bucket)"
              + ") WITH CLUSTERING ORDER BY (minute_bucket DESC)");

      session.execute(
          "CREATE TABLE IF NOT EXISTS streamflow.alerts ("
              + "  stream_id TEXT, date_bucket TEXT, timestamp TIMESTAMP,"
              + "  alert_id UUID, severity TEXT, alert_type TEXT,"
              + "  message TEXT, actual_value DOUBLE, resolved_at TIMESTAMP,"
              + "  PRIMARY KEY ((stream_id, date_bucket), timestamp, alert_id)"
              + ") WITH CLUSTERING ORDER BY (timestamp DESC)");

      // Seed 30 metric snapshots (one per minute, last 30 minutes)
      Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);
      for (int i = 0; i < 30; i++) {
        Instant bucket = now.minus(i, ChronoUnit.MINUTES);
        session.execute(
            "INSERT INTO streamflow.metric_snapshots "
                + "(stream_id, minute_bucket, live_viewer_count, buffer_rate_pct, "
                + "p95_latency_ms, health_score, quality_1080p_pct, quality_720p_pct, "
                + "quality_480p_pct, quality_360p_pct) "
                + "VALUES ('it-stream', "
                + bucket.toEpochMilli()
                + ", "
                + (50000L + i)
                + ", 1.5, 40, 99.0, 50.0, 30.0, 15.0, 5.0)");
      }

      // Seed 5 CRITICAL and 3 INFO alerts
      String today = now.toString().substring(0, 10); // yyyy-MM-dd
      for (int i = 0; i < 5; i++) {
        Instant ts = now.minus(i, ChronoUnit.MINUTES);
        session.execute(
            "INSERT INTO streamflow.alerts "
                + "(stream_id, date_bucket, timestamp, alert_id, severity, alert_type, "
                + "message, actual_value) VALUES ('it-stream', '"
                + today
                + "', "
                + ts.toEpochMilli()
                + ", "
                + UUID.randomUUID()
                + ", 'CRITICAL', "
                + "'HIGH_BUFFER_RATE', 'test critical alert', 9.5)");
      }
      for (int i = 0; i < 3; i++) {
        Instant ts = now.minus(i + 10, ChronoUnit.MINUTES);
        session.execute(
            "INSERT INTO streamflow.alerts "
                + "(stream_id, date_bucket, timestamp, alert_id, severity, alert_type, "
                + "message, actual_value) VALUES ('it-stream', '"
                + today
                + "', "
                + ts.toEpochMilli()
                + ", "
                + UUID.randomUUID()
                + ", 'INFO', "
                + "'VIEWER_DROP', 'test info alert', 1.1)");
      }
    }
  }

  @LocalServerPort private int port;

  // ── AC1: history returns seeded snapshots ordered ASC ────────────────────

  /**
   * AC1: GET /history?from=...&to=...&granularity=MINUTE returns ~30 entries, all with non-zero
   * viewer counts, ordered ascending by timestamp.
   */
  @Test
  void ac1_historyEndpoint_returnsSeededSnapshots_orderedAscending() {
    long to = Instant.now().toEpochMilli();
    long from = Instant.now().minus(35, ChronoUnit.MINUTES).toEpochMilli();

    RestClient client = RestClient.builder().baseUrl("http://localhost:" + port).build();

    List<?> raw =
        client
            .get()
            .uri(
                "/api/v1/streams/it-stream/history?from={from}&to={to}&granularity=MINUTE",
                from,
                to)
            .retrieve()
            .body(List.class);

    assertThat(raw).isNotNull().isNotEmpty();

    // Re-parse to typed DTO list via Jackson
    com.fasterxml.jackson.databind.ObjectMapper om =
        new com.fasterxml.jackson.databind.ObjectMapper();
    String json = null;
    try {
      json = om.writeValueAsString(raw);
      StreamMetricSnapshotDTO[] dtos = om.readValue(json, StreamMetricSnapshotDTO[].class);
      assertThat(dtos.length).isGreaterThanOrEqualTo(10); // at least 10 of 30 seeded

      // Non-zero viewer counts
      for (StreamMetricSnapshotDTO dto : dtos) {
        assertThat(dto.liveViewerCount()).isPositive();
      }

      // Ascending order
      for (int i = 1; i < dtos.length; i++) {
        assertThat(dtos[i].snapshotTs())
            .as("snapshot[%d] should be >= snapshot[%d]", i, i - 1)
            .isGreaterThanOrEqualTo(dtos[i - 1].snapshotTs());
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse history response", e);
    }
  }

  // ── AC2: range > 24h returns 400 ─────────────────────────────────────────

  /** AC2: Out-of-range request returns HTTP 400 with descriptive error body. */
  @Test
  void ac2_rangeExceeding24h_returns400() {
    long to = Instant.now().toEpochMilli();
    long from = to - (25L * 3_600_000L); // 25 hours

    RestClient client = RestClient.builder().baseUrl("http://localhost:" + port).build();

    ResponseEntity<String> response =
        client
            .get()
            .uri("/api/v1/streams/it-stream/history?from={from}&to={to}", from, to)
            .retrieve()
            .onStatus(
                status -> status.value() == 400,
                (req, res) -> {
                  /* suppress throw */
                })
            .toEntity(String.class);

    assertThat(response.getStatusCode().value()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    // Body should contain the problem-detail title
    assertThat(response.getBody()).containsIgnoringCase("range");
  }

  // ── AC3: severity filter on alerts ────────────────────────────────────────

  /** AC3: Severity filter on alerts works — only CRITICAL alerts returned when requested. */
  @Test
  void ac3_severityFilter_returnsCriticalAlertsOnly() {
    long to = Instant.now().toEpochMilli();
    long from = Instant.now().minus(60, ChronoUnit.MINUTES).toEpochMilli();

    RestClient client = RestClient.builder().baseUrl("http://localhost:" + port).build();

    List<?> raw =
        client
            .get()
            .uri("/api/v1/streams/it-stream/alerts?from={from}&to={to}&severity=CRITICAL", from, to)
            .retrieve()
            .body(List.class);

    assertThat(raw).isNotNull().isNotEmpty();

    com.fasterxml.jackson.databind.ObjectMapper om =
        new com.fasterxml.jackson.databind.ObjectMapper();
    try {
      String json = om.writeValueAsString(raw);
      AlertEventDTO[] dtos = om.readValue(json, AlertEventDTO[].class);
      // Should have CRITICAL alerts (we seeded 5)
      assertThat(dtos.length).isGreaterThanOrEqualTo(1);
      for (AlertEventDTO dto : dtos) {
        assertThat(dto.severity()).hasToString("CRITICAL");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse alerts response", e);
    }
  }

  // ── AC4: ETag and 304 ─────────────────────────────────────────────────────

  /**
   * AC4: ETag is returned on the first call; re-fetch with {@code If-None-Match} matching the ETag
   * returns HTTP 304.
   */
  @Test
  void ac4_etag_and304_roundTrip() {
    long to = Instant.now().toEpochMilli();
    long from = Instant.now().minus(35, ChronoUnit.MINUTES).toEpochMilli();

    RestClient client = RestClient.builder().baseUrl("http://localhost:" + port).build();

    // First call — get the ETag
    ResponseEntity<Void> first =
        client
            .get()
            .uri("/api/v1/streams/it-stream/history?from={from}&to={to}", from, to)
            .retrieve()
            .toBodilessEntity();

    assertThat(first.getStatusCode().value()).isEqualTo(200);
    String etag = first.getHeaders().getETag();
    assertThat(etag).isNotNull().startsWith("W/\"");

    // Second call with If-None-Match — expect 304
    ResponseEntity<Void> second =
        client
            .get()
            .uri(
                "/api/v1/streams/it-stream/history?from={from}&to={to}&If-None-Match={etag}",
                from,
                to,
                etag)
            .retrieve()
            .onStatus(
                status -> status.value() == 304,
                (req, res) -> {
                  /* ok */
                })
            .toBodilessEntity();

    assertThat(second.getStatusCode().value()).isEqualTo(304);
  }
}
