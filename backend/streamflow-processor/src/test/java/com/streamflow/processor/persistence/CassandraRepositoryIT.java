package com.streamflow.processor.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for the Cassandra write repositories.
 *
 * <p>SPEC-17 Test Plan: write events/snapshots/alerts, query by partition, assert count + ordering.
 *
 * <p>This test class:
 *
 * <ul>
 *   <li>Starts a real {@link CassandraContainer} (Cassandra 4.1) via Testcontainers.
 *   <li>Uses {@link DynamicPropertySource} to override the contact-points so Spring Data Cassandra
 *       connects to the container port.
 *   <li>Overrides {@code spring.autoconfigure.exclude} to re-enable Cassandra auto-config (which
 *       the test-profile {@code application.properties} disables by default).
 *   <li>Applies the DDL from {@code infra/cassandra/init.cql} programmatically before the Spring
 *       context starts — because the one-shot init container is Docker-only.
 * </ul>
 *
 * <p>Key assertions:
 *
 * <ul>
 *   <li>AC1 – 1000 viewer events written; count query returns ≥ 1000 rows for the partition.
 *   <li>AC2 – One metric snapshot per minute; only one row present after multiple persist() calls.
 *   <li>AC3 – Alert written; row can be read back with matching fields.
 * </ul>
 */
@Testcontainers
@SpringBootTest(
    // Re-enable Cassandra auto-configuration by setting exclude to empty string.
    // The test-profile application.properties disables it; this test needs it enabled.
    properties = {"spring.autoconfigure.exclude=", "spring.cassandra.schema-action=none"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CassandraRepositoryIT {

  @SuppressWarnings("resource")
  @Container
  static final CassandraContainer<?> CASSANDRA =
      new CassandraContainer<>(DockerImageName.parse("cassandra:4.1"))
          .withStartupTimeout(Duration.ofMinutes(3));

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.cassandra.contact-points", CASSANDRA::getHost);
    registry.add("spring.cassandra.port", () -> CASSANDRA.getMappedPort(9042).toString());
    registry.add("spring.cassandra.local-datacenter", () -> "datacenter1");
    registry.add("spring.cassandra.keyspace-name", () -> "streamflow");
  }

  /**
   * Creates the keyspace and tables before the Spring context starts.
   *
   * <p>We use a raw {@link CqlSession} (not Spring's) because the Spring context is not yet live
   * when Testcontainers {@code @BeforeAll} runs.
   */
  @BeforeAll
  static void initSchema() {
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(
                new java.net.InetSocketAddress(CASSANDRA.getHost(), CASSANDRA.getMappedPort(9042)))
            .withLocalDatacenter("datacenter1")
            .build()) {

      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS streamflow "
              + "WITH replication = {'class':'SimpleStrategy','replication_factor':1}");

      session.execute(
          "CREATE TABLE IF NOT EXISTS streamflow.viewer_events ("
              + "  stream_id TEXT, date_bucket TEXT, timestamp TIMESTAMP,"
              + "  event_id UUID, viewer_id TEXT, event_type TEXT,"
              + "  quality TEXT, buffer_ms INT, region TEXT,"
              + "  PRIMARY KEY ((stream_id, date_bucket), timestamp, event_id)"
              + ") WITH CLUSTERING ORDER BY (timestamp DESC)");

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
    }
  }

  @Autowired private CassandraViewerEventRepository viewerEventRepository;

  @Autowired private CassandraMetricSnapshotRepository snapshotRepository;

  @Autowired private CassandraAlertRepository alertRepository;

  @Autowired private CqlSession cqlSession;

  // ── AC1: 1000 viewer events written; count returns ≥ 1000 ─────────────────

  @Test
  void ac1_thousandViewerEventsArePersistedAndQueryable() {
    String streamId = "it-stream-ac1-" + UUID.randomUUID();
    long baseTime = System.currentTimeMillis();
    // All events fall in the same hour bucket
    String dateBucket = CassandraViewerEventRepository.hourBucket(baseTime);

    for (int i = 0; i < 1000; i++) {
      ViewerEventDTO event =
          new ViewerEventDTO(
              UUID.randomUUID().toString(),
              streamId,
              "viewer-" + i,
              EventType.JOIN,
              VideoQuality.Q_1080P,
              null,
              baseTime + i, // distinct millis → distinct timestamps
              "IN-MH");
      viewerEventRepository.persist(event);
    }

    // Wait for all async writes to complete (bounded by semaphore + thread pool)
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              var row =
                  cqlSession
                      .execute(
                          "SELECT COUNT(*) FROM streamflow.viewer_events "
                              + "WHERE stream_id = '"
                              + streamId
                              + "' "
                              + "AND date_bucket = '"
                              + dateBucket
                              + "'")
                      .one();
              assertThat(row).isNotNull();
              assertThat(row.getLong(0))
                  .as("viewer_events count for stream=%s bucket=%s", streamId, dateBucket)
                  .isGreaterThanOrEqualTo(1000L);
            });
  }

  // ── AC1 ordering: latest event is first ───────────────────────────────────

  @Test
  void ac1b_viewerEventsStoredLatestFirst() {
    String streamId = "it-stream-ac1b-" + UUID.randomUUID();
    long earlier = System.currentTimeMillis() - 5000;
    long later = System.currentTimeMillis();

    String bucketEarlier = CassandraViewerEventRepository.hourBucket(earlier);
    String bucketLater = CassandraViewerEventRepository.hourBucket(later);

    // Both events end up in the same bucket (within the same hour)
    String dateBucket = bucketEarlier; // they are the same if within the same hour

    String earlyId = UUID.randomUUID().toString();
    String lateId = UUID.randomUUID().toString();

    viewerEventRepository.persist(
        new ViewerEventDTO(
            earlyId,
            streamId,
            "viewer-early",
            EventType.JOIN,
            VideoQuality.Q_720P,
            null,
            earlier,
            null));
    viewerEventRepository.persist(
        new ViewerEventDTO(
            lateId,
            streamId,
            "viewer-late",
            EventType.JOIN,
            VideoQuality.Q_1080P,
            null,
            later,
            null));

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(
            () -> {
              var rows =
                  cqlSession
                      .execute(
                          "SELECT event_id FROM streamflow.viewer_events "
                              + "WHERE stream_id = '"
                              + streamId
                              + "' "
                              + "AND date_bucket = '"
                              + dateBucket
                              + "' LIMIT 2")
                      .all();
              assertThat(rows).hasSize(2);
              // Descending order: latest event_id (lateId) should be first
              assertThat(rows.get(0).getUuid("event_id").toString())
                  .as("First row should be the LATER event (DESC order)")
                  .isEqualTo(lateId);
            });
  }

  // ── AC2: metric snapshot — one row per minute ─────────────────────────────

  @Test
  void ac2_metricSnapshotWrittenOncePerMinute() {
    String streamId = "it-stream-ac2-" + UUID.randomUUID();
    // Fix minute_bucket at the current minute
    Instant now = Instant.now();
    Instant minuteBucket = now.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
    long baseTs = minuteBucket.toEpochMilli();

    Map<String, Double> qd = Map.of("1080p", 45.0, "720p", 25.0, "480p", 20.0, "360p", 10.0);

    // Simulate SnapshotPublisher calling persist() multiple times within the same minute
    for (int i = 0; i < 5; i++) {
      StreamMetricSnapshotDTO snapshot =
          new StreamMetricSnapshotDTO(
              streamId,
              "Test Stream",
              50000L + i,
              100L + i,
              2.0,
              40,
              qd,
              95.0,
              "CLOSED",
              0,
              baseTs + i * 1000L);
      snapshotRepository.persist(snapshot);
    }

    // Only one row should exist (gated by lastWrittenMinute map)
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(
            () -> {
              var rows =
                  cqlSession
                      .execute(
                          "SELECT live_viewer_count FROM streamflow.metric_snapshots "
                              + "WHERE stream_id = '"
                              + streamId
                              + "'")
                      .all();
              assertThat(rows)
                  .as("Only one snapshot row per minute for stream=%s", streamId)
                  .hasSize(1);
              // The first persist() wins; live_viewer_count = 50000 (i=0)
              assertThat(rows.get(0).getLong("live_viewer_count")).isEqualTo(50000L);
            });
  }

  // ── AC3: alert persisted and queryable ────────────────────────────────────

  @Test
  void ac3_alertPersistedAndQueryableByDateBucket() {
    String streamId = "it-stream-ac3-" + UUID.randomUUID();
    String alertId = UUID.randomUUID().toString();
    long ts = System.currentTimeMillis();
    String dateBucket = CassandraAlertRepository.dayBucket(ts);

    AlertEventDTO alert =
        new AlertEventDTO(
            alertId,
            streamId,
            AlertSeverity.CRITICAL,
            AlertType.HIGH_BUFFER_RATE,
            5.0,
            12.3,
            "Buffer rate 12.3% exceeds threshold 5.0%",
            ts);

    alertRepository.persist(alert);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(
            () -> {
              var rows =
                  cqlSession
                      .execute(
                          "SELECT alert_id, severity, actual_value FROM streamflow.alerts "
                              + "WHERE stream_id = '"
                              + streamId
                              + "' "
                              + "AND date_bucket = '"
                              + dateBucket
                              + "'")
                      .all();
              assertThat(rows).hasSize(1);
              assertThat(rows.get(0).getUuid("alert_id").toString()).isEqualTo(alertId);
              assertThat(rows.get(0).getString("severity")).isEqualTo("CRITICAL");
              assertThat(rows.get(0).getDouble("actual_value")).isEqualTo(12.3);
            });
  }

  // ── Bucket helper tests ────────────────────────────────────────────────────

  @Test
  void hourBucketFormatIsCorrect() {
    // 2024-06-03 14:30:45 UTC → "2024-06-03-14"
    Instant ts = Instant.parse("2024-06-03T14:30:45Z");
    String bucket = CassandraViewerEventRepository.hourBucket(ts.toEpochMilli());
    assertThat(bucket).isEqualTo("2024-06-03-14");
  }

  @Test
  void dayBucketFormatIsCorrect() {
    // 2024-06-03 22:45:00 UTC → "2024-06-03"
    Instant ts = Instant.parse("2024-06-03T22:45:00Z");
    String bucket = CassandraAlertRepository.dayBucket(ts.toEpochMilli());
    assertThat(bucket).isEqualTo("2024-06-03");
  }
}
