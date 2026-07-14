package com.streamflow.api.service;

import com.streamflow.api.exception.HistoryRangeException;
import com.streamflow.api.repository.AlertEntity;
import com.streamflow.api.repository.AlertReadRepository;
import com.streamflow.api.repository.BucketHelper;
import com.streamflow.api.repository.MetricSnapshotEntity;
import com.streamflow.api.repository.MetricSnapshotReadRepository;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Service;

/**
 * Service layer for historical metric and alert queries backed by Cassandra.
 *
 * <p>SPEC-18 R4: implements the core read-path logic:
 *
 * <ul>
 *   <li>Validates the requested time range against {@code streamflow.history.max-range-hours}
 *       (default 24 hours). Throws {@link HistoryRangeException} (→ HTTP 400) if exceeded.
 *   <li>For metric snapshots: issues a single partition range query (the {@code metric_snapshots}
 *       table partitions by {@code stream_id} only, so no bucket enumeration is needed).
 *   <li>For alerts: enumerates daily buckets spanning the range and merges results in memory.
 *   <li>Downsamples to HOUR granularity (keeps every 60th row, ascending) when requested.
 *   <li>Caps results at {@value #MAX_POINTS} data points (SPEC-18 NFR2).
 * </ul>
 *
 * <p>SPEC-18 Design Notes: alert severity filtering is applied in-memory (post-fetch) rather than
 * using ALLOW FILTERING in Cassandra.
 *
 * <p>The {@link ConditionalOnBean} annotation ensures this service is skipped when Cassandra is not
 * configured (e.g. in unit tests for other parts of the api module).
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnBean(CassandraOperations.class)
public class HistoryService {

  /** Maximum number of data points returned in a single response (SPEC-18 NFR2). */
  static final int MAX_POINTS = 1500;

  private final MetricSnapshotReadRepository snapshotRepo;
  private final AlertReadRepository alertRepo;

  @Value("${streamflow.history.max-range-hours:24}")
  private int maxRangeHours;

  // ── Metric Snapshots ─────────────────────────────────────────────────────

  /**
   * Returns historical metric snapshots for a stream in the requested time range.
   *
   * <p>SPEC-18 R1: results are ordered ascending by {@code snapshotTs} (oldest first). SPEC-18 R4:
   * HOUR granularity keeps every 60th row (one per hour) from the ascending list.
   *
   * @param streamId the stream to query
   * @param from start of range (inclusive), epoch-milliseconds
   * @param to end of range (inclusive), epoch-milliseconds
   * @param granularity {@code "MINUTE"} (default) or {@code "HOUR"} (downsample)
   * @return list of snapshots ordered ascending; max {@value #MAX_POINTS} entries
   * @throws HistoryRangeException if {@code to - from} exceeds the configured max range
   */
  public List<StreamMetricSnapshotDTO> getHistory(
      String streamId, long from, long to, String granularity) {
    validateRange(from, to);

    // Use a mutable copy so we can sort (repo may return an unmodifiable list)
    List<MetricSnapshotEntity> rows =
        new ArrayList<>(snapshotRepo.findByStreamAndRange(streamId, from, to));

    // Sort ascending (Cassandra returns DESC)
    rows.sort(Comparator.comparing(MetricSnapshotEntity::getMinuteBucket));

    // Downsample to HOUR: keep every 60th row from the sorted list (one per hour)
    if ("HOUR".equalsIgnoreCase(granularity)) {
      rows = downsampleToHour(rows);
    }

    // Cap at MAX_POINTS
    if (rows.size() > MAX_POINTS) {
      rows = rows.subList(0, MAX_POINTS);
    }

    log.debug(
        "SPEC-18: history query: stream={} from={} to={} granularity={} resultCount={}",
        streamId,
        from,
        to,
        granularity,
        rows.size());

    return rows.stream().map(this::toSnapshotDTO).toList();
  }

  // ── Alerts ───────────────────────────────────────────────────────────────

  /**
   * Returns historical alerts for a stream in the requested time range.
   *
   * <p>SPEC-18 R2: results are ordered descending by {@code timestamp} (newest first). Severity
   * filter is applied in-memory.
   *
   * @param streamId the stream to query
   * @param from start of range (inclusive), epoch-milliseconds
   * @param to end of range (inclusive), epoch-milliseconds
   * @param severityFilter optional severity filter (may be null — returns all severities)
   * @return list of alert DTOs ordered DESC; max {@value #MAX_POINTS} entries
   * @throws HistoryRangeException if {@code to - from} exceeds the configured max range
   */
  public List<AlertEventDTO> getAlerts(String streamId, long from, long to, String severityFilter) {
    validateRange(from, to);

    List<String> dayBuckets = BucketHelper.dayBuckets(from, to);
    List<AlertEntity> rows = alertRepo.findByStreamAndRange(streamId, from, to, dayBuckets);

    // Apply severity filter in-memory (SPEC-18 Design Notes §4)
    if (severityFilter != null && !severityFilter.isBlank()) {
      rows = rows.stream().filter(a -> severityFilter.equalsIgnoreCase(a.getSeverity())).toList();
    }

    // Sort DESC (newest first) — merge across buckets may be out of order
    List<AlertEntity> sorted = new ArrayList<>(rows);
    sorted.sort(Comparator.comparing(AlertEntity::getTimestamp).reversed());

    // Cap at MAX_POINTS
    if (sorted.size() > MAX_POINTS) {
      sorted = sorted.subList(0, MAX_POINTS);
    }

    log.debug(
        "SPEC-18: alerts query: stream={} from={} to={} severity={} resultCount={}",
        streamId,
        from,
        to,
        severityFilter,
        sorted.size());

    return sorted.stream().map(this::toAlertDTO).toList();
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  /**
   * Validates that the requested range does not exceed the configured maximum.
   *
   * @throws HistoryRangeException if {@code to - from > maxRangeHours * 3600 * 1000}
   */
  void validateRange(long from, long to) {
    long maxMs = (long) maxRangeHours * 3_600_000L;
    if (to - from > maxMs) {
      throw new HistoryRangeException(from, to, maxRangeHours);
    }
  }

  /**
   * Downsamples a minute-resolution list to hourly resolution.
   *
   * <p>SPEC-18 R4: "every 60 rows → 1" — keeps indices 0, 60, 120, … from the ascending-sorted
   * input. This approximates one row per hour.
   *
   * @param rows ascending-sorted snapshot rows
   * @return downsampled list
   */
  static List<MetricSnapshotEntity> downsampleToHour(List<MetricSnapshotEntity> rows) {
    if (rows.isEmpty()) {
      return rows;
    }
    List<MetricSnapshotEntity> sampled = new ArrayList<>();
    for (int i = 0; i < rows.size(); i += 60) {
      sampled.add(rows.get(i));
    }
    return sampled;
  }

  private StreamMetricSnapshotDTO toSnapshotDTO(MetricSnapshotEntity e) {
    Map<String, Double> qualityDist = buildQualityDist(e);
    return new StreamMetricSnapshotDTO(
        e.getStreamId(),
        e.getStreamId(), // streamName: stub to streamId
        e.getLiveViewerCount() != null ? e.getLiveViewerCount() : 0L,
        0L, // viewerDelta not stored
        e.getBufferRatePct() != null ? e.getBufferRatePct() : 0.0,
        e.getP95LatencyMs() != null ? e.getP95LatencyMs() : 0,
        qualityDist,
        e.getHealthScore() != null ? e.getHealthScore() : 0.0,
        "CLOSED", // CB state not stored per-snapshot
        0, // activeAlerts not stored
        e.getMinuteBucket() != null ? e.getMinuteBucket().toEpochMilli() : 0L);
  }

  private Map<String, Double> buildQualityDist(MetricSnapshotEntity e) {
    return Map.of(
        "1080p", e.getQuality1080pPct() != null ? e.getQuality1080pPct() : 0.0,
        "720p", e.getQuality720pPct() != null ? e.getQuality720pPct() : 0.0,
        "480p", e.getQuality480pPct() != null ? e.getQuality480pPct() : 0.0,
        "360p", e.getQuality360pPct() != null ? e.getQuality360pPct() : 0.0);
  }

  private AlertEventDTO toAlertDTO(AlertEntity e) {
    AlertSeverity severity = parseSeverity(e.getSeverity());
    AlertType alertType = parseAlertType(e.getAlertType());
    long timestampMs = e.getTimestamp() != null ? e.getTimestamp().toEpochMilli() : 0L;
    String alertId = e.getAlertId() != null ? e.getAlertId().toString() : null;

    return new AlertEventDTO(
        alertId,
        e.getStreamId(),
        severity,
        alertType,
        0.0, // threshold not stored
        e.getActualValue() != null ? e.getActualValue() : 0.0,
        e.getMessage(),
        timestampMs);
  }

  private AlertSeverity parseSeverity(String s) {
    if (s == null) return null;
    try {
      return AlertSeverity.valueOf(s);
    } catch (IllegalArgumentException e) {
      log.warn("SPEC-18: unknown severity value '{}' in Cassandra row", s);
      return null;
    }
  }

  private AlertType parseAlertType(String s) {
    if (s == null) return null;
    try {
      return AlertType.valueOf(s);
    } catch (IllegalArgumentException e) {
      log.warn("SPEC-18: unknown alertType value '{}' in Cassandra row", s);
      return null;
    }
  }
}
