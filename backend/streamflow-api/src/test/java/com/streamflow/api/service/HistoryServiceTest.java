package com.streamflow.api.service;

import com.streamflow.api.exception.HistoryRangeException;
import com.streamflow.api.repository.AlertEntity;
import com.streamflow.api.repository.AlertReadRepository;
import com.streamflow.api.repository.MetricSnapshotEntity;
import com.streamflow.api.repository.MetricSnapshotReadRepository;
import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HistoryService}.
 *
 * <p>SPEC-18 Test Plan: verifies range validation, HOUR downsampling, severity filtering,
 * MAX_POINTS cap, and ASC/DESC ordering — all without requiring a running Cassandra instance.
 */
@ExtendWith(MockitoExtension.class)
class HistoryServiceTest {

    @Mock
    private MetricSnapshotReadRepository snapshotRepo;

    @Mock
    private AlertReadRepository alertRepo;

    private HistoryService service;

    @BeforeEach
    void setUp() {
        service = new HistoryService(snapshotRepo, alertRepo);
        ReflectionTestUtils.setField(service, "maxRangeHours", 24);
    }

    // ── validateRange ─────────────────────────────────────────────────────────

    @Test
    void validateRange_withinMax_doesNotThrow() {
        long from = System.currentTimeMillis();
        long to   = from + 23 * 3_600_000L;
        service.validateRange(from, to);   // must not throw
    }

    @Test
    void validateRange_equalToMax_doesNotThrow() {
        long from = System.currentTimeMillis();
        long to   = from + 24 * 3_600_000L;
        service.validateRange(from, to);   // exactly 24h — within limit
    }

    @Test
    void validateRange_exceedsMax_throwsHistoryRangeException() {
        long from = System.currentTimeMillis();
        long to   = from + 25 * 3_600_000L;   // 25 hours
        assertThatThrownBy(() -> service.validateRange(from, to))
                .isInstanceOf(HistoryRangeException.class)
                .hasMessageContaining("24 hours");
    }

    @Test
    void validateRange_fromEqualsTo_doesNotThrow() {
        long ts = System.currentTimeMillis();
        service.validateRange(ts, ts);   // range = 0ms, must not throw
    }

    // ── getHistory — ordering ─────────────────────────────────────────────────

    @Test
    void getHistory_resultsReturnedAscending() {
        Instant now   = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        Instant older = now.minus(2, ChronoUnit.MINUTES);

        // Repo returns DESC (as Cassandra would)
        when(snapshotRepo.findByStreamAndRange(anyString(), anyLong(), anyLong()))
                .thenReturn(List.of(
                        buildSnapshot("s1", now, 200L),
                        buildSnapshot("s1", older, 100L)
                ));

        long from = now.minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long to   = now.toEpochMilli();

        List<StreamMetricSnapshotDTO> results = service.getHistory("s1", from, to, "MINUTE");

        assertThat(results).hasSize(2);
        assertThat(results.get(0).snapshotTs()).isLessThan(results.get(1).snapshotTs());
        assertThat(results.get(0).liveViewerCount()).isEqualTo(100L);
        assertThat(results.get(1).liveViewerCount()).isEqualTo(200L);
    }

    // ── getHistory — HOUR downsampling ────────────────────────────────────────

    @Test
    void getHistory_hourGranularity_keeps_every60thRow() {
        Instant base = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        // 125 minute-resolution rows
        List<MetricSnapshotEntity> minuteRows = new ArrayList<>();
        for (int i = 0; i < 125; i++) {
            minuteRows.add(buildSnapshot("s1", base.plus(i, ChronoUnit.MINUTES), (long) i));
        }
        when(snapshotRepo.findByStreamAndRange(anyString(), anyLong(), anyLong()))
                .thenReturn(minuteRows);

        long from = base.toEpochMilli();
        long to   = base.plus(125, ChronoUnit.MINUTES).toEpochMilli();

        List<StreamMetricSnapshotDTO> results = service.getHistory("s1", from, to, "HOUR");

        // indices 0, 60, 120 → 3 entries
        assertThat(results).hasSize(3);
        assertThat(results.get(0).liveViewerCount()).isEqualTo(0L);
        assertThat(results.get(1).liveViewerCount()).isEqualTo(60L);
        assertThat(results.get(2).liveViewerCount()).isEqualTo(120L);
    }

    // ── getHistory — MAX_POINTS cap ───────────────────────────────────────────

    @Test
    void getHistory_moreThanMaxPoints_isTruncatedTo1500() {
        Instant base = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        // Build 2000 rows but use a range within 24h (from=now-23h, to=now)
        // The mock returns 2000 rows regardless; service should cap at MAX_POINTS.
        List<MetricSnapshotEntity> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            rows.add(buildSnapshot("s1", base.plus(i, ChronoUnit.MINUTES), (long) i));
        }
        when(snapshotRepo.findByStreamAndRange(anyString(), anyLong(), anyLong()))
                .thenReturn(rows);

        // Range is within 24h (23 hours) — validation passes
        long from = base.toEpochMilli();
        long to   = base.plus(23, ChronoUnit.HOURS).toEpochMilli();

        List<StreamMetricSnapshotDTO> results = service.getHistory("s1", from, to, "MINUTE");

        assertThat(results).hasSize(HistoryService.MAX_POINTS);
    }

    // ── getAlerts — severity filter ───────────────────────────────────────────

    @Test
    void getAlerts_severityFilter_excludesNonMatchingAlerts() {
        when(alertRepo.findByStreamAndRange(anyString(), anyLong(), anyLong(), anyList()))
                .thenReturn(List.of(
                        buildAlert("s1", "CRITICAL", Instant.now()),
                        buildAlert("s1", "WARNING", Instant.now().minus(1, ChronoUnit.SECONDS)),
                        buildAlert("s1", "CRITICAL", Instant.now().minus(2, ChronoUnit.SECONDS))
                ));

        long from = Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long to   = Instant.now().toEpochMilli();

        List<AlertEventDTO> results = service.getAlerts("s1", from, to, "CRITICAL");

        assertThat(results).hasSize(2);
        results.forEach(a -> assertThat(a.severity()).hasToString("CRITICAL"));
    }

    @Test
    void getAlerts_noSeverityFilter_returnsAllAlerts() {
        when(alertRepo.findByStreamAndRange(anyString(), anyLong(), anyLong(), anyList()))
                .thenReturn(List.of(
                        buildAlert("s1", "CRITICAL", Instant.now()),
                        buildAlert("s1", "INFO", Instant.now().minus(1, ChronoUnit.SECONDS))
                ));

        long from = Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long to   = Instant.now().toEpochMilli();

        List<AlertEventDTO> results = service.getAlerts("s1", from, to, null);

        assertThat(results).hasSize(2);
    }

    @Test
    void getAlerts_resultsReturnedDescending() {
        Instant now    = Instant.now();
        Instant older  = now.minus(5, ChronoUnit.SECONDS);

        when(alertRepo.findByStreamAndRange(anyString(), anyLong(), anyLong(), anyList()))
                .thenReturn(List.of(
                        buildAlert("s1", "CRITICAL", older),
                        buildAlert("s1", "WARNING",  now)
                ));

        long from = now.minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long to   = now.toEpochMilli();

        List<AlertEventDTO> results = service.getAlerts("s1", from, to, null);

        assertThat(results).hasSize(2);
        // DESC: newest (now) first
        assertThat(results.get(0).timestamp()).isGreaterThan(results.get(1).timestamp());
    }

    // ── from == to edge case ──────────────────────────────────────────────────

    @Test
    void getHistory_fromEqualsTo_returnsEmptyList() {
        when(snapshotRepo.findByStreamAndRange(anyString(), anyLong(), anyLong()))
                .thenReturn(List.of());

        long ts = System.currentTimeMillis();
        List<StreamMetricSnapshotDTO> results = service.getHistory("s1", ts, ts, "MINUTE");
        assertThat(results).isEmpty();
    }

    // ── downsampleToHour static helper ────────────────────────────────────────

    @Test
    void downsampleToHour_emptyList_returnsEmpty() {
        assertThat(HistoryService.downsampleToHour(List.of())).isEmpty();
    }

    @Test
    void downsampleToHour_lessThan60_returnsSingleEntry() {
        List<MetricSnapshotEntity> rows = new ArrayList<>();
        for (int i = 0; i < 59; i++) {
            rows.add(buildSnapshot("s1", Instant.now().plus(i, ChronoUnit.MINUTES), (long) i));
        }
        List<MetricSnapshotEntity> result = HistoryService.downsampleToHour(rows);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLiveViewerCount()).isEqualTo(0L);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private MetricSnapshotEntity buildSnapshot(String streamId, Instant minuteBucket, long viewers) {
        return MetricSnapshotEntity.builder()
                .streamId(streamId)
                .minuteBucket(minuteBucket)
                .liveViewerCount(viewers)
                .bufferRatePct(1.5)
                .p95LatencyMs(40)
                .healthScore(99.0)
                .quality1080pPct(50.0)
                .quality720pPct(30.0)
                .quality480pPct(15.0)
                .quality360pPct(5.0)
                .build();
    }

    private AlertEntity buildAlert(String streamId, String severity, Instant timestamp) {
        return AlertEntity.builder()
                .streamId(streamId)
                .dateBucket("2024-06-03")
                .timestamp(timestamp)
                .alertId(UUID.randomUUID())
                .severity(severity)
                .alertType("HIGH_BUFFER_RATE")
                .message("test alert")
                .actualValue(8.5)
                .build();
    }
}
