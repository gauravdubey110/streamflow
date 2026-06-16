package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link HighBufferRateRule}.
 *
 * <p>SPEC-11 Test Plan — Unit: 3+ scenarios per rule (below, at, above threshold).
 *
 * <p>RedisTemplate is not used by this rule; it is passed as a mock.
 */
class HighBufferRateRuleTest {

    private static final double WARNING_THRESHOLD = 5.0;
    private static final double CRITICAL_THRESHOLD = 10.0;

    private HighBufferRateRule rule;

    @SuppressWarnings("unchecked")
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);

    @BeforeEach
    void setUp() {
        rule = new HighBufferRateRule(WARNING_THRESHOLD, CRITICAL_THRESHOLD);
    }

    // ── Below threshold ────────────────────────────────────────────────────────

    @ParameterizedTest(name = "bufferRatePct={0} → no alert")
    @ValueSource(doubles = {0.0, 1.0, 4.9, 5.0})
    void belowOrAtWarningThreshold_returnsEmpty(double bufferRate) {
        StreamMetricSnapshotDTO snapshot = snapshotWithBufferRate("stream-A", bufferRate);

        Optional<AlertEventDTO> result = rule.evaluate(snapshot, redis);

        assertThat(result)
                .as("bufferRatePct=%.1f should not trigger an alert", bufferRate)
                .isEmpty();
    }

    // ── Above warning threshold, below critical ────────────────────────────────

    @ParameterizedTest(name = "bufferRatePct={0} → WARNING")
    @ValueSource(doubles = {5.1, 7.5, 9.9, 10.0})
    void aboveWarningBelowCritical_returnsWarning(double bufferRate) {
        StreamMetricSnapshotDTO snapshot = snapshotWithBufferRate("stream-B", bufferRate);

        Optional<AlertEventDTO> result = rule.evaluate(snapshot, redis);

        assertThat(result).isPresent();
        AlertEventDTO alert = result.get();
        assertThat(alert.alertType()).isEqualTo(AlertType.HIGH_BUFFER_RATE);
        assertThat(alert.severity()).isEqualTo(AlertSeverity.WARNING);
        assertThat(alert.threshold()).isEqualTo(WARNING_THRESHOLD);
        assertThat(alert.actualValue()).isEqualTo(bufferRate);
        assertThat(alert.streamId()).isEqualTo("stream-B");
        assertThat(alert.alertId()).isNotBlank();
        assertThat(alert.message()).contains("stream-B");
    }

    // ── Above critical threshold ───────────────────────────────────────────────

    @ParameterizedTest(name = "bufferRatePct={0} → CRITICAL")
    @ValueSource(doubles = {10.1, 15.0, 50.0, 100.0})
    void aboveCriticalThreshold_returnsCritical(double bufferRate) {
        StreamMetricSnapshotDTO snapshot = snapshotWithBufferRate("stream-C", bufferRate);

        Optional<AlertEventDTO> result = rule.evaluate(snapshot, redis);

        assertThat(result).isPresent();
        AlertEventDTO alert = result.get();
        assertThat(alert.alertType()).isEqualTo(AlertType.HIGH_BUFFER_RATE);
        assertThat(alert.severity()).isEqualTo(AlertSeverity.CRITICAL);
        assertThat(alert.threshold()).isEqualTo(CRITICAL_THRESHOLD);
        assertThat(alert.actualValue()).isEqualTo(bufferRate);
    }

    // ── Threshold boundary with custom config ─────────────────────────────────

    @ParameterizedTest(name = "warning={1} critical={2} bufferRate={0}")
    @CsvSource({
            "3.0,  2.0,  6.0",   // above warning (2.0), below critical (6.0) → WARNING
            "7.0,  2.0,  6.0",   // above critical (6.0) → CRITICAL
            "1.5,  2.0,  6.0",   // below warning (2.0) → empty
    })
    void customThresholds_respectsConfig(double bufferRate, double warn, double crit) {
        HighBufferRateRule customRule = new HighBufferRateRule(warn, crit);
        StreamMetricSnapshotDTO snapshot = snapshotWithBufferRate("stream-D", bufferRate);

        Optional<AlertEventDTO> result = customRule.evaluate(snapshot, redis);

        if (bufferRate <= warn) {
            assertThat(result).isEmpty();
        } else if (bufferRate <= crit) {
            assertThat(result).isPresent();
            assertThat(result.get().severity()).isEqualTo(AlertSeverity.WARNING);
        } else {
            assertThat(result).isPresent();
            assertThat(result.get().severity()).isEqualTo(AlertSeverity.CRITICAL);
        }
    }

    // ── SPEC-11 AC1 direct: forcing bufferRatePct=12 → exactly one CRITICAL ──

    @ParameterizedTest(name = "AC1 proxy — force bufferRatePct={0}")
    @ValueSource(doubles = {12.0})
    void ac1_forceHighBuffer_emitsCriticalAlert(double bufferRate) {
        StreamMetricSnapshotDTO snapshot = snapshotWithBufferRate("stream-E", bufferRate);

        Optional<AlertEventDTO> result = rule.evaluate(snapshot, redis);

        assertThat(result).isPresent();
        assertThat(result.get().severity()).isEqualTo(AlertSeverity.CRITICAL);
        assertThat(result.get().alertType()).isEqualTo(AlertType.HIGH_BUFFER_RATE);
        assertThat(result.get().actualValue()).isEqualTo(12.0);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private StreamMetricSnapshotDTO snapshotWithBufferRate(String streamId, double bufferRate) {
        return new StreamMetricSnapshotDTO(
                streamId,
                "Test Stream",
                50_000L,    // liveViewerCount
                100L,        // viewerDelta
                bufferRate,  // bufferRatePct
                42,          // p95LatencyMs
                Map.of("1080p", 100.0),
                95.0,        // healthScore
                "CLOSED",
                0,
                System.currentTimeMillis()
        );
    }
}
