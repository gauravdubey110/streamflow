package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/**
 * Alert rule that fires when {@code bufferRatePct} exceeds a configurable threshold.
 *
 * <p>SPEC-11 R2:
 * <ul>
 *   <li>WARNING when {@code bufferRatePct > streamflow.alerts.high-buffer.warning-threshold}
 *       (default 5.0%).</li>
 *   <li>CRITICAL when {@code bufferRatePct > streamflow.alerts.high-buffer.critical-threshold}
 *       (default 10.0%).</li>
 * </ul>
 *
 * <p>This rule is stateless — it reads only the snapshot's {@code bufferRatePct} field.
 * Deduplication is handled by {@link AlertEngine}.
 */
@Slf4j
@Component
public class HighBufferRateRule implements AlertRule {

    private final double warningThreshold;
    private final double criticalThreshold;

    public HighBufferRateRule(
            @Value("${streamflow.alerts.high-buffer.warning-threshold:5.0}") double warningThreshold,
            @Value("${streamflow.alerts.high-buffer.critical-threshold:10.0}") double criticalThreshold) {
        this.warningThreshold = warningThreshold;
        this.criticalThreshold = criticalThreshold;
    }

    @Override
    public Optional<AlertEventDTO> evaluate(StreamMetricSnapshotDTO snapshot,
                                            RedisTemplate<String, String> redisTemplate) {
        double bufferRate = snapshot.bufferRatePct();

        if (bufferRate <= warningThreshold) {
            return Optional.empty();
        }

        AlertSeverity severity = bufferRate > criticalThreshold
                ? AlertSeverity.CRITICAL
                : AlertSeverity.WARNING;

        double threshold = severity == AlertSeverity.CRITICAL ? criticalThreshold : warningThreshold;

        String message = String.format(
                "Buffer rate %.1f%% exceeds threshold %.1f%% on %s",
                bufferRate, threshold, snapshot.streamId());

        AlertEventDTO alert = new AlertEventDTO(
                UUID.randomUUID().toString(),
                snapshot.streamId(),
                severity,
                AlertType.HIGH_BUFFER_RATE,
                threshold,
                bufferRate,
                message,
                System.currentTimeMillis()
        );

        log.debug("HighBufferRateRule triggered: stream={} bufferRate={} severity={}",
                snapshot.streamId(), bufferRate, severity);
        return Optional.of(alert);
    }

    // ── package-visible for tests ─────────────────────────────────────────────

    double getWarningThreshold() {
        return warningThreshold;
    }

    double getCriticalThreshold() {
        return criticalThreshold;
    }
}
