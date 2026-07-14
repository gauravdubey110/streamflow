package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;
import com.streamflow.processor.consumer.StreamHealthConsumer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Alert rule that fires when the stream bitrate drops below a configurable threshold.
 *
 * <p>SPEC-11 R2 — BitrateDegradationRule:
 *
 * <ul>
 *   <li>Reads the {@code stream_health:{streamId}} Redis Hash written by {@link
 *       StreamHealthConsumer} to get the latest {@code bitrateKbps}.
 *   <li>Fires a WARNING alert when {@code bitrateKbps < streamflow.alerts.bitrate.min-kbps}
 *       (default 2500).
 *   <li>If the health hash is absent (expired or never written), the rule clears silently — no
 *       alert is raised for missing data.
 * </ul>
 *
 * <p>This rule is stateless: all data lives in the Redis health hash maintained by {@link
 * StreamHealthConsumer}.
 */
@Slf4j
@Component
public class BitrateDegradationRule implements AlertRule {

  private final int minBitrateKbps;

  public BitrateDegradationRule(
      @Value("${streamflow.alerts.bitrate.min-kbps:2500}") int minBitrateKbps) {
    this.minBitrateKbps = minBitrateKbps;
  }

  @Override
  public Optional<AlertEventDTO> evaluate(
      StreamMetricSnapshotDTO snapshot, RedisTemplate<String, String> redisTemplate) {
    String streamId = snapshot.streamId();
    String key = StreamHealthConsumer.HEALTH_KEY_PREFIX + streamId;

    Map<Object, Object> raw = redisTemplate.opsForHash().entries(key);
    if (raw == null || raw.isEmpty()) {
      log.trace("BitrateDegradationRule: health hash absent for stream={} — skipping", streamId);
      return Optional.empty();
    }

    Object bitrateRaw = raw.get(StreamHealthConsumer.FIELD_BITRATE);
    if (bitrateRaw == null) {
      return Optional.empty();
    }

    int bitrateKbps;
    try {
      bitrateKbps = (int) Double.parseDouble(bitrateRaw.toString());
    } catch (NumberFormatException e) {
      log.warn(
          "BitrateDegradationRule: cannot parse bitrateKbps='{}' for stream={}",
          bitrateRaw,
          streamId);
      return Optional.empty();
    }

    if (bitrateKbps >= minBitrateKbps) {
      return Optional.empty();
    }

    String message =
        String.format(
            "Bitrate %d kbps below minimum threshold %d kbps on %s",
            bitrateKbps, minBitrateKbps, streamId);

    AlertEventDTO alert =
        new AlertEventDTO(
            UUID.randomUUID().toString(),
            streamId,
            AlertSeverity.WARNING,
            AlertType.BITRATE_DEGRADATION,
            (double) minBitrateKbps,
            (double) bitrateKbps,
            message,
            System.currentTimeMillis());

    log.debug(
        "BitrateDegradationRule triggered: stream={} bitrateKbps={} threshold={}",
        streamId,
        bitrateKbps,
        minBitrateKbps);
    return Optional.of(alert);
  }

  // ── package-visible for tests ─────────────────────────────────────────────

  int getMinBitrateKbps() {
    return minBitrateKbps;
  }
}
