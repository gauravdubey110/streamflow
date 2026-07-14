package com.streamflow.processor.alert;

import com.streamflow.common.dto.AlertEventDTO;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import java.util.Optional;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Contract for a single alert evaluation rule.
 *
 * <p>SPEC-11 R1: each rule receives the current snapshot and a Redis template for any stateful
 * look-ups (e.g. bitrate from the health hash or rolling deltas). Rules are stateless beans — all
 * state lives in Redis.
 *
 * <p>Implementations must be thread-safe; the {@link AlertEngine} may call {@code evaluate} from a
 * scheduled thread that is shared across multiple rules.
 *
 * @see HighBufferRateRule
 * @see ViewerDropRule
 * @see BitrateDegradationRule
 */
public interface AlertRule {

  /**
   * Evaluates this rule for the given stream snapshot.
   *
   * @param snapshot the current metric snapshot for the stream
   * @param redisTemplate shared Redis template for stateful look-ups
   * @return a non-empty {@link Optional} containing the alert to publish when the rule's threshold
   *     is crossed; {@link Optional#empty()} when the rule clears
   */
  Optional<AlertEventDTO> evaluate(
      StreamMetricSnapshotDTO snapshot, RedisTemplate<String, String> redisTemplate);
}
