package com.streamflow.producer.chaos;

/**
 * Enumeration of chaos scenarios that can be injected into a simulated stream.
 *
 * <p>SPEC-13 R2: each value describes a distinct degradation behaviour applied by {@link
 * ChaosInjector} and surfaced through {@link com.streamflow.producer.strategy.ChaosAwareStrategy}.
 *
 * <ul>
 *   <li>{@link #VIEWER_DROP} — doubles the ratio of DROP events in the viewer-event stream.
 *   <li>{@link #BITRATE_SPIKE} — pushes {@code bitrateKbps} down to 1 500 and {@code frameDropRate}
 *       up to 0.15 in health events (simulates bitrate degradation).
 *   <li>{@link #HIGH_BUFFER} — emits {@code BUFFER_START} at 12 % rate (vs. ~5 % baseline).
 *   <li>{@link #STREAM_DOWN} — pauses all event emission for the stream during the chaos window.
 * </ul>
 */
public enum ChaosScenario {
  VIEWER_DROP,
  BITRATE_SPIKE,
  HIGH_BUFFER,
  STREAM_DOWN
}
