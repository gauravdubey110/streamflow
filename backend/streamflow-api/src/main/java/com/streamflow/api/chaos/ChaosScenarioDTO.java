package com.streamflow.api.chaos;

/**
 * Chaos scenario enum as seen by the public API.
 *
 * <p>SPEC-13 R2/R6: mirrors the producer-internal {@code ChaosScenario} enum.
 * Kept separate so the API module does not depend on internal producer classes.
 */
public enum ChaosScenarioDTO {
    VIEWER_DROP,
    BITRATE_SPIKE,
    HIGH_BUFFER,
    STREAM_DOWN
}
