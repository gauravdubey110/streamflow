package com.streamflow.api.chaos;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Public chaos request body received from REST clients.
 *
 * <p>SPEC-13 R4: posted to {@code POST /api/v1/streams/{streamId}/chaos}.
 * The {@code scenario} field is validated as a non-null {@link ChaosScenarioDTO} enum value.
 *
 * @param scenario        chaos scenario to inject (VIEWER_DROP, BITRATE_SPIKE, HIGH_BUFFER, STREAM_DOWN)
 * @param durationSeconds how long chaos should last; must be 1–300
 */
public record ChaosRequest(
        @NotNull(message = "scenario must not be null")
        ChaosScenarioDTO scenario,

        @Min(value = 1, message = "durationSeconds must be at least 1")
        @Max(value = 300, message = "durationSeconds must not exceed 300")
        int durationSeconds
) {}
