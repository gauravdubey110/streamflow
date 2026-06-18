package com.streamflow.api.chaos;

/**
 * Chaos start response body returned to REST clients.
 *
 * <p>SPEC-13 R4: returned by {@code POST /api/v1/streams/{streamId}/chaos} as HTTP 202 Accepted.
 *
 * @param chaosId  unique identifier for the chaos session (UUID)
 * @param startsAt epoch-millis when the chaos session was initiated
 */
public record ChaosResponse(
        String chaosId,
        long startsAt
) {}
