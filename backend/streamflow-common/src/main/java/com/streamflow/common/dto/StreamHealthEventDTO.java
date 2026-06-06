package com.streamflow.common.dto;

/**
 * Kafka message payload for the {@code stream-health} topic.
 *
 * <p>Matches the JSON schema in StreamFlow Project Plan §5.
 * Camel-case field names are the JSON wire names (Jackson default).
 */
public record StreamHealthEventDTO(
        String eventId,
        String streamId,
        int bitrateKbps,
        double frameDropRate,
        int encoderLatencyMs,
        String cdnEdgeNode,
        long timestamp
) {}
