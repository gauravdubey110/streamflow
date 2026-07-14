package com.streamflow.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.streamflow.common.enums.EventType;
import com.streamflow.common.enums.VideoQuality;

/**
 * Kafka message payload for the {@code viewer-events} topic.
 *
 * <p>Matches the JSON schema in StreamFlow Project Plan §5. Camel-case field names are the JSON
 * wire names (Jackson default).
 *
 * <p>{@code bufferDurationMs} and {@code region} are optional — they are omitted from the
 * serialized JSON when {@code null} (NFR: {@code @JsonInclude(NON_NULL)}).
 */
public record ViewerEventDTO(
    String eventId,
    String streamId,
    String viewerId,
    EventType eventType,
    VideoQuality quality,
    @JsonInclude(JsonInclude.Include.NON_NULL) Long bufferDurationMs,
    long timestamp,
    @JsonInclude(JsonInclude.Include.NON_NULL) String region) {}
