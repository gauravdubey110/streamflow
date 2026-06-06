package com.streamflow.common.enums;

/**
 * Categories of alerts published to the {@code alerts} Kafka topic.
 * Enum names are the JSON wire values.
 */
public enum AlertType {
    VIEWER_DROP,
    HIGH_BUFFER_RATE,
    BITRATE_DEGRADATION,
    STREAM_DOWN
}
