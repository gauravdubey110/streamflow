package com.streamflow.common.dto;

import com.streamflow.common.enums.AlertSeverity;
import com.streamflow.common.enums.AlertType;

/**
 * Kafka message payload for the {@code alerts} topic.
 *
 * <p>Matches the JSON schema in StreamFlow Project Plan §5.
 * Camel-case field names are the JSON wire names (Jackson default).
 *
 * <p>{@code threshold} and {@code actualValue} are {@code double} as specified in the design notes.
 */
public record AlertEventDTO(
        String alertId,
        String streamId,
        AlertSeverity severity,
        AlertType alertType,
        double threshold,
        double actualValue,
        String message,
        long timestamp
) {}
