package com.streamflow.producer.strategy;

import com.streamflow.common.dto.ViewerEventDTO;

/**
 * Strategy interface for generating simulated {@link ViewerEventDTO} messages.
 *
 * <p>SPEC-03 Design Notes: extensibility hook for later specs (e.g. SPEC-13 chaos injection will
 * add {@code SpikeLoadStrategy}). {@code NormalLoadStrategy} is the only implementation for this
 * spec.
 */
public interface EventGenerationStrategy {

  /**
   * Generate a single {@link ViewerEventDTO} for the given stream.
   *
   * @param streamId the Kafka message key (e.g. {@code "stream-001"})
   * @param streamName human-readable stream name used in log context
   * @return a fully populated DTO ready for Kafka serialisation
   */
  ViewerEventDTO generate(String streamId, String streamName);
}
