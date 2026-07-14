package com.streamflow.producer.health;

import com.streamflow.producer.simulator.StreamSimulator;
import com.streamflow.producer.simulator.ViewerEventProducer;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Custom Actuator health indicator for the Kafka producer.
 *
 * <p>SPEC-03 R7: reports {@code UP} only when at least one event has been published in the last 10
 * seconds across any configured stream.
 *
 * <p>When simulation is disabled ({@link StreamSimulator#getProducers()} is empty), the indicator
 * returns {@code UP} with a note — this is intentional; a disabled simulation is not an error
 * state.
 */
@Component
@RequiredArgsConstructor
public class KafkaProducerHealthIndicator implements HealthIndicator {

  private final StreamSimulator streamSimulator;

  @Override
  public Health health() {
    List<ViewerEventProducer> producers = streamSimulator.getProducers();

    if (producers.isEmpty()) {
      return Health.up().withDetail("note", "Simulation disabled or no streams configured").build();
    }

    boolean anyHealthy = producers.stream().anyMatch(ViewerEventProducer::isHealthy);

    if (anyHealthy) {
      long healthyCount = producers.stream().filter(ViewerEventProducer::isHealthy).count();
      return Health.up()
          .withDetail("healthyStreams", healthyCount)
          .withDetail("totalStreams", producers.size())
          .build();
    } else {
      return Health.down()
          .withDetail("reason", "No events published in the last 10 seconds on any stream")
          .withDetail("totalStreams", producers.size())
          .build();
    }
  }
}
