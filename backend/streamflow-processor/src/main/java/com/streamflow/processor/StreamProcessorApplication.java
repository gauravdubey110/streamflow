package com.streamflow.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the StreamFlow Processor service.
 *
 * <p>Responsibilities (implemented in later specs):
 *
 * <ul>
 *   <li>SPEC-04: Consume {@code viewer-events}, maintain Redis sliding-window viewer count
 *   <li>SPEC-05: Publish metric snapshots to Redis + {@code metrics-aggregated} topic
 *   <li>SPEC-09: Consume {@code stream-health}, calculate health score
 *   <li>SPEC-10: Quality distribution aggregation (Redis Hash)
 *   <li>SPEC-11: Alert engine — threshold evaluation + publish to {@code alerts} topic
 *   <li>SPEC-12: Resilience4j circuit breaker on alert engine (Redis-backed state)
 *   <li>SPEC-17: Cassandra write repositories for events, snapshots, alerts
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class StreamProcessorApplication {

  public static void main(String[] args) {
    SpringApplication.run(StreamProcessorApplication.class, args);
  }
}
