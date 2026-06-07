package com.streamflow.producer.simulator;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.producer.strategy.EventGenerationStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-stream event producer.
 *
 * <p>SPEC-03 R3: Uses a single-threaded {@link ScheduledExecutorService} to emit
 * {@link ViewerEventDTO} events at a target rate of {@code tps / numStreams}
 * events per second, keyed by {@code streamId}.
 *
 * <p>Lifecycle: created by {@link StreamSimulator}; call {@link #start()} once,
 * {@link #stop()} on shutdown.
 */
@Slf4j
public class ViewerEventProducer {

    private static final long HEALTH_WINDOW_NS = 10L * 1_000_000_000L; // 10 seconds

    private final String streamId;
    private final String streamName;
    private final int perStreamTps;
    private final KafkaTemplate<String, ViewerEventDTO> kafkaTemplate;
    private final EventGenerationStrategy strategy;

    /** Epoch-nanotime of the last successfully sent event; 0 = never sent. */
    private final AtomicLong lastPublishNs = new AtomicLong(0);

    private final ScheduledExecutorService scheduler;

    public ViewerEventProducer(String streamId,
                               String streamName,
                               int perStreamTps,
                               KafkaTemplate<String, ViewerEventDTO> kafkaTemplate,
                               EventGenerationStrategy strategy) {
        this.streamId = streamId;
        this.streamName = streamName;
        this.perStreamTps = perStreamTps;
        this.kafkaTemplate = kafkaTemplate;
        this.strategy = strategy;
        // Named thread so logs are easy to correlate; must be created after streamId is set
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "vep-" + streamId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start emitting at {@code perStreamTps} events per second.
     *
     * <p>Period is computed in microseconds to allow sub-millisecond precision.
     * At 334 TPS per stream (1000 / 3), period ≈ 2994 µs.
     */
    public void start() {
        long periodMicros = 1_000_000L / perStreamTps;
        log.info("Starting ViewerEventProducer for stream={} at {}tps (period={}µs)",
                streamId, perStreamTps, periodMicros);

        scheduler.scheduleAtFixedRate(
                this::emitEvent,
                0,
                periodMicros,
                TimeUnit.MICROSECONDS
        );
    }

    /**
     * Graceful shutdown: stops accepting new work and flushes the Kafka producer.
     *
     * <p>SPEC-03 R6: completes within 5 seconds.
     */
    public void stop() {
        log.info("Stopping ViewerEventProducer for stream={}", streamId);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                log.warn("ViewerEventProducer for stream={} did not stop cleanly within 5s", streamId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }

    /**
     * Returns {@code true} if at least one event was published within the last 10 seconds.
     *
     * <p>SPEC-03 R7: used by {@link com.streamflow.producer.health.KafkaProducerHealthIndicator}.
     */
    public boolean isHealthy() {
        long last = lastPublishNs.get();
        return last != 0 && (System.nanoTime() - last) < HEALTH_WINDOW_NS;
    }

    public String getStreamId() {
        return streamId;
    }

    private void emitEvent() {
        try {
            ViewerEventDTO event = strategy.generate(streamId, streamName);
            kafkaTemplate.send(KafkaTopics.VIEWER_EVENTS, streamId, event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.warn("Failed to send event for stream={}: {}", streamId, ex.getMessage());
                        } else {
                            lastPublishNs.set(System.nanoTime());
                        }
                    });
        } catch (Exception e) {
            log.error("Unexpected error in ViewerEventProducer for stream={}", streamId, e);
        }
    }
}
