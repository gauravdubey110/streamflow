package com.streamflow.producer.simulator;

import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamHealthEventDTO;
import com.streamflow.producer.chaos.ChaosInjector;
import com.streamflow.producer.chaos.ChaosScenario;
import com.streamflow.producer.config.SimulationConfig;
import com.streamflow.producer.config.SimulationConfig.StreamDefinition;
import com.streamflow.producer.metrics.ProducerMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * Emits one {@link StreamHealthEventDTO} per configured stream every 2 seconds.
 *
 * <p>SPEC-09 R1 — values are randomised within ±10 % of these means:
 * <ul>
 *   <li>bitrate:         4 500 kbps</li>
 *   <li>frame-drop rate: 0.02</li>
 *   <li>encoder latency: 100–150 ms (midpoint 125 ms)</li>
 * </ul>
 *
 * <p>CDN edge nodes cycle through a small fixed set for realism.
 * Production would source these from a service-discovery registry; the
 * fixed list is sufficient for simulation (SPEC-09 scope).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamHealthProducer {

    private static final int BASE_BITRATE_KBPS      = 4_500;
    private static final double BASE_FRAME_DROP_RATE = 0.02;
    private static final int BASE_LATENCY_MS         = 125;  // midpoint of 100–150 ms
    private static final double JITTER_FRACTION      = 0.10; // ±10 %

    private static final String[] CDN_EDGE_NODES = {
            "edge-mumbai-01",
            "edge-delhi-01",
            "edge-california-01",
            "edge-london-01",
            "edge-singapore-01"
    };

    /** SPEC-13 BITRATE_SPIKE: degraded bitrate target in kbps. */
    static final int CHAOS_BITRATE_KBPS = 1_500;

    /** SPEC-13 BITRATE_SPIKE: degraded frame-drop rate. */
    static final double CHAOS_FRAME_DROP_RATE = 0.15;

    private final SimulationConfig simulationConfig;
    private final KafkaTemplate<String, StreamHealthEventDTO> healthEventKafkaTemplate;
    private final ChaosInjector chaosInjector;
    private final ProducerMetrics producerMetrics;
    private final Random random = new Random();

    /**
     * Fires every 2 000 ms (SPEC-09 R1). Skips emission when simulation is disabled.
     *
     * <p>Each invocation emits one event per configured stream, keyed by {@code streamId},
     * so all events for a stream land in the same Kafka partition.
     */
    @Scheduled(fixedRate = 2_000)
    public void emitHealthEvents() {
        if (!simulationConfig.isEnabled()) {
            log.trace("Simulation disabled — skipping stream-health emission");
            return;
        }

        List<StreamDefinition> streams = simulationConfig.getStreams();
        if (streams.isEmpty()) {
            log.trace("No streams configured — skipping stream-health emission");
            return;
        }

        for (StreamDefinition stream : streams) {
            try {
                // SPEC-13: STREAM_DOWN suppresses health events; BITRATE_SPIKE modulates values
                Optional<ChaosScenario> active = chaosInjector.activeScenario(stream.getId());
                if (active.isPresent() && active.get() == ChaosScenario.STREAM_DOWN) {
                    log.trace("Skipping health event for stream={} (STREAM_DOWN chaos active)", stream.getId());
                    continue;
                }
                StreamHealthEventDTO event = active.isPresent() && active.get() == ChaosScenario.BITRATE_SPIKE
                        ? buildDegradedEvent(stream.getId())
                        : buildEvent(stream.getId());
                healthEventKafkaTemplate.send(KafkaTopics.STREAM_HEALTH, stream.getId(), event)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                log.warn("Failed to send health event for stream={}: {}",
                                        stream.getId(), ex.getMessage());
                            } else {
                                log.trace("Health event sent: stream={} bitrate={} frameDrop={} latency={}",
                                        stream.getId(), event.bitrateKbps(),
                                        event.frameDropRate(), event.encoderLatencyMs());
                                // SPEC-20 R3: increment health events published counter
                                producerMetrics.incrementHealthEventsPublished();
                            }
                        });
            } catch (Exception e) {
                log.error("Unexpected error emitting health event for stream={}", stream.getId(), e);
            }
        }
    }

    // ── internal ──────────────────────────────────────────────────────────────

    /**
     * Constructs a degraded health event for the BITRATE_SPIKE chaos scenario.
     *
     * <p>SPEC-13 R2: bitrateKbps = 1 500, frameDropRate = 0.15, encoderLatencyMs gets normal jitter.
     *
     * @param streamId the stream this event belongs to
     * @return a degraded {@link StreamHealthEventDTO}
     */
    StreamHealthEventDTO buildDegradedEvent(String streamId) {
        String cdnEdgeNode = CDN_EDGE_NODES[random.nextInt(CDN_EDGE_NODES.length)];
        return new StreamHealthEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                CHAOS_BITRATE_KBPS,
                CHAOS_FRAME_DROP_RATE,
                applyJitter(BASE_LATENCY_MS),
                cdnEdgeNode,
                System.currentTimeMillis()
        );
    }

    /**
     * Constructs a health event with ±10 % randomisation around the base values.
     *
     * @param streamId the stream this event belongs to
     * @return a freshly generated {@link StreamHealthEventDTO}
     */
    StreamHealthEventDTO buildEvent(String streamId) {
        int bitrateKbps      = applyJitter(BASE_BITRATE_KBPS);
        double frameDropRate = applyJitter(BASE_FRAME_DROP_RATE);
        int encoderLatencyMs = applyJitter(BASE_LATENCY_MS);
        String cdnEdgeNode   = CDN_EDGE_NODES[random.nextInt(CDN_EDGE_NODES.length)];

        return new StreamHealthEventDTO(
                UUID.randomUUID().toString(),
                streamId,
                bitrateKbps,
                frameDropRate,
                encoderLatencyMs,
                cdnEdgeNode,
                System.currentTimeMillis()
        );
    }

    /**
     * Returns a random value uniformly distributed within ±{@link #JITTER_FRACTION}
     * of {@code baseValue}.
     *
     * @param baseValue the nominal value
     * @return {@code baseValue * (1 ± JITTER_FRACTION)}
     */
    private int applyJitter(int baseValue) {
        double factor = 1.0 + (random.nextDouble() * 2 * JITTER_FRACTION) - JITTER_FRACTION;
        return (int) Math.round(baseValue * factor);
    }

    /**
     * Returns a random double uniformly distributed within ±{@link #JITTER_FRACTION}
     * of {@code baseValue}.
     *
     * @param baseValue the nominal value
     * @return {@code baseValue * (1 ± JITTER_FRACTION)}
     */
    private double applyJitter(double baseValue) {
        double factor = 1.0 + (random.nextDouble() * 2 * JITTER_FRACTION) - JITTER_FRACTION;
        double result = baseValue * factor;
        // Clamp frame-drop rate to [0, 1]
        return Math.max(0.0, Math.min(1.0, result));
    }
}
