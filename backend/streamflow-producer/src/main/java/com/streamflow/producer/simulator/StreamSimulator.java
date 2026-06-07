package com.streamflow.producer.simulator;

import com.streamflow.common.dto.ViewerEventDTO;
import com.streamflow.producer.config.SimulationConfig;
import com.streamflow.producer.config.SimulationConfig.StreamDefinition;
import com.streamflow.producer.strategy.EventGenerationStrategy;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Orchestrator for all per-stream {@link ViewerEventProducer}s.
 *
 * <p>SPEC-03 R3: starts one producer per configured stream on application startup.
 * The aggregate TPS is distributed evenly across streams
 * ({@code perStreamTps = totalTps / numStreams}).
 *
 * <p>SPEC-03 R4: {@code enabled=false} starts the app but emits zero messages (AC4).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamSimulator {

    private final SimulationConfig simulationConfig;
    private final KafkaTemplate<String, ViewerEventDTO> viewerEventKafkaTemplate;
    private final EventGenerationStrategy eventGenerationStrategy;

    private final List<ViewerEventProducer> producers = new ArrayList<>();

    /**
     * Start all per-stream producers on application startup.
     * No-ops if {@code streamflow.simulation.enabled=false}.
     */
    @PostConstruct
    public void start() {
        if (!simulationConfig.isEnabled()) {
            log.info("Simulation disabled (streamflow.simulation.enabled=false). No events will be produced.");
            return;
        }

        List<StreamDefinition> streams = simulationConfig.getStreams();
        if (streams.isEmpty()) {
            log.warn("No streams configured under streamflow.simulation.streams[]. No events will be produced.");
            return;
        }

        int perStreamTps = Math.max(1, simulationConfig.getTps() / streams.size());
        log.info("Starting StreamSimulator: {} streams, {}tps total, {}tps per stream",
                streams.size(), simulationConfig.getTps(), perStreamTps);

        for (StreamDefinition def : streams) {
            ViewerEventProducer producer = new ViewerEventProducer(
                    def.getId(),
                    def.getName(),
                    perStreamTps,
                    viewerEventKafkaTemplate,
                    eventGenerationStrategy
            );
            producer.start();
            producers.add(producer);
        }
    }

    /**
     * Graceful shutdown: stop all producers, flush Kafka.
     *
     * <p>SPEC-03 R6: each {@link ViewerEventProducer#stop()} waits up to 5s;
     * called sequentially so the overall budget is {@code 5s * numStreams}.
     */
    @PreDestroy
    public void stop() {
        log.info("Shutting down StreamSimulator ({} producers)", producers.size());
        for (ViewerEventProducer producer : producers) {
            producer.stop();
        }
        viewerEventKafkaTemplate.getProducerFactory().closeThreadBoundProducer();
        log.info("StreamSimulator stopped.");
    }

    /**
     * Returns an unmodifiable view of all active producers.
     * Used by the health indicator.
     */
    public List<ViewerEventProducer> getProducers() {
        return Collections.unmodifiableList(producers);
    }
}
