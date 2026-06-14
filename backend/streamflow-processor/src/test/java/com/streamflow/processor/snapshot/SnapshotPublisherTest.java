package com.streamflow.processor.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.common.constants.KafkaTopics;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import com.streamflow.processor.aggregator.HealthScoreCalculator;
import com.streamflow.processor.aggregator.ViewerCountAggregator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link SnapshotPublisher}.
 *
 * <p>SPEC-05 Test Plan — Unit: verify that for each active stream the publisher
 * writes a JSON snapshot to Redis (SET with TTL) and sends a Kafka message to
 * {@code metrics-aggregated}.
 *
 * <p>All external dependencies (Redis, Kafka, aggregator) are mocked so the test
 * runs without any infrastructure.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SnapshotPublisherTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    @SuppressWarnings("rawtypes")
    private SetOperations setOperations;

    @Mock
    @SuppressWarnings("rawtypes")
    private ValueOperations valueOperations;

    @Mock
    @SuppressWarnings("rawtypes")
    private HashOperations hashOperations;

    @Mock
    private KafkaTemplate<String, StreamMetricSnapshotDTO> snapshotKafkaTemplate;

    @Mock
    private ViewerCountAggregator viewerCountAggregator;

    private SnapshotPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        // By default: empty health hash → fallback path (SPEC-09 AC4)
        when(hashOperations.entries(anyString())).thenReturn(Map.of());

        publisher = new SnapshotPublisher(
                redisTemplate,
                snapshotKafkaTemplate,
                viewerCountAggregator,
                new HealthScoreCalculator(),
                new ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    // ── AC1-proxy: Redis SET called per stream ────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_writesRedisSnapshotForEachActiveStream() {
        String streamId = "stream-001";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(500L);

        publisher.publishSnapshots();

        // Verify SET was called with the correct key and a 30-second TTL
        verify(valueOperations).set(
                eq("stream_snapshot:" + streamId),
                anyString(),
                eq(Duration.ofSeconds(30))
        );
    }

    // ── AC2-proxy: Kafka send called per stream ───────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_sendsKafkaMessageForEachActiveStream() {
        String streamId = "stream-002";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(300L);

        publisher.publishSnapshots();

        verify(snapshotKafkaTemplate).send(
                eq(KafkaTopics.METRICS_AGGREGATED),
                eq(streamId),
                any(StreamMetricSnapshotDTO.class)
        );
    }

    // ── AC4: viewerDelta = count_t - count_{t-1} ─────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_computesViewerDeltaCorrectly() {
        String streamId = "stream-003";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));

        // First call: 1000 viewers → delta = 0 (no previous)
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(1000L);
        publisher.publishSnapshots();

        // Second call: 1200 viewers → delta should be +200
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(1200L);
        publisher.publishSnapshots();

        ArgumentCaptor<StreamMetricSnapshotDTO> captor =
                ArgumentCaptor.forClass(StreamMetricSnapshotDTO.class);
        // send is called twice total; grab the second invocation
        verify(snapshotKafkaTemplate, times(2))
                .send(eq(KafkaTopics.METRICS_AGGREGATED), eq(streamId), captor.capture());

        StreamMetricSnapshotDTO secondSnapshot = captor.getAllValues().get(1);
        assertThat(secondSnapshot.viewerDelta())
                .as("viewerDelta should be 200 (1200 - 1000)")
                .isEqualTo(200L);
        assertThat(secondSnapshot.liveViewerCount())
                .as("liveViewerCount should be 1200")
                .isEqualTo(1200L);
    }

    // ── Snapshot JSON contains correct fields ─────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_snapshotJsonContainsStreamIdAndCount() throws Exception {
        String streamId = "stream-004";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(750L);

        publisher.publishSnapshots();

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(
                eq("stream_snapshot:" + streamId),
                jsonCaptor.capture(),
                eq(Duration.ofSeconds(30))
        );

        String json = jsonCaptor.getValue();
        assertThat(json).contains("\"streamId\":\"" + streamId + "\"");
        assertThat(json).contains("\"liveViewerCount\":750");
        assertThat(json).contains("\"circuitBreakerState\":\"CLOSED\"");
    }

    // ── No active streams → no writes ────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_noActiveStreams_noRedisOrKafkaWrites() {
        when(setOperations.members("active_streams")).thenReturn(Set.of());

        publisher.publishSnapshots();

        verify(valueOperations, never()).set(anyString(), anyString(), any(Duration.class));
        verify(snapshotKafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    // ── Null result from SMEMBERS → no writes ────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_nullActiveStreams_noRedisOrKafkaWrites() {
        when(setOperations.members("active_streams")).thenReturn(null);

        publisher.publishSnapshots();

        verify(valueOperations, never()).set(anyString(), anyString(), any(Duration.class));
        verify(snapshotKafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    // ── SPEC-09: health hash absent → fallback score 50.0 ────────────────────

    /**
     * When the {@code stream_health:{streamId}} hash is absent (empty map),
     * the snapshot must contain healthScore = 50.0 (SPEC-09 AC4 / Q1 resolution).
     */
    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_absentHealthHash_usesDefaultHealthScore() {
        String streamId = "stream-no-health";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(100L);
        // hashOperations.entries returns empty map (already configured in setUp)

        publisher.publishSnapshots();

        ArgumentCaptor<StreamMetricSnapshotDTO> captor =
                ArgumentCaptor.forClass(StreamMetricSnapshotDTO.class);
        verify(snapshotKafkaTemplate).send(
                eq(KafkaTopics.METRICS_AGGREGATED), eq(streamId), captor.capture());

        StreamMetricSnapshotDTO snapshot = captor.getValue();
        assertThat(snapshot.healthScore())
                .as("Missing health hash should fall back to 50.0 (SPEC-09 AC4)")
                .isEqualTo(50.0);
    }

    // ── SPEC-09: health hash present → computed score ────────────────────────

    /**
     * When the {@code stream_health:{streamId}} hash is present with normal values,
     * the snapshot's healthScore must be computed (not 50.0 fallback).
     * Normal: bitrate=4500, frameDrop=0.02, latency=120 → score = 80.0.
     */
    @Test
    @SuppressWarnings("unchecked")
    void publishSnapshots_presentHealthHash_usesComputedScore() {
        String streamId = "stream-with-health";
        when(setOperations.members("active_streams")).thenReturn(Set.of(streamId));
        when(viewerCountAggregator.getLiveCount(streamId)).thenReturn(200L);

        // Provide a health hash: bitrate=4500, frameDrop=0.02, latency=120 ms
        // Penalty: 0 (buffer) + 20 (frameDrop) + 0 (latency) + 0 (bitrate) = 20 → score = 80.0
        Map<Object, Object> healthHash = Map.of(
                "bitrateKbps",       "4500",
                "frameDropRate",     "0.02",
                "encoderLatencyMs",  "120",
                "cdnEdgeNode",       "edge-test",
                "timestamp",         String.valueOf(System.currentTimeMillis())
        );
        when(hashOperations.entries("stream_health:" + streamId)).thenReturn(healthHash);

        publisher.publishSnapshots();

        ArgumentCaptor<StreamMetricSnapshotDTO> captor =
                ArgumentCaptor.forClass(StreamMetricSnapshotDTO.class);
        verify(snapshotKafkaTemplate).send(
                eq(KafkaTopics.METRICS_AGGREGATED), eq(streamId), captor.capture());

        StreamMetricSnapshotDTO snapshot = captor.getValue();
        assertThat(snapshot.healthScore())
                .as("healthScore with normal health hash should be 80.0")
                .isEqualTo(80.0);
        assertThat(snapshot.p95LatencyMs())
                .as("p95LatencyMs should be encoderLatencyMs (120)")
                .isEqualTo(120);
    }
}
