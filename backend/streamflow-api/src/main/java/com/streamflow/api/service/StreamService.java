package com.streamflow.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.api.dto.StreamSummaryDTO;
import com.streamflow.api.exception.StreamNotFoundException;
import com.streamflow.common.dto.StreamMetricSnapshotDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Service layer for stream operations in the API gateway.
 *
 * <p>SPEC-06 R3/R4: Reads from Redis:
 * <ul>
 *   <li>{@code active_streams} — Set of active stream IDs (written by the processor).</li>
 *   <li>{@code stream_snapshot:{streamId}} — JSON-encoded {@link StreamMetricSnapshotDTO}
 *       with a 30-second TTL (written by {@code SnapshotPublisher} in the processor).</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StreamService {

    static final String ACTIVE_STREAMS_KEY = "active_streams";
    static final String SNAPSHOT_KEY_PREFIX = "stream_snapshot:";

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Returns a summary for every active stream.
     *
     * <p>SPEC-06 R3: {@code SMEMBERS active_streams} to get stream IDs,
     * then GET each {@code stream_snapshot:{id}} JSON and project it.
     *
     * @return list of stream summaries; empty if no streams are active
     */
    public List<StreamSummaryDTO> listStreams() {
        Set<String> members = redisTemplate.opsForSet().members(ACTIVE_STREAMS_KEY);
        if (members == null || members.isEmpty()) {
            log.debug("No active streams in Redis");
            return List.of();
        }

        List<StreamSummaryDTO> result = new ArrayList<>(members.size());
        for (String streamId : members) {
            try {
                StreamMetricSnapshotDTO snapshot = readSnapshot(streamId);
                if (snapshot != null) {
                    result.add(toSummary(snapshot));
                }
            } catch (JsonProcessingException e) {
                log.warn("Failed to deserialise snapshot for stream={}: {}", streamId, e.getMessage());
            }
        }
        return result;
    }

    /**
     * Returns the full snapshot for a single stream.
     *
     * <p>SPEC-06 R4: Throws {@link StreamNotFoundException} (→ HTTP 404) if the
     * Redis key is absent (the snapshot TTL is 30 s, so an expired key means
     * the stream is no longer active).
     *
     * @param streamId the stream to look up
     * @return full metric snapshot
     * @throws StreamNotFoundException if no snapshot exists in Redis
     */
    public StreamMetricSnapshotDTO getStream(String streamId) {
        try {
            StreamMetricSnapshotDTO snapshot = readSnapshot(streamId);
            if (snapshot == null) {
                throw new StreamNotFoundException(streamId);
            }
            return snapshot;
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialise snapshot for stream={}: {}", streamId, e.getMessage());
            throw new StreamNotFoundException(streamId);
        }
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /**
     * Reads and deserialises the Redis snapshot for a stream.
     *
     * @return the snapshot, or {@code null} if the key is absent
     */
    private StreamMetricSnapshotDTO readSnapshot(String streamId)
            throws JsonProcessingException {
        String json = redisTemplate.opsForValue().get(SNAPSHOT_KEY_PREFIX + streamId);
        if (json == null) {
            return null;
        }
        return objectMapper.readValue(json, StreamMetricSnapshotDTO.class);
    }

    /**
     * Projects a full snapshot to the summary DTO.
     *
     * <p>{@code streamName} is stubbed to {@code streamId} until stream-metadata
     * persistence is added in a later spec (see SPEC-06 §4).
     */
    private StreamSummaryDTO toSummary(StreamMetricSnapshotDTO snapshot) {
        String name = snapshot.streamName() != null ? snapshot.streamName() : snapshot.streamId();
        return new StreamSummaryDTO(
                snapshot.streamId(),
                name,
                snapshot.liveViewerCount(),
                snapshot.healthScore(),
                snapshot.activeAlerts(),
                snapshot.circuitBreakerState()
        );
    }
}
