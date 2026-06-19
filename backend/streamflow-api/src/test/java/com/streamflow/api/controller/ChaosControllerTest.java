package com.streamflow.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.api.chaos.ChaosRequest;
import com.streamflow.api.chaos.ChaosResponse;
import com.streamflow.api.chaos.ChaosScenarioDTO;
import com.streamflow.api.chaos.ProducerChaosClient;
import com.streamflow.api.exception.GlobalExceptionHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * MockMvc slice test for {@link ChaosController}.
 *
 * <p>SPEC-13 Test Plan: verifies HTTP 202 on start, 204/404 on cancel, and 400 on invalid input.
 * {@link ProducerChaosClient} is mocked — no actual HTTP call is made to the producer.
 */
@WebMvcTest(ChaosController.class)
@Import(GlobalExceptionHandler.class)
class ChaosControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ProducerChaosClient producerChaosClient;

    @Test
    void postChaos_validRequest_returns202WithChaosId() throws Exception {
        ChaosResponse mockResponse = new ChaosResponse("chaos-uuid-abc", System.currentTimeMillis());
        when(producerChaosClient.startChaos(eq("stream-001"), eq(ChaosScenarioDTO.HIGH_BUFFER), eq(30)))
                .thenReturn(mockResponse);

        ChaosRequest request = new ChaosRequest(ChaosScenarioDTO.HIGH_BUFFER, 30);

        mockMvc.perform(post("/api/v1/streams/stream-001/chaos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.chaosId").value("chaos-uuid-abc"))
                .andExpect(jsonPath("$.startsAt").isNumber());
    }

    @Test
    void postChaos_nullScenario_returns400() throws Exception {
        // Omitting scenario field results in null which fails @NotNull
        String body = "{\"durationSeconds\": 30}";

        mockMvc.perform(post("/api/v1/streams/stream-001/chaos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postChaos_durationTooLarge_returns400() throws Exception {
        ChaosRequest request = new ChaosRequest(ChaosScenarioDTO.VIEWER_DROP, 500);

        mockMvc.perform(post("/api/v1/streams/stream-001/chaos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postChaos_invalidScenarioString_returns400() throws Exception {
        String body = "{\"scenario\": \"INVALID_SCENARIO\", \"durationSeconds\": 30}";

        mockMvc.perform(post("/api/v1/streams/stream-001/chaos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void deleteChaos_existingId_returns204() throws Exception {
        when(producerChaosClient.cancelChaos("chaos-123")).thenReturn(true);

        mockMvc.perform(delete("/api/v1/streams/stream-001/chaos/chaos-123"))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteChaos_unknownId_returns404() throws Exception {
        when(producerChaosClient.cancelChaos("unknown")).thenReturn(false);

        mockMvc.perform(delete("/api/v1/streams/stream-001/chaos/unknown"))
                .andExpect(status().isNotFound());
    }
}
