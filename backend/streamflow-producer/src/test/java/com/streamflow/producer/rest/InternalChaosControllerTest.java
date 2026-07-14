package com.streamflow.producer.rest;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.producer.chaos.ChaosInjector;
import com.streamflow.producer.chaos.ChaosScenario;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * MockMvc slice test for {@link InternalChaosController}.
 *
 * <p>SPEC-13 Test Plan: verifies HTTP semantics (202, 204, 404, 400) without starting the full
 * Spring context.
 */
@WebMvcTest(InternalChaosController.class)
class InternalChaosControllerTest {

  @Autowired private MockMvc mockMvc;

  @Autowired private ObjectMapper objectMapper;

  @MockBean private ChaosInjector chaosInjector;

  @Test
  void postChaos_validRequest_returns202WithChaosId() throws Exception {
    when(chaosInjector.start(eq(ChaosScenario.HIGH_BUFFER), eq("stream-001"), eq(30)))
        .thenReturn("test-chaos-id-123");

    Map<String, Object> body =
        Map.of(
            "streamId", "stream-001",
            "scenario", "HIGH_BUFFER",
            "durationSeconds", 30);

    mockMvc
        .perform(
            post("/internal/chaos")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.chaosId").value("test-chaos-id-123"))
        .andExpect(jsonPath("$.startsAt").isNumber());
  }

  @Test
  void postChaos_invalidScenario_returns400() throws Exception {
    Map<String, Object> body =
        Map.of(
            "streamId", "stream-001",
            "scenario", "INVALID_SCENARIO",
            "durationSeconds", 30);

    mockMvc
        .perform(
            post("/internal/chaos")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void postChaos_durationTooLarge_returns400() throws Exception {
    Map<String, Object> body =
        Map.of(
            "streamId", "stream-001",
            "scenario", "HIGH_BUFFER",
            "durationSeconds", 400 // > max 300
            );

    mockMvc
        .perform(
            post("/internal/chaos")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void postChaos_durationZero_returns400() throws Exception {
    Map<String, Object> body =
        Map.of(
            "streamId", "stream-001",
            "scenario", "HIGH_BUFFER",
            "durationSeconds", 0 // < min 1
            );

    mockMvc
        .perform(
            post("/internal/chaos")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void deleteChaos_existingId_returns204() throws Exception {
    when(chaosInjector.cancel("existing-id")).thenReturn(true);

    mockMvc.perform(delete("/internal/chaos/existing-id")).andExpect(status().isNoContent());
  }

  @Test
  void deleteChaos_unknownId_returns404() throws Exception {
    when(chaosInjector.cancel("unknown-id")).thenReturn(false);

    mockMvc.perform(delete("/internal/chaos/unknown-id")).andExpect(status().isNotFound());
  }

  @Test
  void postChaos_blankStreamId_returns400() throws Exception {
    Map<String, Object> body =
        Map.of(
            "streamId", "",
            "scenario", "HIGH_BUFFER",
            "durationSeconds", 30);

    mockMvc
        .perform(
            post("/internal/chaos")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)))
        .andExpect(status().isBadRequest());
  }
}
