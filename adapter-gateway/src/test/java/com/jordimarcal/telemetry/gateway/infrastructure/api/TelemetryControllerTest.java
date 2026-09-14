package com.jordimarcal.telemetry.gateway.infrastructure.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(TelemetryController.class)
class TelemetryControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase useCase;

    @Test
    void validEventReturns201() throws Exception {
        mvc.perform(post("/api/v1/telemetry")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"adapterId\":\"gateway-es-1\",\"country\":\"ES\",\"status\":\"UP\",\"latencyMs\":120}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.adapterId").value("gateway-es-1"))
                .andExpect(jsonPath("$.eventId").isNotEmpty());
    }

    @Test
    void invalidEventReturns400WithFieldErrors() throws Exception {
        mvc.perform(post("/api/v1/telemetry")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"adapterId\":\"BAD\",\"country\":\"FR\",\"status\":\"MAYBE\",\"latencyMs\":99999}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("adapterId"));
    }
}
