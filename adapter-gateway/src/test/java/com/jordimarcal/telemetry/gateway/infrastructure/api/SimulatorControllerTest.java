package com.jordimarcal.telemetry.gateway.infrastructure.api;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import com.jordimarcal.telemetry.gateway.application.SimulationReport;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(SimulatorController.class)
class SimulatorControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private PublishTelemetryUseCase useCase;

    @Test
    void knownProfileReturnsReport() throws Exception {
        Mockito.when(useCase.simulate(any()))
                .thenReturn(new SimulationReport("low", 20, 0, 0, 12));
        mvc.perform(post("/api/v1/telemetry/simulate?profile=low"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.profile").value("low"))
                .andExpect(jsonPath("$.published").value(20));
    }

    @Test
    void unknownProfileReturns400WithAvailableNames() throws Exception {
        mvc.perform(post("/api/v1/telemetry/simulate?profile=nope"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("nope")))
                .andExpect(jsonPath("$.available[*]", org.hamcrest.Matchers.hasItem("low")));
    }
}
