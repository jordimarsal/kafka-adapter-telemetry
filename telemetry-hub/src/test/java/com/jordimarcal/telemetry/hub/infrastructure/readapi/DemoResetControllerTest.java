package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.hub.application.ResetDemoUseCase;
import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(DemoResetController.class)
class DemoResetControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private ResetDemoUseCase resetUseCase;

    @MockitoBean
    private InMemoryTelemetryMetrics metrics;

    @Test
    void resetInvokesTheUseCaseZeroesMetricsAndReturnsTheFreshSnapshot() throws Exception {
        when(metrics.seq()).thenReturn(9081L);
        when(metrics.totals()).thenReturn(new InMemoryTelemetryMetrics.Totals(0, 0, 0));

        mvc.perform(post("/api/v1/demo/reset"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(9081))
                .andExpect(jsonPath("$.totals.duplicates").value(0))
                .andExpect(jsonPath("$.totals.alerts").value(0))
                .andExpect(jsonPath("$.totals.dlt").value(0));

        verify(resetUseCase).reset();
        verify(metrics).reset();
    }
}
