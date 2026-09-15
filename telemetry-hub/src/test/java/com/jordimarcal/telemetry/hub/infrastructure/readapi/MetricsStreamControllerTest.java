package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(MetricsStreamController.class)
class MetricsStreamControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private InMemoryTelemetryMetrics metrics;

    @Test
    void snapshotReturnsSeqAndTotals() throws Exception {
        when(metrics.seq()).thenReturn(42L);
        when(metrics.totals()).thenReturn(new InMemoryTelemetryMetrics.Totals(95, 14, 20));

        mvc.perform(get("/api/v1/metrics/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seq").value(42))
                .andExpect(jsonPath("$.totals.duplicates").value(95))
                .andExpect(jsonPath("$.totals.alerts").value(14))
                .andExpect(jsonPath("$.totals.dlt").value(20));
    }
}
