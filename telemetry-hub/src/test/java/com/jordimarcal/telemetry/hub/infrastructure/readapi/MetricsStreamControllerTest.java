package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockReset;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@WebMvcTest(MetricsStreamController.class)
class MetricsStreamControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean(reset = MockReset.NONE)
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

    @Test
    void streamDeliversMetricsFramesAsNamedServerSentEvents() throws Exception {
        ArgumentCaptor<Consumer<InMemoryTelemetryMetrics.Frame>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(metrics).addListener(captor.capture());

        MvcResult result = mvc.perform(get("/api/v1/stream"))
                .andExpect(request().asyncStarted())
                .andReturn();

        captor.getValue().accept(new InMemoryTelemetryMetrics.Frame.TelemetryFrame(
                7, UUID.randomUUID(), "gateway-es-1", "UP", 120, "ES", Instant.parse("2026-09-15T10:00:00Z")));

        String body = result.getResponse().getContentAsString();
        assertTrue(body.contains("event:telemetry"));
        assertTrue(body.contains("gateway-es-1"));
        assertFalse(body.contains("event:heartbeat"));
    }
}
