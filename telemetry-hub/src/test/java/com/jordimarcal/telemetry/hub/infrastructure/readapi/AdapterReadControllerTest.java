package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.hub.infrastructure.readapi.AdapterReadController.AdapterSummary;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(AdapterReadController.class)
class AdapterReadControllerTest {

    private static final AdapterSummary SUMMARY =
            new AdapterSummary("gateway-es-1", 2, true, Instant.parse("2026-09-14T10:05:00Z"));

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private JdbcTemplate jdbc;

    @Test
    void listReturnsEveryAdapterWithItsHealth() throws Exception {
        whenHealthQuery().thenReturn(List.of(SUMMARY));

        mvc.perform(get("/api/v1/adapters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].adapterId").value("gateway-es-1"))
                .andExpect(jsonPath("$[0].consecutiveDown").value(2))
                .andExpect(jsonPath("$[0].alertActive").value(true))
                .andExpect(jsonPath("$[0].lastSeen").value("2026-09-14T10:05:00Z"));
    }

    @Test
    void detailReturnsHealthWithLastEventsAndAlerts() throws Exception {
        when(jdbc.query(eq(AdapterReadController.FIND_HEALTH), ArgumentMatchers.<RowMapper<AdapterSummary>>any(), eq("gateway-es-1")))
                .thenReturn(List.of(SUMMARY));
        when(jdbc.query(eq(AdapterReadController.LAST_EVENTS), ArgumentMatchers.<RowMapper<AdapterReadController.EventView>>any(), eq("gateway-es-1")))
                .thenReturn(List.of(new AdapterReadController.EventView(
                        UUID.fromString("00000000-0000-0000-0000-000000000001"), "ES", "DOWN", 250,
                        Instant.parse("2026-09-14T10:04:00Z"))));
        when(jdbc.query(eq(AdapterReadController.LAST_ALERTS), ArgumentMatchers.<RowMapper<AdapterReadController.AlertView>>any(), eq("gateway-es-1")))
                .thenReturn(List.of(new AdapterReadController.AlertView(
                        UUID.fromString("00000000-0000-0000-0000-000000000002"), "3 consecutive DOWN observations",
                        Instant.parse("2026-09-14T10:04:30Z"))));

        mvc.perform(get("/api/v1/adapters/gateway-es-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.adapterId").value("gateway-es-1"))
                .andExpect(jsonPath("$.consecutiveDown").value(2))
                .andExpect(jsonPath("$.alertActive").value(true))
                .andExpect(jsonPath("$.lastEvents[0].eventId").value("00000000-0000-0000-0000-000000000001"))
                .andExpect(jsonPath("$.lastEvents[0].status").value("DOWN"))
                .andExpect(jsonPath("$.alerts[0].reason").value("3 consecutive DOWN observations"));
    }

    @Test
    void unknownAdapterReturns404() throws Exception {
        whenHealthQuery().thenReturn(List.of());

        mvc.perform(get("/api/v1/adapters/ghost-adapter"))
                .andExpect(status().isNotFound());
    }

    private OngoingStubbing<List<AdapterSummary>> whenHealthQuery() {
        return when(jdbc.query(eq(AdapterReadController.LIST_HEALTH),
                ArgumentMatchers.<RowMapper<AdapterSummary>>any()));
    }
}
