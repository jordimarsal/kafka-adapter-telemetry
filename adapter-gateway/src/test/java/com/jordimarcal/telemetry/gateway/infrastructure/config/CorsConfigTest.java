package com.jordimarcal.telemetry.gateway.infrastructure.config;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import com.jordimarcal.telemetry.gateway.infrastructure.api.TelemetryController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(TelemetryController.class)
@Import(CorsConfig.class)
class CorsConfigTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private PublishTelemetryUseCase useCase;

    @Test
    void preflightFromTheDashboardOriginIsAllowed() throws Exception {
        mvc.perform(options("/api/v1/telemetry")
                        .header(HttpHeaders.ORIGIN, "http://localhost:8082")
                        .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST"))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:8082"));
    }

    @Test
    void preflightFromAnyOtherOriginIsRejected() throws Exception {
        mvc.perform(options("/api/v1/telemetry")
                        .header(HttpHeaders.ORIGIN, "http://evil.example")
                        .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST"))
                .andExpect(status().isForbidden());
    }
}
