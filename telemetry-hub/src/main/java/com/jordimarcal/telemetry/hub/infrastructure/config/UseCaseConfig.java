package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.hub.application.AlertStore;
import com.jordimarcal.telemetry.hub.application.AlertPublisher;
import com.jordimarcal.telemetry.hub.application.HealthRepository;
import com.jordimarcal.telemetry.hub.application.ProcessTelemetryUseCase;
import com.jordimarcal.telemetry.hub.application.TelemetryStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Composition root of the use case: pure application logic wired to its
 * ports. Keeps Spring out of the application layer.
 */
@Configuration
public class UseCaseConfig {

    @Bean
    ProcessTelemetryUseCase processTelemetryUseCase(
            TelemetryStore telemetryStore, HealthRepository healthRepository, AlertStore alertStore,
            AlertPublisher alertPublisher) {
        return new ProcessTelemetryUseCase(telemetryStore, healthRepository, alertStore, alertPublisher);
    }
}
