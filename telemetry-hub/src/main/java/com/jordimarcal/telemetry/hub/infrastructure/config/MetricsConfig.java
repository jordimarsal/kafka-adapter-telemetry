package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Composition root of the metrics stream: one shared in-memory registry. */
@Configuration
public class MetricsConfig {

    @Bean
    InMemoryTelemetryMetrics inMemoryTelemetryMetrics() {
        return new InMemoryTelemetryMetrics();
    }
}
