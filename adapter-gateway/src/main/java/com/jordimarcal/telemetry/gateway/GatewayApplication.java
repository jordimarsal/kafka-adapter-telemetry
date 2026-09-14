package com.jordimarcal.telemetry.gateway;

import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import com.jordimarcal.telemetry.gateway.application.Sleeper;
import com.jordimarcal.telemetry.gateway.application.TelemetryPublisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    @Bean
    public PublishTelemetryUseCase publishTelemetryUseCase(
            TelemetryPublisher publisher, ObjectMapper json, Sleeper sleeper) {
        return new PublishTelemetryUseCase(publisher, json, sleeper);
    }

    @Bean
    public Sleeper sleeper() {
        return GatewayApplication::pace;
    }

    private static void pace(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while pacing traffic", e);
        }
    }
}
