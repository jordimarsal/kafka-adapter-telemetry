package com.jordimarcal.telemetry.gateway.infrastructure.api;

import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/telemetry")
public class TelemetryController {

    private final PublishTelemetryUseCase useCase;

    public TelemetryController(PublishTelemetryUseCase useCase) {
        this.useCase = useCase;
    }

    @PostMapping
    public ResponseEntity<?> receive(@RequestBody TelemetryRequest request) {
        return TelemetryEvent.of(
                        request.eventId() != null ? request.eventId() : UUID.randomUUID(),
                        request.adapterId(),
                        request.country(),
                        request.status(),
                        request.latencyMs(),
                        request.occurredAt() != null ? request.occurredAt() : Instant.now())
                .fold(
                        event -> {
                            useCase.publish(event);
                            return ResponseEntity.status(201).body(event);
                        },
                        errors -> ResponseEntity.badRequest().body(ApiError.of(errors)));
    }

    public record TelemetryRequest(
            UUID eventId, String adapterId, String country, String status, Integer latencyMs, Instant occurredAt) {
    }
}
