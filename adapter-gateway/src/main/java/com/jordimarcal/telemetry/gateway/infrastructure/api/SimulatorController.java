package com.jordimarcal.telemetry.gateway.infrastructure.api;

import com.jordimarcal.telemetry.gateway.application.PublishTelemetryUseCase;
import com.jordimarcal.telemetry.gateway.domain.TrafficProfile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/telemetry")
public class SimulatorController {

    private final PublishTelemetryUseCase useCase;

    public SimulatorController(PublishTelemetryUseCase useCase) {
        this.useCase = useCase;
    }

    @PostMapping("/simulate")
    public ResponseEntity<?> simulate(@RequestParam("profile") String profile) {
        return TrafficProfile.named(profile).<ResponseEntity<?>>fold(
                p -> ResponseEntity.ok(useCase.simulate(p)),
                unknown -> ResponseEntity.badRequest().body(ApiError.unknownProfile(unknown.requested(), unknown.available())));
    }
}
