package com.jordimarcal.telemetry.gateway.application;

import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.gateway.domain.TrafficGenerator;
import com.jordimarcal.telemetry.gateway.domain.TrafficProfile;
import java.time.Instant;
import java.util.List;
import tools.jackson.databind.ObjectMapper;

/**
 * Publishes telemetry. The simulated adapters of the demo live here — the
 * rest of the class stays a thin orchestration of pure collaborators.
 */
public final class PublishTelemetryUseCase {

    private static final List<String> DEMO_ADAPTERS =
            List.of("gateway-es-1", "gateway-uk-2", "gateway-de-3", "gateway-br-4");

    private final TelemetryPublisher publisher;
    private final ObjectMapper json;
    private final Sleeper sleeper;

    public PublishTelemetryUseCase(TelemetryPublisher publisher, ObjectMapper json, Sleeper sleeper) {
        this.publisher = publisher;
        this.json = json;
        this.sleeper = sleeper;
    }

    public void publish(TelemetryEvent event) {
        publisher.publish(event.adapterId().value(), json.writeValueAsString(event));
    }

    public SimulationReport simulate(TrafficProfile profile) {
        TrafficGenerator generator =
                new TrafficGenerator(profile, System.nanoTime(), DEMO_ADAPTERS, json::writeValueAsString);
        List<com.jordimarcal.telemetry.gateway.domain.PlannedMessage> plan = generator.generate(Instant.now());

        int published = 0;
        int duplicates = 0;
        int corrupt = 0;
        long startNanos = System.nanoTime();
        var it = plan.iterator();
        while (it.hasNext()) {
            var message = it.next();
            publisher.publish(message.key(), message.payloadJson());
            published += message.kind() == com.jordimarcal.telemetry.gateway.domain.PlannedMessage.Kind.NEW ? 1 : 0;
            duplicates += message.kind() == com.jordimarcal.telemetry.gateway.domain.PlannedMessage.Kind.DUPLICATE ? 1 : 0;
            corrupt += message.kind() == com.jordimarcal.telemetry.gateway.domain.PlannedMessage.Kind.CORRUPT ? 1 : 0;
            if (it.hasNext()) {
                sleeper.sleep(1_000L / profile.eventsPerSecond());
            }
        }
        return new SimulationReport(profile.name(), published, duplicates, corrupt, (System.nanoTime() - startNanos) / 1_000_000);
    }
}
