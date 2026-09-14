package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;
import com.jordimarcal.telemetry.hub.domain.HealthEffect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes one telemetry event: stores it (idempotently), tells the adapter's
 * health aggregate what happened, persists the new state and, if the aggregate
 * raised an alert, records it and places it on the wire. The use case never
 * inspects event fields to make decisions — the aggregate owns that judgement.
 */
public final class ProcessTelemetryUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTelemetryUseCase.class);

    private final TelemetryStore telemetryStore;
    private final HealthRepository healthRepository;
    private final AlertStore alertStore;
    private final AlertPublisher alertPublisher;

    public ProcessTelemetryUseCase(
            TelemetryStore telemetryStore, HealthRepository healthRepository, AlertStore alertStore,
            AlertPublisher alertPublisher) {
        this.telemetryStore = telemetryStore;
        this.healthRepository = healthRepository;
        this.alertStore = alertStore;
        this.alertPublisher = alertPublisher;
    }

    public void process(TelemetryEvent event) {
        switch (telemetryStore.append(event)) {
            case Result.Err(var duplicate) -> {
                log.debug("duplicate telemetry {} ignored", duplicate.eventId());
                return;
            }
            case Result.Ok(var ignored) -> { }
        }

        AdapterHealth current = healthRepository.find(event.adapterId());
        HealthEffect effect = current.observe(event);
        healthRepository.save(effect.next());
        log.info("telemetry processed adapter={} status={} consecutiveDown={}",
                event.adapterId(), event.status(), effect.next().consecutiveDown());

        effect.alertToPublish().ifPresent(alert -> {
            alertStore.record(alert);
            alertPublisher.publish(alert);
            log.warn("alert raised adapter={} reason={}", alert.adapterId(), alert.reason());
        });
    }
}
