package com.jordimarcal.telemetry.hub.infrastructure.kafka;

import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.contracts.TopicNames;
import com.jordimarcal.telemetry.hub.application.ProcessTelemetryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka edge of the hub: hands every well-formed telemetry event to the use
 * case. Malformed payloads never reach this method — the container's error
 * handler routes them to the DLT (see KafkaConsumerConfig).
 */
@Component
public class TelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(TelemetryListener.class);

    private final ProcessTelemetryUseCase useCase;

    public TelemetryListener(ProcessTelemetryUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(topics = TopicNames.TELEMETRY, groupId = "telemetry-hub")
    public void onTelemetry(TelemetryEvent event) {
        log.info("telemetry received adapter={} eventId={}", event.adapterId(), event.eventId());
        useCase.process(event);
    }
}
