package com.jordimarcal.telemetry.hub.infrastructure.kafka;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TopicNames;
import com.jordimarcal.telemetry.hub.application.AlertPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes raised alerts to the alerts topic, keyed by adapter so every
 * consumer of a partition sees one adapter's alerts in order.
 */
@Component
public class KafkaAlertPublisher implements AlertPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaAlertPublisher.class);

    private final KafkaTemplate<String, AlertEvent> kafka;

    public KafkaAlertPublisher(KafkaTemplate<String, AlertEvent> kafka) {
        this.kafka = kafka;
    }

    @Override
    public void publish(AlertEvent alert) {
        kafka.send(TopicNames.ALERTS, alert.adapterId().value(), alert).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("alert publish failed adapter={}: {}", alert.adapterId(), ex.getMessage());
            } else {
                log.debug("alert published adapter={} partition={} offset={}",
                        alert.adapterId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
