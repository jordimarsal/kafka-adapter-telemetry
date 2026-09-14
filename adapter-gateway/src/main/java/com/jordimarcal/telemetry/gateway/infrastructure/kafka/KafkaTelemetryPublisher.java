package com.jordimarcal.telemetry.gateway.infrastructure.kafka;

import com.jordimarcal.telemetry.contracts.TopicNames;
import com.jordimarcal.telemetry.gateway.application.TelemetryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaTelemetryPublisher implements TelemetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTelemetryPublisher.class);

    private final KafkaTemplate<String, String> kafka;

    public KafkaTelemetryPublisher(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @Override
    public void publish(String key, String payloadJson) {
        kafka.send(TopicNames.TELEMETRY, key, payloadJson).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("publish failed for adapter key={}: {}", key, ex.getMessage());
            } else {
                log.debug("published key={} partition={} offset={}",
                        key, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
