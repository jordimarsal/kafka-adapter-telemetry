package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import com.jordimarcal.telemetry.contracts.TopicNames;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Watches the DLT topic so the dashboard can show dead letters in real time.
 * Counts only — recovery stays a manual, deliberate act on the raw topic.
 */
@Component
public class DltObserver {

    private static final Logger log = LoggerFactory.getLogger(DltObserver.class);
    private static final int REASON_MAX = 120;

    private final InMemoryTelemetryMetrics metrics;

    DltObserver(InMemoryTelemetryMetrics metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(id = "dlt-observer", topics = TopicNames.TELEMETRY_DLT,
            containerFactory = "dltListenerContainerFactory")
    public void observe(ConsumerRecord<String, String> record) {
        String value = record.value();
        String reason = value == null ? "unparseable"
                : value.substring(0, Math.clamp(value.length(), 0, REASON_MAX));
        log.debug("dlt message key={}", record.key());
        metrics.onDlt(reason);
    }
}
