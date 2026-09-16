package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import com.jordimarcal.telemetry.contracts.TopicNames;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Watches the DLT topic so the dashboard can show dead letters in real time.
 * Counts only — recovery stays a manual, deliberate act on the raw topic.
 * The reason is derived from the DLT metadata the recoverer attaches, never
 * from the payload (which is exactly the unreadable thing that failed).
 */
@Component
public class DltObserver {

    private static final Logger log = LoggerFactory.getLogger(DltObserver.class);
    private static final int REASON_MAX = 80;
    private static final String DESERIALIZATION_EXCEPTION = "DeserializationException";

    private final InMemoryTelemetryMetrics metrics;

    DltObserver(InMemoryTelemetryMetrics metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(id = "dlt-observer", topics = TopicNames.TELEMETRY_DLT,
            containerFactory = "dltListenerContainerFactory")
    public void observe(ConsumerRecord<String, String> record) {
        log.debug("dlt message key={}", record.key());
        metrics.onDlt(reasonFor(record));
    }

    private static String reasonFor(ConsumerRecord<String, String> record) {
        String fqcn = header(record, KafkaHeaders.DLT_EXCEPTION_FQCN);
        if (fqcn != null) {
            String simpleName = fqcn.substring(fqcn.lastIndexOf('.') + 1);
            if (DESERIALIZATION_EXCEPTION.equals(simpleName)) {
                return "malformed JSON";
            }
            String message = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
            return message == null || message.isBlank() ? simpleName : truncate(simpleName + ": " + message);
        }
        String value = record.value();
        return value == null || value.isBlank() ? "empty record" : "unprocessable record";
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String truncate(String reason) {
        return reason.substring(0, Math.clamp(reason.length(), 0, REASON_MAX));
    }
}
