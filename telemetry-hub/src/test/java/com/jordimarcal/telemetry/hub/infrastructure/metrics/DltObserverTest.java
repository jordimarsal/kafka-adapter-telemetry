package com.jordimarcal.telemetry.hub.infrastructure.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;

class DltObserverTest {

    private final InMemoryTelemetryMetrics metrics = new InMemoryTelemetryMetrics();
    private final DltObserver observer = new DltObserver(metrics);
    private final List<String> reasons = new ArrayList<>();

    DltObserverTest() {
        metrics.addListener(frame -> {
            if (frame instanceof InMemoryTelemetryMetrics.Frame.DltFrame(var _, var reason)) {
                reasons.add(reason);
            }
        });
    }

    @Test
    void deserializationFailuresAreReportedAsMalformedJson() {
        observe(record("{\"eventId\": \"not-json", headers(
                KafkaHeaders.DLT_EXCEPTION_FQCN, "org.springframework.kafka.support.serializer.DeserializationException",
                KafkaHeaders.DLT_EXCEPTION_MESSAGE, "failed to deserialize")));
        assertEquals(List.of("malformed JSON"), reasons);
    }

    @Test
    void otherFailuresCarryTheExceptionNameAndMessage() {
        observe(record("{}", headers(
                KafkaHeaders.DLT_EXCEPTION_FQCN, "org.springframework.kafka.listener.ListenerExecutionFailedException",
                KafkaHeaders.DLT_EXCEPTION_MESSAGE, "unknown adapter profile")));
        assertEquals(List.of("ListenerExecutionFailedException: unknown adapter profile"), reasons);
    }

    @Test
    void failuresWithoutAMessageFallBackToTheExceptionName() {
        observe(record("{}", headers(
                KafkaHeaders.DLT_EXCEPTION_FQCN, "org.springframework.kafka.KafkaException",
                KafkaHeaders.DLT_EXCEPTION_MESSAGE, "  ")));
        assertEquals(List.of("KafkaException"), reasons);
    }

    @Test
    void longMessagesAreTruncated() {
        String longMessage = "boom ".repeat(40);
        observe(record("{}", headers(
                KafkaHeaders.DLT_EXCEPTION_FQCN, "org.springframework.kafka.KafkaException",
                KafkaHeaders.DLT_EXCEPTION_MESSAGE, longMessage)));
        assertEquals(1, reasons.size());
        assertEquals(80, reasons.getFirst().length());
    }

    @Test
    void recordsWithoutDltHeadersFallBackToAGenericLabel() {
        observe(record("{\"eventId\": \"not-json", new RecordHeaders()));
        assertEquals(List.of("unprocessable record"), reasons);
    }

    @Test
    void blankRecordsWithoutHeadersAreReportedAsEmpty() {
        observe(record(null, new RecordHeaders()));
        assertEquals(List.of("empty record"), reasons);
    }

    private void observe(ConsumerRecord<String, String> record) {
        observer.observe(record);
    }

    private static ConsumerRecord<String, String> record(String value, Headers headers) {
        return new ConsumerRecord<>("adapter.telemetry.v1.dlt", 0, 0L, 0L, TimestampType.CREATE_TIME,
                -1, -1, "gw-1", value, headers, Optional.empty());
    }

    private static Headers headers(String firstName, String firstValue, String secondName, String secondValue) {
        return new RecordHeaders()
                .add(firstName, firstValue.getBytes(StandardCharsets.UTF_8))
                .add(secondName, secondValue.getBytes(StandardCharsets.UTF_8));
    }
}
