package com.jordimarcal.telemetry.hub.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.TopicNames;
import com.jordimarcal.telemetry.hub.application.AlertStore;
import com.jordimarcal.telemetry.hub.application.HealthRepository;
import com.jordimarcal.telemetry.hub.application.TelemetryStore;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;
import com.jordimarcal.telemetry.hub.infrastructure.metrics.InMemoryTelemetryMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.filter.annotation.TypeExcludeFilters;
import org.springframework.context.annotation.Bean;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import tools.jackson.databind.json.JsonMapper;

/**
 * Wiring test for the Kafka edge of the hub: a valid event reaches the use
 * case, a poisoned one lands on the DLT, and raised alerts reach the alerts
 * topic. Runs in the default build with an embedded broker and in-memory
 * stores — no Docker, no Oracle.
 */
@EmbeddedKafka(partitions = 1, topics = {TopicNames.ALERTS, TopicNames.TELEMETRY_DLT})
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"
})
@TypeExcludeFilters(TelemetryListenerIT.InMemoryStoresOnly.class)
class TelemetryListenerIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private InMemoryTelemetryStore store;

    @Autowired
    private InMemoryTelemetryMetrics metrics;

    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void validEventReachesTheStoreWithinFiveSeconds() {
        TelemetryEvent event = event("it-valid-adapter", Status.UP);

        producer().send(TopicNames.TELEMETRY, event.adapterId().value(), json.writeValueAsString(event));

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertTrue(store.byEventId().containsKey(event.eventId()), "event must be appended"));
    }

    @Test
    void malformedJsonIsRoutedToTheDlt() {
        String broken = "{\"trencat";
        String key = "it-broken-adapter";

        producer().send(TopicNames.TELEMETRY, key, broken);

        try (Consumer<String, byte[]> reader = dltReader()) {
            broker.consumeFromAnEmbeddedTopic(reader, TopicNames.TELEMETRY_DLT);
            var found = new AtomicReference<ConsumerRecord<String, byte[]>>();
            Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
                reader.poll(Duration.ofMillis(200)).forEach(record -> {
                    if (key.equals(record.key())) {
                        found.set(record);
                    }
                });
                assertNotNull(found.get(), "poisoned record must reach the DLT");
            });
            assertEquals(key, found.get().key());
            assertEquals(broken, new String(found.get().value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void dltObserverCountsMessagesLandingOnTheDlt() {
        producer().send(TopicNames.TELEMETRY, "it-dlt-count", "{\"trencat");

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertTrue(metrics.totals().dlt() >= 1, "dlt observer must count the dead letter"));
    }

    @Test
    void threeConsecutiveDownsPublishAnAlertToTheAlertsTopic() {
        for (int i = 0; i < 3; i++) {
            TelemetryEvent down = event("it-alert-adapter", Status.DOWN);
            producer().send(TopicNames.TELEMETRY, down.adapterId().value(), json.writeValueAsString(down));
        }

        try (Consumer<String, String> reader = alertsReader()) {
            broker.consumeFromAnEmbeddedTopic(reader, TopicNames.ALERTS);
            var record = KafkaTestUtils.getSingleRecord(reader, TopicNames.ALERTS, TIMEOUT);
            assertEquals("it-alert-adapter", record.key());
            AlertEvent alert = json.readValue(record.value(), AlertEvent.class);
            assertEquals("it-alert-adapter", alert.adapterId().value());
            assertTrue(alert.reason().contains("DOWN"), "reason must mention the DOWN streak");
        }
    }

    private KafkaTemplate<String, String> producer() {
        var factory = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
        return new KafkaTemplate<>(factory);
    }

    private Consumer<String, byte[]> dltReader() {
        return new KafkaConsumer<>(readerProps("it-dlt-reader"), new StringDeserializer(), new ByteArrayDeserializer());
    }

    private Consumer<String, String> alertsReader() {
        return new KafkaConsumer<>(readerProps("it-alerts-reader"), new StringDeserializer(), new StringDeserializer());
    }

    private Map<String, Object> readerProps(String group) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, group + "-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private TelemetryEvent event(String adapter, Status status) {
        return TelemetryEvent.of(UUID.randomUUID(), adapter, "ES", status.name(), 100, Instant.now()).orElseThrow();
    }

    static class InMemoryStoresOnly extends TypeExcludeFilter {

        @Override
        public boolean match(MetadataReader reader, MetadataReaderFactory factory) {
            String name = reader.getClassMetadata().getClassName();
            return name.startsWith("com.jordimarcal.telemetry.hub.infrastructure.oracle")
                    || name.startsWith("com.jordimarcal.telemetry.hub.infrastructure.readapi");
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof InMemoryStoresOnly;
        }

        @Override
        public int hashCode() {
            return InMemoryStoresOnly.class.hashCode();
        }
    }

    @TestConfiguration
    static class InMemoryConfig {

        @Bean
        InMemoryTelemetryStore inMemoryTelemetryStore() {
            return new InMemoryTelemetryStore();
        }

        @Bean
        InMemoryHealthRepository inMemoryHealthRepository() {
            return new InMemoryHealthRepository();
        }

        @Bean
        InMemoryAlertStore inMemoryAlertStore() {
            return new InMemoryAlertStore();
        }
    }

    static class InMemoryTelemetryStore implements TelemetryStore {

        private final Map<UUID, TelemetryEvent> appended = new LinkedHashMap<>();

        Map<UUID, TelemetryEvent> byEventId() {
            return appended;
        }

        @Override
        public synchronized Result<Long, DuplicateTelemetry> append(TelemetryEvent event) {
            if (appended.containsKey(event.eventId())) {
                return Result.err(new DuplicateTelemetry(event.eventId()));
            }
            appended.put(event.eventId(), event);
            return Result.ok((long) appended.size());
        }
    }

    static class InMemoryHealthRepository implements HealthRepository {

        private final Map<String, AdapterHealth> saved = new LinkedHashMap<>();

        @Override
        public AdapterHealth find(AdapterId adapterId) {
            return saved.getOrDefault(adapterId.value(), AdapterHealth.initial(adapterId, Instant.EPOCH));
        }

        @Override
        public void save(AdapterHealth health) {
            saved.put(health.adapterId().value(), health);
        }
    }

    static class InMemoryAlertStore implements AlertStore {

        private final List<AlertEvent> recorded = new CopyOnWriteArrayList<>();

        @Override
        public void record(AlertEvent alert) {
            recorded.add(alert);
        }
    }
}
