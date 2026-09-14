package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jordimarcal.telemetry.contracts.AlertEvent;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Producer configuration for alerts: Jackson's java.time module renders
 * instants as ISO-8601, the same wire format the REST edge and the telemetry
 * topic already speak.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    KafkaTemplate<String, AlertEvent> alertTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        ObjectMapper json = JacksonUtils.enhancedObjectMapper();
        json.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        var serializer = new JsonSerializer<AlertEvent>(json);
        var factory = new DefaultKafkaProducerFactory<String, AlertEvent>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                new StringSerializer(), serializer);
        return new KafkaTemplate<>(factory);
    }
}
