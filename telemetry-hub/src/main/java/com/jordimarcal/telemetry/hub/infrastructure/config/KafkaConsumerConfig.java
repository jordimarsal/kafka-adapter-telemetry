package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.contracts.TopicNames;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling of the telemetry consumer: after the configured backoff
 * budget is exhausted (or the failure is fatal, like malformed JSON), the
 * offending record is published unchanged to the DLT topic.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(
                        dltTemplate(bootstrapServers),
                        (record, ex) -> new TopicPartition(TopicNames.TELEMETRY_DLT, 0)),
                new FixedBackOff(500, 2));
    }

    private static KafkaTemplate<String, byte[]> dltTemplate(String bootstrapServers) {
        var factory = new DefaultKafkaProducerFactory<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        return new KafkaTemplate<>(factory);
    }
}
