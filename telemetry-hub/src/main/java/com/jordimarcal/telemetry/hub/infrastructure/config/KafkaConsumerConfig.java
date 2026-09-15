package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.contracts.TopicNames;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling of the telemetry consumer: after the configured backoff
 * budget is exhausted (or the failure is fatal, like malformed JSON), the
 * offending record is published unchanged to the DLT topic.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(
                        dltTemplate(bootstrapServers),
                        (record, ex) -> new TopicPartition(TopicNames.TELEMETRY_DLT, 0)),
                new FixedBackOff(500, 2));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> dltListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "telemetry-hub-dlt",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        // no DeadLetterPublishingRecoverer here: an observer that republishes to its own topic loops forever
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (record, _) -> log.warn("dlt observer gave up on key={}", record.key()),
                new FixedBackOff(500, 2)));
        return factory;
    }

    private static KafkaTemplate<String, Object> dltTemplate(String bootstrapServers) {
        var factory = new DefaultKafkaProducerFactory<String, Object>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                new StringSerializer(),
                new RawOrJsonSerializer());
        return new KafkaTemplate<>(factory);
    }

    static final class RawOrJsonSerializer implements Serializer<Object> {

        private final JsonSerializer<Object> json = new JsonSerializer<>();

        @Override
        public byte[] serialize(String topic, Object data) {
            return data instanceof byte[] raw ? raw : json.serialize(topic, data);
        }
    }
}
