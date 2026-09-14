package com.jordimarcal.telemetry.hub.infrastructure.config;

import com.jordimarcal.telemetry.contracts.TopicNames;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declarative topology of the pipeline. KafkaAdmin applies these on startup;
 * partition count of the ingress topic spreads the adapter traffic.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic telemetryTopic() {
        return TopicBuilder.name(TopicNames.TELEMETRY).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic alertsTopic() {
        return TopicBuilder.name(TopicNames.ALERTS).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic telemetryDltTopic() {
        return TopicBuilder.name(TopicNames.TELEMETRY_DLT).partitions(1).replicas(1).build();
    }
}
