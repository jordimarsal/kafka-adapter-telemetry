package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.TopicNames;

/**
 * Outbound port: publishes alerts to {@link TopicNames#ALERTS}.
 */
public interface AlertPublisher {

    void publish(AlertEvent alert);
}
