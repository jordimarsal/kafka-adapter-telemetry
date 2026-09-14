package com.jordimarcal.telemetry.gateway.application;

/**
 * Outbound port: the only way the application talks to the event broker.
 * Payloads are raw JSON strings on purpose — corrupt messages must reach
 * the broker untouched to exercise the dead letter topic.
 */
public interface TelemetryPublisher {

    void publish(String key, String payloadJson);
}
