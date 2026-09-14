package com.jordimarcal.telemetry.contracts;

/**
 * Topic names of the pipeline. Versioned suffix (v1) so the schema can evolve
 * by publishing to a new topic instead of mutating this one.
 */
public final class TopicNames {

    public static final String TELEMETRY = "adapter.telemetry.v1";
    public static final String ALERTS = "adapter.alerts.v1";
    public static final String TELEMETRY_DLT = "adapter.telemetry.v1.dlt";

    private TopicNames() {
    }
}
