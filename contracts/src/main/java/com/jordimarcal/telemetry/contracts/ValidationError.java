package com.jordimarcal.telemetry.contracts;

/**
 * A single validation failure: which field and why.
 */
public record ValidationError(String field, String message) {
}
