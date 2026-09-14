package com.jordimarcal.telemetry.contracts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Country where an adapter operates. Only the four countries of the domain are allowed.
 */
public record Country(@JsonValue String code) {

    public static final Set<String> ALLOWED = Set.of("ES", "UK", "DE", "BR");

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public Country {
        Objects.requireNonNull(code, "country is required");
        code = code.strip().toUpperCase(Locale.ROOT);
        if (!ALLOWED.contains(code)) {
            throw new IllegalArgumentException("country must be one of " + ALLOWED + ": " + code);
        }
    }

    public static Result<Country, ValidationError> parse(String raw) {
        try {
            return Result.ok(new Country(raw));
        } catch (IllegalArgumentException | NullPointerException e) {
            return Result.err(new ValidationError("country", "must be one of " + ALLOWED));
        }
    }

    @Override
    public String toString() {
        return code;
    }
}
