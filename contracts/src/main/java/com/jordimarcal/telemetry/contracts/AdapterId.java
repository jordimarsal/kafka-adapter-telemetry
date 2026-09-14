package com.jordimarcal.telemetry.contracts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Identifier of an API adapter. Invariant: lowercase alphanumeric with dashes,
 * 3 to 40 chars. Constructors fail fast; {@link #parse(String)} is the
 * boundary-friendly factory returning a {@link Result}.
 */
public record AdapterId(@JsonValue String value) {

    private static final Pattern SHAPE = Pattern.compile("[a-z0-9][a-z0-9-]{2,39}");

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public AdapterId {
        Objects.requireNonNull(value, "adapterId is required");
        value = value.strip();
        if (!SHAPE.matcher(value).matches()) {
            throw new IllegalArgumentException("adapterId must match [a-z0-9][a-z0-9-]{2,39}: " + value);
        }
    }

    public static Result<AdapterId, ValidationError> parse(String raw) {
        try {
            return Result.ok(new AdapterId(raw));
        } catch (IllegalArgumentException | NullPointerException e) {
            return Result.err(new ValidationError("adapterId", "must match [a-z0-9][a-z0-9-]{2,39}"));
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
