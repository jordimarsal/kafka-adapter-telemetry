package com.jordimarcal.telemetry.gateway.infrastructure.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.jordimarcal.telemetry.contracts.ValidationError;
import java.util.List;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiError(String message, List<ValidationError> errors, Set<String> available) {

    public static ApiError of(List<ValidationError> errors) {
        return new ApiError(null, List.copyOf(errors), null);
    }

    public static ApiError unknownProfile(String requested, Set<String> available) {
        return new ApiError("unknown profile: " + requested, null, available);
    }
}
