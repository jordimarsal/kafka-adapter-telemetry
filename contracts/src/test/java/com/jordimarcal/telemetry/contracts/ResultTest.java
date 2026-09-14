package com.jordimarcal.telemetry.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ResultTest {

    @Test
    void okCarriesValue() {
        assertTrue(Result.ok(3).isOk());
        assertEquals(3, Result.ok(3).orElseThrow());
    }

    @Test
    void errCarriesError() {
        Result<Integer, String> r = Result.err("boom");
        assertFalse(r.isOk());
        assertEquals("boom", r.error());
    }

    @Test
    void orElseThrowOnErrThrows() {
        assertThrows(IllegalStateException.class, () -> Result.<Integer, String>err("boom").orElseThrow());
        assertThrows(IllegalStateException.class, () -> Result.<Integer, String>ok(1).error());
    }

    @Test
    void foldPicksBranch() {
        assertEquals("yes", Result.<Integer, String>ok(1).fold(v -> "yes", e -> "no"));
        assertEquals("no", Result.<Integer, Integer>err(9).fold(v -> "yes", e -> "no"));
    }
}
