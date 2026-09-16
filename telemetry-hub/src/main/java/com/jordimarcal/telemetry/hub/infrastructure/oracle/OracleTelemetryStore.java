package com.jordimarcal.telemetry.hub.infrastructure.oracle;

import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.hub.application.TelemetryStore;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OracleTelemetryStore implements TelemetryStore {

    private static final String INSERT = """
            INSERT INTO telemetry_event (event_id, adapter_id, country, status, latency_ms, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;

    public OracleTelemetryStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Result<Long, DuplicateTelemetry> append(TelemetryEvent event) {
        try {
            jdbc.update(INSERT, uuidBytes(event.eventId()), event.adapterId().value(), event.country().code(),
                    event.status().name(), event.latencyMs().value(), Timestamp.from(event.occurredAt()));
            return Result.ok(1L);
        } catch (DuplicateKeyException e) {
            return Result.err(new DuplicateTelemetry(event.eventId()));
        }
    }

    @Override
    public void clear() {
        jdbc.execute("TRUNCATE TABLE telemetry_event");
    }

    static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }
}
