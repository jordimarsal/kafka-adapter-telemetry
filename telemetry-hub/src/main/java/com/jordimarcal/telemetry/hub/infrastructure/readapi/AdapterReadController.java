package com.jordimarcal.telemetry.hub.infrastructure.readapi;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read side of the hub: how operations see each adapter's health, its latest
 * events and the alerts it raised. Pure projection — nothing here mutates.
 */
@RestController
public class AdapterReadController {

    static final String LIST_HEALTH = """
            SELECT adapter_id, consecutive_down, alert_active, last_seen
            FROM adapter_health
            ORDER BY adapter_id""";

    static final String FIND_HEALTH = """
            SELECT adapter_id, consecutive_down, alert_active, last_seen
            FROM adapter_health
            WHERE adapter_id = ?""";

    static final String LAST_EVENTS = """
            SELECT event_id, country, status, latency_ms, occurred_at
            FROM telemetry_event
            WHERE adapter_id = ?
            ORDER BY occurred_at DESC
            FETCH FIRST 20 ROWS ONLY""";

    static final String LAST_ALERTS = """
            SELECT alert_id, reason, raised_at
            FROM adapter_alert
            WHERE adapter_id = ?
            ORDER BY raised_at DESC
            FETCH FIRST 20 ROWS ONLY""";

    private static final RowMapper<AdapterSummary> SUMMARY = AdapterReadController::summary;
    private static final RowMapper<EventView> EVENT = AdapterReadController::event;
    private static final RowMapper<AlertView> ALERT = AdapterReadController::alert;

    private final JdbcTemplate jdbc;

    public AdapterReadController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/api/v1/adapters")
    public List<AdapterSummary> listAdapters() {
        return jdbc.query(LIST_HEALTH, SUMMARY);
    }

    @GetMapping("/api/v1/adapters/{adapterId}")
    public ResponseEntity<AdapterDetail> adapterDetail(@PathVariable String adapterId) {
        List<AdapterSummary> found = jdbc.query(FIND_HEALTH, SUMMARY, adapterId);
        if (found.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        AdapterSummary health = found.getFirst();
        return ResponseEntity.ok(new AdapterDetail(
                health.adapterId(), health.consecutiveDown(), health.alertActive(), health.lastSeen(),
                jdbc.query(LAST_EVENTS, EVENT, adapterId),
                jdbc.query(LAST_ALERTS, ALERT, adapterId)));
    }

    private static AdapterSummary summary(ResultSet rs, int rowNum) throws SQLException {
        return new AdapterSummary(
                rs.getString("adapter_id"),
                rs.getInt("consecutive_down"),
                rs.getInt("alert_active") == 1,
                toInstant(rs, "last_seen"));
    }

    private static EventView event(ResultSet rs, int rowNum) throws SQLException {
        return new EventView(
                toUuid(rs.getBytes("event_id")),
                rs.getString("country"),
                rs.getString("status"),
                rs.getInt("latency_ms"),
                toInstant(rs, "occurred_at"));
    }

    private static AlertView alert(ResultSet rs, int rowNum) throws SQLException {
        return new AlertView(toUuid(rs.getBytes("alert_id")), rs.getString("reason"), toInstant(rs, "raised_at"));
    }

    private static Instant toInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static UUID toUuid(byte[] raw) {
        if (raw == null) {
            return null;
        }
        ByteBuffer packed = ByteBuffer.wrap(raw);
        return new UUID(packed.getLong(), packed.getLong());
    }

    public record AdapterSummary(String adapterId, int consecutiveDown, boolean alertActive, Instant lastSeen) {
    }

    public record AdapterDetail(
            String adapterId, int consecutiveDown, boolean alertActive, Instant lastSeen,
            List<EventView> lastEvents, List<AlertView> alerts) {
    }

    public record EventView(UUID eventId, String country, String status, int latencyMs, Instant occurredAt) {
    }

    public record AlertView(UUID alertId, String reason, Instant raisedAt) {
    }
}
