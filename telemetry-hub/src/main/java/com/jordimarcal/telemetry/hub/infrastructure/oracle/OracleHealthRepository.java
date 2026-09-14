package com.jordimarcal.telemetry.hub.infrastructure.oracle;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.hub.application.HealthRepository;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;
import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OracleHealthRepository implements HealthRepository {

    private static final String MERGE = """
            MERGE INTO adapter_health h
            USING (SELECT ? AS adapter_id FROM dual) src
            ON (h.adapter_id = src.adapter_id)
            WHEN MATCHED THEN UPDATE
               SET h.consecutive_down = ?, h.alert_active = ?, h.last_seen = ?
            WHEN NOT MATCHED THEN
               INSERT (adapter_id, consecutive_down, alert_active, last_seen)
               VALUES (?, ?, ?, ?)
            """;

    private static final String SELECT = """
            SELECT adapter_id, consecutive_down, alert_active, last_seen
            FROM adapter_health WHERE adapter_id = ?
            """;

    private final JdbcTemplate jdbc;

    public OracleHealthRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public AdapterHealth find(AdapterId adapterId) {
        try {
            return jdbc.queryForObject(SELECT, (rs, i) -> new AdapterHealth(
                    new AdapterId(rs.getString("adapter_id")),
                    rs.getInt("consecutive_down"),
                    rs.getInt("alert_active") == 1,
                    rs.getTimestamp("last_seen").toInstant()), adapterId.value());
        } catch (EmptyResultDataAccessException e) {
            return AdapterHealth.initial(adapterId, Instant.EPOCH);
        }
    }

    @Override
    public void save(AdapterHealth health) {
        int active = health.alertActive() ? 1 : 0;
        Timestamp seen = Timestamp.from(health.lastSeen());
        jdbc.update(MERGE, health.adapterId().value(),
                health.consecutiveDown(), active, seen,
                health.adapterId().value(), health.consecutiveDown(), active, seen);
    }
}
