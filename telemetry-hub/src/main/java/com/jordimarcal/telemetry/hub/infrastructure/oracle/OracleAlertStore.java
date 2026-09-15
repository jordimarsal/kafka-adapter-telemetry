package com.jordimarcal.telemetry.hub.infrastructure.oracle;

import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.hub.application.AlertStore;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OracleAlertStore implements AlertStore {

    private static final Logger log = LoggerFactory.getLogger(OracleAlertStore.class);

    private static final String INSERT = """
            INSERT INTO adapter_alert (alert_id, adapter_id, reason, raised_at, trigger_event_id)
            VALUES (?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;

    public OracleAlertStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void record(AlertEvent alert) {
        try {
            jdbc.update(INSERT, OracleTelemetryStore.uuidBytes(alert.alertId()), alert.adapterId().value(),
                    alert.reason(), Timestamp.from(alert.raisedAt()),
                    OracleTelemetryStore.uuidBytes(alert.triggerEventId()));
        } catch (DuplicateKeyException _) {
            log.debug("alert {} already recorded (replay)", alert.alertId());
        }
    }
}
