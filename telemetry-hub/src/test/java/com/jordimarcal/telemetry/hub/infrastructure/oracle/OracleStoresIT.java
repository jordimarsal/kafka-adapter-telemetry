package com.jordimarcal.telemetry.hub.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.contracts.AlertEvent;
import com.jordimarcal.telemetry.contracts.Result;
import com.jordimarcal.telemetry.contracts.Status;
import com.jordimarcal.telemetry.contracts.TelemetryEvent;
import com.jordimarcal.telemetry.hub.application.AlertStore;
import com.jordimarcal.telemetry.hub.application.TelemetryStore;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Runs against a real Oracle Free in Docker (Testcontainers). Not part of the
 * default `mvn test` run: execute with
 * `mvn -pl telemetry-hub -am test -Dtest=OracleStoresIT -DfailIfNoTests=false`.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("integration")
class OracleStoresIT {

    @Container
    private static final OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart")
            .withDatabaseName("telemetry")
            .withUsername("telemetry")
            .withPassword("telemetry")
            .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(5)));

    private JdbcTemplate jdbc;
    private OracleTelemetryStore telemetryStore;
    private OracleHealthRepository healthRepository;
    private OracleAlertStore alertStore;

    @BeforeAll
    void start() {
        var dataSource = new DriverManagerDataSource(
                oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        telemetryStore = new OracleTelemetryStore(jdbc);
        healthRepository = new OracleHealthRepository(jdbc);
        alertStore = new OracleAlertStore(jdbc);
    }

    @Test
    void flywayCreatedTheThreeTables() {
        Integer tables = jdbc.queryForObject(
                "SELECT COUNT(*) FROM user_tables WHERE table_name IN ('TELEMETRY_EVENT','ADAPTER_HEALTH','ADAPTER_ALERT')",
                Integer.class);
        assertEquals(3, tables);
    }

    @Test
    void appendIsIdempotentByEventId() {
        var event = event(Status.UP, UUID.randomUUID());
        assertTrue(telemetryStore.append(event).isOk());
        Result<Long, TelemetryStore.DuplicateTelemetry> again = telemetryStore.append(event);
        assertTrue(again.isErr());
        assertEquals(event.eventId(), again.error().eventId());
        Integer rows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM telemetry_event WHERE event_id = ?",
                Integer.class, OracleTelemetryStore.uuidBytes(event.eventId()));
        assertEquals(1, rows);
    }

    @Test
    void healthFindStartsInitialAndMergePersistsStreak() {
        AdapterId id = new AdapterId("it-adapter-1");
        var health = healthRepository.find(id);
        assertEquals(0, health.consecutiveDown());

        var afterOne = health.observe(event(Status.DOWN, UUID.randomUUID(), Instant.now())).next();
        var afterTwo = afterOne.observe(event(Status.DOWN, UUID.randomUUID(), Instant.now())).next();
        healthRepository.save(afterTwo);

        var reloaded = healthRepository.find(id);
        assertEquals(2, reloaded.consecutiveDown());
        assertEquals(false, reloaded.alertActive());

        healthRepository.save(afterTwo.observe(event(Status.DOWN, UUID.randomUUID(), Instant.now())).next());
        assertEquals(3, healthRepository.find(id).consecutiveDown());
        assertEquals(true, healthRepository.find(id).alertActive());
    }

    @Test
    void alertRecordingToleratesReplays() {
        UUID trigger = UUID.randomUUID();
        AlertEvent alert = AlertEvent.forTrigger(trigger, new AdapterId("it-adapter-2"), "3 DOWN", Instant.now());
        alertStore.record(alert);
        alertStore.record(alert);
        Integer rows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM adapter_alert WHERE alert_id = ?",
                Integer.class, OracleTelemetryStore.uuidBytes(alert.alertId()));
        assertEquals(1, rows);
    }

    private static TelemetryEvent event(Status status, UUID eventId) {
        return event(status, eventId, Instant.now());
    }

    private static TelemetryEvent event(Status status, UUID eventId, Instant at) {
        return TelemetryEvent.of(eventId, "it-adapter-1", "ES", status.name(), 250, at).orElseThrow();
    }
}
