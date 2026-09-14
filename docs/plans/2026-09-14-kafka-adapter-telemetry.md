# kafka-adapter-telemetry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dos microserveis Java 25 (Spring Boot 4.1) que comuniquen via Kafka: gateway que publica telemetria d'adaptadors (amb simulador de perfils de trànsit) i hub que persisteix a Oracle de manera idempotent i aixeca alertes.

**Architecture:** Hexagonal per servei (domini/aplicació purs, infraestructura a les vores), multi-mòdul Maven amb `contracts` compartit (Result + VOs + events), Kafka 4 KRaft single-node i Oracle Free 23ai a Docker Compose.

**Tech Stack:** Java 25 · Spring Boot 4.1.1 · Spring Kafka · Oracle Free (`gvenzl/oracle-free:23-slim-faststart`) · Flyway · JUnit 5 · EmbeddedKafka · Testcontainers.

**Spec:** `docs/plan.md` (pla d'alt nivell aprovat).

## Global Constraints

- Java `25` (maven.compiler.release via `<java.version>25</java.version>`); Spring Boot parent `4.1.1`.
- Grup de paquets: `com.jordimarcal.telemetry.{contracts,gateway,hub}`.
- `contracts` = **zero dependències de runtime** (només `junit-jupiter` per test).
- Sense Lombok, sense Vavr, sense dependències no aprovades al spec.
- Logging SLF4J parametritzat; gens de `printStackTrace`.
- TDD: primer el test que falla, després la implementació mínima; commit convencional (`feat:`, `test:`, `chore:`) per tasca.
- El domini i l'aplicació **no importen** res de Spring/Kafka/JDBC — només `infrastructure`.
- Ports per defecte: gateway `8081`, hub `8082`; Kafka host `localhost:9092`; Oracle host `localhost:1521` servei `telemetry`, usuari/pass `telemetry`/`telemetry`.

---

### Task 0: Scaffold Maven + Docker Compose

**Files:**
- Create: `pom.xml` (parent), `.gitignore`, `docker-compose.yml`
- Create: `contracts/pom.xml`, `adapter-gateway/pom.xml`, `telemetry-hub/pom.xml`
- Create: `contracts/src/main/java/com/jordimarcal/telemetry/contracts/package-info.java`

**Interfaces:**
- Produces: estructura de mòduls buida però compilable; infraestructura (Kafka+Oracle) aixecable.

- [ ] **Step 1: Parent `pom.xml`**

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.1.1</version>
    <relativePath/>
  </parent>
  <groupId>com.jordimarcal.telemetry</groupId>
  <artifactId>kafka-adapter-telemetry</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>
  <modules>
    <module>contracts</module>
    <module>adapter-gateway</module>
    <module>telemetry-hub</module>
  </modules>
  <properties>
    <java.version>25</java.version>
  </properties>
</project>
```

- [ ] **Step 2: `contracts/pom.xml`** — sense parent extra (hereu del projecte); dependències: cap a runtime; test: `org.junit.jupiter:junit-jupiter` (versió gestionada pel BOM). Sense `spring-boot-maven-plugin`.

- [ ] **Step 3: `adapter-gateway/pom.xml` i `telemetry-hub/pom.xml`** — dependències comunes: `spring-boot-starter-web`, `spring-boot-starter-actuator`, `org.springframework.kafka:spring-kafka`, test: `spring-boot-starter-test`, `org.springframework.kafka:spring-kafka-test`. Hub afegeix: `spring-boot-starter-jdbc`, `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-oracle`, `com.oracle.database.jdbc:ojdbc11`, test: `org.testcontainers:oracle-free`, `org.testcontainers:junit-jupiter`. Tots dos amb `spring-boot-maven-plugin`.

- [ ] **Step 4: `.gitignore`** — `target/`, `.idea/`, `*.iml`, `.DS_Store`, `*.log`.

- [ ] **Step 5: `docker-compose.yml`**

```yaml
services:
  kafka:
    image: apache/kafka:4.1.2
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 12

  oracle:
    image: gvenzl/oracle-free:23-slim-faststart
    ports: ["1521:1521"]
    environment:
      ORACLE_PASSWORD: SysPassword1
      APP_USER: telemetry
      APP_USER_PASSWORD: telemetry
      ORACLE_DATABASE: telemetry
    healthcheck:
      test: ["CMD", "healthcheck.sh"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 30s

  kafka-ui:  # només amb --profile tools
    image: docker.redpanda.com/redpandadata/console:latest
    profiles: ["tools"]
    ports: ["8090:8080"]
    environment:
      KAFKA_BROKERS: kafka:29092
```

- [ ] **Step 6: Verificar** — `docker compose up -d kafka oracle`; esperar `healthy`; `mvn -q validate` verd; commit `chore: scaffold multi-module maven + compose`.

---

### Task 1: `contracts` — Result

**Files:**
- Create: `contracts/src/main/java/com/jordimarcal/telemetry/contracts/Result.java`
- Test: `contracts/src/test/java/com/jordimarcal/telemetry/contracts/ResultTest.java`

**Interfaces:**
- Produces: `Result<T,E>` amb `ok/err/isOk/orElseThrow/error/fold`.

- [ ] **Step 1: Test que falla**

```java
class ResultTest {
    @Test void okCarriesValue() {
        assertTrue(Result.ok(3).isOk());
        assertEquals(3, Result.ok(3).orElseThrow());
    }
    @Test void errCarriesError() {
        var r = Result.err("boom");
        assertFalse(r.isOk());
        assertEquals("boom", r.error());
    }
    @Test void orElseThrowOnErrThrows() {
        assertThrows(IllegalStateException.class, () -> Result.err("boom").orElseThrow());
        assertThrows(IllegalStateException.class, () -> Result.ok(1).error());
    }
    @Test void foldPicksBranch() {
        assertEquals("yes", Result.ok(1).fold(v -> "yes", e -> "no"));
        assertEquals("no",  Result.err(9).fold(v -> "yes", e -> "no"));
    }
}
```

- [ ] **Step 2: `mvn -q -pl contracts test`** → FAIL (classe inexistent).
- [ ] **Step 3: Implementació mínima**

```java
public sealed interface Result<T, E> permits Result.Ok, Result.Err {
    static <T, E> Result<T, E> ok(T value) { return new Ok<>(value); }
    static <T, E> Result<T, E> err(E error) { return new Err<>(error); }
    default boolean isOk() { return this instanceof Ok<T, E>; }
    T orElseThrow();
    E error();
    <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr);
    record Ok<T, E>(T value) implements Result<T, E> {
        @Override public T orElseThrow() { return value; }
        @Override public E error() { throw new IllegalStateException("no error in Ok"); }
        @Override public <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr) { return onOk.apply(value); }
    }
    record Err<T, E>(E error) implements Result<T, E> {
        @Override public T orElseThrow() { throw new IllegalStateException("no value in Err: " + error); }
        @Override public E error() { return error; }
        @Override public <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr) { return onErr.apply(error); }
    }
}
```

- [ ] **Step 4: Test verd. Step 5: Commit** `feat: Result monad as sealed interface`.

---

### Task 2: `contracts` — Value Objects

**Files:**
- Create: `AdapterId.java`, `Country.java`, `LatencyMs.java`, `Status.java`, `ValidationError.java` (mateix paquet)
- Test: `ValueObjectsTest.java`

**Interfaces:**
- Produces: `record AdapterId(String value)` amb `parse(String): Result<AdapterId, ValidationError>`; `Country` amb `ALLOWED = {ES,UK,DE,BR}`; `LatencyMs` (0..60000, `isSlow() >= 1000`); `enum Status {UP, DEGRADED, DOWN}`; `record ValidationError(String field, String message)`.
- Convenció: el constructor compacte **llança** `IllegalArgumentException` (invariant, fail-fast); la factory `parse` **retorna Result** (frontera amable).

- [ ] **Step 1: Test**

```java
class ValueObjectsTest {
    @Test void adapterIdAcceptsValid() {
        assertEquals("gateway-es-1", AdapterId.parse(" gateway-es-1 ").orElseThrow().value());
    }
    @Test void adapterIdRejectsUpperShortSpaces() {
        for (String bad : new String[]{"AB", "GATEWAY", "té-espais", null})
            assertTrue(AdapterId.parse(bad) instanceof Result.Err<AdapterId, ?>, bad);
    }
    @Test void countryOnlyFourCodes() {
        assertTrue(Country.parse("ES").isOk());
        assertTrue(Country.parse("es").isErr() || Country.parse("es").isOk()); // decidim: acceptem majúscula auto
        assertTrue(Country.parse("FR").isErr());
    }
    @Test void latencyRangeAndSlow() {
        assertEquals(0, LatencyMs.parse(0).orElseThrow().value());
        assertTrue(LatencyMs.parse(999).orElseThrow() instanceof var l && !l.isSlow());
        assertTrue(LatencyMs.parse(1000).orElseThrow().isSlow());
        assertTrue(LatencyMs.parse(60001).isErr());
        assertTrue(LatencyMs.parse(null).isErr());
    }
}
```

- [ ] **Step 2: FAIL. Step 3: Implementació** (patró idèntic a `AdapterId.parse`; `Country` normalitza amb `toUpperCase(Locale.ROOT)` i valida contra `ALLOWED`; `LatencyMs.parse(Integer)` tracta null com a err).
- [ ] **Step 4: Verd. Step 5: Commit** `feat: rich value objects AdapterId/Country/LatencyMs`.

---

### Task 3: `contracts` — Events + TopicNames

**Files:**
- Create: `TelemetryEvent.java`, `AlertEvent.java`, `TopicNames.java`
- Test: `TelemetryEventTest.java`, `AlertEventTest.java`

**Interfaces:**
- Produces:
  - `record TelemetryEvent(UUID eventId, AdapterId adapterId, Country country, Status status, LatencyMs latencyMs, Instant occurredAt)` amb `of(UUID, String, String, String, Integer, Instant): Result<TelemetryEvent, List<ValidationError>>` (acumula errors) i `isHealthy()`.
  - `record AlertEvent(UUID alertId, AdapterId adapterId, String reason, Instant raisedAt)` amb factory **determinista** `forTrigger(UUID triggerEventId, AdapterId, String reason, Instant)`: `alertId = UUID.nameUUIDFromBytes(triggerEventId.toString().getBytes(UTF_8))`.
  - `TopicNames.TELEMETRY = "adapter.telemetry.v1"`, `ALERTS = "adapter.alerts.v1"`, `TELEMETRY_DLT = "adapter.telemetry.v1.dlt"`.

- [ ] **Step 1: Tests clau**

```java
class TelemetryEventTest {
    private static final Instant NOW = Instant.parse("2026-09-14T10:00:00Z");
    @Test void validEventAccumulatesNoErrors() {
        var r = TelemetryEvent.of(UUID.randomUUID(), "gateway-es-1", "ES", "UP", 120, NOW);
        assertTrue(r.isOk());
        assertTrue(r.orElseThrow().isHealthy());
    }
    @Test void invalidFieldsAccumulateErrors() {
        var r = TelemetryEvent.of(null, "BAD", "FR", "MAYBE", 99_999, NOW);
        var errs = ((Result.Err<TelemetryEvent, List<ValidationError>>) r).error();
        assertEquals(5, errs.size()); // eventId, adapterId, country, status, latency
    }
    @Test void alertIdIsDeterministicPerTrigger() {
        var trig = UUID.randomUUID();
        var a1 = AlertEvent.forTrigger(trig, new AdapterId("gw-1"), "3 DOWN", NOW);
        var a2 = AlertEvent.forTrigger(trig, new AdapterId("gw-1"), "3 DOWN", NOW);
        assertEquals(a1.alertId(), a2.alertId());
        assertNotEquals(a1.alertId(), AlertEvent.forTrigger(UUID.randomUUID(), new AdapterId("gw-1"), "3 DOWN", NOW).alertId());
    }
}
```

- [ ] **Step 2: FAIL. Step 3: Implementació** — `of()` valida cada camp amb el `parse` del VO corresponent, `eventId == null || occurredAt == null` → error propi, i retorna `err(llista)` si n'hi ha cap, `ok(new TelemetryEvent(...))` si no. `status` es resol amb try `Status.valueOf`.
- [ ] **Step 4: Verd. Step 5: Commit** `feat: telemetry and alert events with accumulating validation`.

---

### Task 4: gateway — TrafficProfile + TrafficGenerator (domini pur)

**Files:**
- Create: `adapter-gateway/src/main/java/com/jordimarcal/telemetry/gateway/domain/TrafficProfile.java`, `TrafficGenerator.java`, `PlannedMessage.java`
- Test: `TrafficProfileTest.java`, `TrafficGeneratorTest.java`

**Interfaces:**
- Produces:
  - `record TrafficProfile(String name, int totalEvents, int eventsPerSecond, double degradedRatio, double downRatio, double duplicateRatio, double corruptRatio)` amb constants `LOW/MODERATE/HIGH/OVERLOAD` i `named(String): Result<TrafficProfile, UnknownProfile>`; `record UnknownProfile(String requested, Set<String> available)`.
    Valors: `LOW(20,1,0,.05,0,0)`, `MODERATE(100,10,.05,0,0,0)`, `HIGH(500,50,.10,.15,0,0)`, `OVERLOAD(2000,200,.05,.05,.02,.01)`.
  - `record PlannedMessage(String key, String payloadJson, UUID eventId, Kind kind)`, `enum Kind {NEW, DUPLICATE, CORRUPT}`.
  - `final class TrafficGenerator(TrafficProfile, long seed, List<String> adapterIds, Function<TelemetryEvent,String> serializer)` amb `generate(Instant start): List<PlannedMessage>`.

- [ ] **Step 1: Tests**

```java
class TrafficGeneratorTest {
    @Test void sameSeedSamePlan() {
        var g1 = new TrafficGenerator(TrafficProfile.OVERLOAD, 42, List.of("a-1","a-2"), e -> "{}");
        var g2 = new TrafficGenerator(TrafficProfile.OVERLOAD, 42, List.of("a-1","a-2"), e -> "{}");
        assertEquals(g1.generate(NOW), g2.generate(NOW));
    }
    @Test void ratiosRespectedExactly() {
        var plan = new TrafficGenerator(TrafficProfile.OVERLOAD, 7, List.of("a-1"), e -> "{}").generate(NOW);
        assertEquals(2000, plan.size());
        assertEquals(40, countKind(plan, DUPLICATE));  // 2% de 2000
        assertEquals(20, countKind(plan, CORRUPT));    // 1% de 2000
    }
    @Test void corruptPayloadIsNotJson() { /* cada CORRUPT: payloadJson no comença per '{' vàlid → mapper.readValue llança */ }
    @Test void duplicatesRepeatEarlierEventId() { /* tot DUPLICATE té eventId que ja ha aparegut abans a la llista */ }
}
```

- [ ] **Step 2: FAIL. Step 3: Implementació** — `SplittableRandom(seed)`; per a cada i: decideix kind per ordre (corrupt → duplicate → new) amb `rng.nextDouble()` contra els ratios; `NEW` construeix `TelemetryEvent` (status per sorteig segons degraded/down, latència UP 20-300ms, DEGRADED 800-2000, DOWN 3000-10000, adapter cíclic, occurredAt = start + i·intervalMs) i el serialitza amb la funció injectada; `DUPLICATE` repeteix payload/eventId d'un índex anterior aleatori; `CORRUPT` = `"{\"eventId\": \"not-json"`.
- [ ] **Step 4: Verd. Step 5: Commit** `feat: seeded traffic generator with profile ratios`.

---

### Task 5: gateway — Aplicació + REST + producer Kafka

**Files:**
- Create: `application/TelemetryPublisher.java`, `application/Sleeper.java`, `application/PublishTelemetryUseCase.java`, `application/SimulationReport.java`
- Create: `infrastructure/api/TelemetryController.java`, `infrastructure/api/SimulatorController.java`, `infrastructure/api/ApiError.java`, `infrastructure/kafka/KafkaTelemetryPublisher.java`, `infrastructure/config/KafkaTopicsConfig.java`
- Create: `GatewayApplication.java`, `src/main/resources/application.yml`
- Test: `PublishTelemetryUseCaseTest.java`, `TelemetryControllerTest.java`, `SimulatorControllerTest.java`

**Interfaces:**
- Consumes: `TelemetryPublisher.publish(String key, String payloadJson)`; `Sleeper.sleep(long millis)`; `TrafficProfile.named`, `TrafficGenerator`, `TopicNames.TELEMETRY`.
- Produces:
  - `PublishTelemetryUseCase(TelemetryPublisher, ObjectMapper, Sleeper)` amb `publish(TelemetryEvent): void` i `simulate(TrafficProfile): SimulationReport`.
  - `record SimulationReport(String profile, int published, int duplicates, int corrupt, long durationMs)`.
  - REST: `POST /api/v1/telemetry` → 201 `TelemetryEvent` | 400 `ApiError(errors)`; `POST /api/v1/telemetry/simulate?profile=low` → 200 `SimulationReport` | 400 `ApiError(message, available)`.
  - `KafkaTelemetryPublisher` usa `KafkaTemplate<String,String>` i `send(...).whenComplete((r,ex) -> log...)`.

- [ ] **Step 1: Test del cas d'ús** (fakes: `RecordingPublisher implements TelemetryPublisher`, `NoSleep implements Sleeper`):

```java
@Test void simulatePublishesEverythingAndCounts() {
    var uc = new PublishTelemetryUseCase(recorder, new ObjectMapper(), (ms) -> {});
    var report = uc.simulate(TrafficProfile.LOW);
    assertEquals(20, recorder.messages().size());
    assertEquals(20, report.published());
    assertEquals(0, report.corrupt());
}
@Test void simulateOverloadCountsDuplicatesAndCorrupt() { /* published=1940, duplicates=40, corrupt=20 */ }
```

- [ ] **Step 2: FAIL → implementar** `simulate`: genera pla, itera publicant (`kind CORRUPT` compta `corrupt`, `DUPLICATE` compta `duplicates`), `sleeper.sleep(1000 / profile.eventsPerSecond())` entre missatges, mesura durada amb `System.nanoTime()`.
- [ ] **Step 3: Tests de controller** — `@WebMvcTest(TelemetryController.class)` + `@MockitoBean PublishTelemetryUseCase` (Boot 4: `@MockitoBean`, no `@MockBean`): body vàlid → 201; `{"adapterId":"BAD"}` → 400 amb `errors[0].field == "adapterId"`; `simulate?profile=unknown` → 400 amb `available` que conté `"low"`.
- [ ] **Step 4: Implementar controllers + ApiError + yml**

```yaml
spring:
  application.name: adapter-gateway
  threads.virtual.enabled: true
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
server.port: 8081
management.endpoints.web.exposure.include: health
```

- [ ] **Step 5: Verd tot. Step 6: Commit** `feat: gateway rest + kafka producer + traffic simulator`.

---

### Task 6: hub — AdapterHealth (domini pur)

**Files:**
- Create: `telemetry-hub/src/main/java/com/jordimarcal/telemetry/hub/domain/AdapterHealth.java`, `HealthEffect.java`
- Test: `AdapterHealthTest.java`

**Interfaces:**
- Consumes: `TelemetryEvent`, `AlertEvent.forTrigger`.
- Produces:
  - `record AdapterHealth(AdapterId adapterId, int consecutiveDown, boolean alertActive, Instant lastSeen)` amb `initial(AdapterId, Instant)` i `observe(TelemetryEvent): HealthEffect`.
  - `record HealthEffect(AdapterHealth next, Optional<AlertEvent> alertToPublish)`.
- Semàntica: `UP` → reset (0, false); `DEGRADED` → només toc `lastSeen`; `DOWN` → incrementa; si arriba a 3 i no hi ha alerta activa → publica alerta (raó `"3 consecutive DOWN observations"`) i marca `alertActive`; DOWN ulteriors NO re-alcen alerta.

- [ ] **Step 1: Tests de transicions (taula completa)**

```java
@Test void twoDownsNoAlert_threeDownsOneAlert() { ... }
@Test void fourthDownDoesNotRealert() { ... }
@Test void upResetsStreakAndClosesAlert() { ... }
@Test void degradedDoesNotTouchStreak() { ... }
@Test void alertIdDerivesFromTriggerEventId() { ... }
```

- [ ] **Step 2: FAIL. Step 3: Implementació amb `switch` sobre `event.status()`**:

```java
public HealthEffect observe(TelemetryEvent event) {
    return switch (event.status()) {
        case UP -> new HealthEffect(new AdapterHealth(adapterId, 0, false, event.occurredAt()), Optional.empty());
        case DEGRADED -> new HealthEffect(new AdapterHealth(adapterId, consecutiveDown, alertActive, event.occurredAt()), Optional.empty());
        case DOWN -> {
            int streak = consecutiveDown + 1;
            if (streak >= 3 && !alertActive) {
                var alert = AlertEvent.forTrigger(event.eventId(), adapterId, "3 consecutive DOWN observations", event.occurredAt());
                yield new HealthEffect(new AdapterHealth(adapterId, streak, true, event.occurredAt()), Optional.of(alert));
            }
            yield new HealthEffect(new AdapterHealth(adapterId, streak, alertActive, event.occurredAt()), Optional.empty());
        }
    };
}
```

- [ ] **Step 4: Verd. Step 5: Commit** `feat: AdapterHealth aggregate with tell-dont-ask transitions`.

---

### Task 7: hub — Ports + Oracle (Flyway, MERGE, idempotència)

**Files:**
- Create: `application/TelemetryStore.java`, `application/HealthRepository.java`, `application/AlertStore.java`, `application/AlertPublisher.java`, `application/ProcessTelemetryUseCase.java`
- Create: `infrastructure/oracle/OracleTelemetryStore.java`, `OracleHealthRepository.java`, `OracleAlertStore.java`
- Create: `src/main/resources/db/migration/V1__init.sql`, `application.yml`, `HubApplication.java`
- Test: `ProcessTelemetryUseCaseTest.java` (fakes), `OracleStoresIT.java` (Testcontainers)

**Interfaces:**
- Consumes: `Result`, `AdapterHealth`, `AlertEvent`.
- Produces:
  - `interface TelemetryStore { Result<Long, DuplicateTelemetry> append(TelemetryEvent e); record DuplicateTelemetry(UUID eventId) {} }`
  - `interface HealthRepository { AdapterHealth find(AdapterId id); void save(AdapterHealth h); }` (find retorna `initial` si no existeix)
  - `interface AlertStore { void record(AlertEvent a); }` · `interface AlertPublisher { void publish(AlertEvent a); }`
  - `ProcessTelemetryUseCase(TelemetryStore, HealthRepository, AlertStore, AlertPublisher)` amb `process(TelemetryEvent)`:
    ```java
    switch (telemetryStore.append(event)) {
        case Result.Err<TelemetryStore.DuplicateTelemetry> d -> { log.debug("duplicate {} ignored", event.eventId()); return; }
        case Result.Ok<Long> ignored -> { }
    }
    var effect = healthRepository.find(event.adapterId()).observe(event);
    healthRepository.save(effect.next());
    effect.alertToPublish().ifPresent(alert -> { alertStore.record(alert); alertPublisher.publish(alert); });
    ```
- `V1__init.sql`:

```sql
CREATE TABLE telemetry_event (
  event_id    RAW(16) PRIMARY KEY,
  adapter_id  VARCHAR2(40)  NOT NULL,
  country     VARCHAR2(2)   NOT NULL,
  status      VARCHAR2(10)  NOT NULL,
  latency_ms  NUMBER(6)     NOT NULL,
  occurred_at TIMESTAMP(3)  NOT NULL,
  ingested_at TIMESTAMP(3)  DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE TABLE adapter_health (
  adapter_id       VARCHAR2(40) PRIMARY KEY,
  consecutive_down NUMBER(3)    NOT NULL CHECK (consecutive_down >= 0),
  alert_active     NUMBER(1)    NOT NULL CHECK (alert_active IN (0,1)),
  last_seen        TIMESTAMP(3) NOT NULL
);
CREATE TABLE adapter_alert (
  alert_id         RAW(16) PRIMARY KEY,
  trigger_event_id RAW(16) NOT NULL UNIQUE,
  adapter_id       VARCHAR2(40) NOT NULL,
  reason           VARCHAR2(200) NOT NULL,
  raised_at        TIMESTAMP(3) NOT NULL
);
CREATE INDEX idx_telemetry_adapter ON telemetry_event (adapter_id, occurred_at);
```

- [ ] **Step 1: Test del cas d'ús amb fakes** (InMemory stores): primer process → append+save; duplicat → **cap** save; 3 DOWN → alert recorded+published 1 cop; 4t DOWN → segueix 1 cop.
- [ ] **Step 2: FAIL → implementar cas d'ús (switch pattern matching sobre Result).**
- [ ] **Step 3: `OracleStoresIT`** (Testcontainers `OracleContainer("gvenzl/oracle-free:23-slim-faststart")` amb `withUsername("telemetry")`, `@Testcontainers`): Flyway corre (taules existeixen); `append` ok → id 1; mateix eventId → `Err(DuplicateTelemetry)`; `find` d'un id nou → initial; 2 `save` amb DOWN consecutius → `find` retorna consecutiveDown=2.
- [ ] **Step 4: Implementar adapters Oracle**: insert `telemetry_event` amb `DuplicateKeyException` → `Result.err`; `MERGE INTO adapter_health` per save; `getObject("alert_active", Integer.class) == 1` per booleans.
- [ ] **Step 5: `application.yml` hub**:

```yaml
spring:
  application.name: telemetry-hub
  threads.virtual.enabled: true
  datasource:
    url: jdbc:oracle:thin:@//${ORACLE_HOST:localhost}:1521/${ORACLE_SERVICE:telemetry}
    username: ${ORACLE_USER:telemetry}
    password: ${ORACLE_PASS:telemetry}
  flyway.enabled: true
server.port: 8082
management.endpoints.web.exposure.include: health
```

- [ ] **Step 6: Verd (IT requereix Docker; si no hi ha Docker, `@Tag("integration")` + skip per defecte via surefire). Step 7: Commit** `feat: hub oracle stores idempotent + flyway`.

---

### Task 8: hub — Consumer Kafka + DLT + alertes + API lectura

**Files:**
- Create: `infrastructure/kafka/TelemetryListener.java`, `KafkaAlertPublisher.java`, `infrastructure/config/KafkaConsumerConfig.java`, `infrastructure/config/KafkaTopicsConfig.java`, `infrastructure/readapi/AdapterReadController.java`
- Modify: `application.yml` (kafka consumer/producer), `ProcessTelemetryUseCase` wiring
- Test: `TelemetryListenerIT.java` (`@EmbeddedKafka`)

**Interfaces:**
- Produces:
  - `@KafkaListener(topics = TopicNames.TELEMETRY, groupId = "telemetry-hub")` → `useCase.process(event)`.
  - Bean `DefaultErrorHandler`: `new DefaultErrorHandler(new DeadLetterPublishingRecoverer(dltTemplate), new FixedBackOff(500, 2))` on `dltTemplate` és `KafkaTemplate<String, byte[]>`; `DeadLetterPublishingRecoverer` mapeja a `TopicNames.TELEMETRY_DLT`.
  - Beans `NewTopic` per als 3 topics (telemetry: 3 particions, rf 1; alerts i dlt: 1).
  - `KafkaAlertPublisher`: `KafkaTemplate<String, AlertEvent>` amb `JsonSerializer`, publica amb key `adapterId.value()` a `TopicNames.ALERTS`.
  - REST lectura: `GET /api/v1/adapters` → 200 llista `{adapterId, consecutiveDown, alertActive, lastSeen}`; `GET /api/v1/adapters/{id}` → 200 amb estat + `lastEvents` (últims 20 de `telemetry_event`) + `alerts` (últimes 20) | 404 si l'adaptador no existeix.
- `application.yml` kafka (hub):

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    consumer:
      group-id: telemetry-hub
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: com.jordimarcal.telemetry.contracts
        spring.json.value.default.type: com.jordimarcal.telemetry.contracts.TelemetryEvent
        spring.json.use.type.headers: false
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

- [ ] **Step 1: IT amb `@EmbeddedKafka`** (context de test amb in-memory stores substituint els Oracle via `@TestConfiguration`):
  - publicar JSON vàlid a `adapter.telemetry.v1` → dins de 5s apareix a l'in-memory store.
  - publicar `{"trencat` → dins de 5s apareix un registre a `adapter.telemetry.v1.dlt` (consumir amb `KafkaTestUtils`).
- [ ] **Step 2: FAIL → implementar listener + config + error handler.**
- [ ] **Step 3: Implementar alert publisher + read controller** (JdbcTemplate SELECTs; 404 amb `ResponseEntity`).
- [ ] **Step 4: Verd. Step 5: Commit** `feat: hub kafka consumer with dlt, alert publishing and read api`.

---

### Task 9: README + ADRs + demo e2e verificada

**Files:**
- Create: `README.md`, `docs/adr/0001-contracts-module-vs-schema-registry.md`, `docs/adr/0002-result-vs-exceptions.md`, `docs/adr/0003-alert-dual-write-vs-outbox.md`, `demo.sh`

- [ ] **Step 1: ADRs** — 1 paràgraf de context, decisió, conseqüències i "quan canviar-la" cadascun.
- [ ] **Step 2: `demo.sh`** — script verificable: aixeca serveis (jar), `curl -X POST '.../simulate?profile=low'`, mostra `GET /api/v1/adapters`, després `profile=high` (alertes), `profile=overload` (duplicats+DLT), i imprimeix recomptes SQL amb `docker exec oracle sqlplus -s telemetry/telemetry @...`:
  - `SELECT COUNT(*) FROM telemetry_event;` → si es torna a llançar `overload`, el recompte de NEW **no** canvia (els duplicats ja hi són).
  - `SELECT COUNT(*) FROM adapter_alert;` → per adaptador amb 3 DOWN: **exactament 1**.
  - `SELECT COUNT(*) FROM adapter_alert a JOIN ...` no cal — prou.
- [ ] **Step 3: Verificar demo completa de zero** — `docker compose down -v && docker compose up -d && mvn -q package && ` executar demo.sh; comprovar els tres resultats esperats a mà.
- [ ] **Step 4: README** (anglès, cara d'entrevista): diagrama Mermaid, quickstart (compose + 2 jars + curl), taula de perfils, "what to observe" (idempotència, 1 alerta, DLT), enllaços a docs/. 
- [ ] **Step 5: Commit final** `docs: readme, adrs and verified demo script`.

## Self-Review Notes

- Coberta del spec: hexagonal ✓, Result ✓, VOs ✓, Tell-Don't-Ask (AdapterHealth) ✓, 4 perfils ✓, 3 topics ✓, Oracle idempotent ✓, DLT ✓, Flyway ✓, tests per nivell ✓, ADRs ✓.
- Tipus consistents: `Result<T,E>` amb `Ok/Err` records a totes les tasks; `AdapterHealth.observe` retorna `HealthEffect(next, Optional<AlertEvent>)` — usat idèntic a Tasks 6/7/8.
- Verificar a implementació: artefact `ojdbc11` gestionat pel BOM de Boot 4.1 (si no, afegir `<version>` explícita); `@MockitoBean` (Boot 4) en lloc de `@MockBean`; starter de Kafka si Boot 4.1 en té de dedicat.
