# AGENTS.md

Guidance for AI agents (and humans) working in this repository. Read this before
writing any code: the rules below are derived from the actual codebase and its
ADRs — follow them, don't reinvent style per file.

## What this project is

Kafka adapter-telemetry pipeline (demo / interview-grade, but built properly):
an **adapter-gateway** service that ingests telemetry over REST and publishes to
Kafka with a deterministic traffic simulator, and a **telemetry-hub** service
that consumes, persists idempotently to Oracle, tracks per-adapter health and
raises alerts. Java 25, Spring Boot 4.1, hexagonal architecture per service,
TDD throughout.

## Modules (Maven multi-module)

| Module           | Purpose                                                                 | May depend on         |
|------------------|-------------------------------------------------------------------------|-----------------------|
| `contracts`      | Zero-dependency shared language: `TelemetryEvent`, `AlertEvent`, value objects (`AdapterId`, `LatencyMs`, `Country`), `Status`, `ValidationError`, sealed `Result`, `TopicNames` | nothing except `jackson-annotations` (`provided`) |
| `adapter-gateway`| REST ingest + Kafka producer + seeded simulator (`:8081`)               | `contracts`           |
| `telemetry-hub`  | Kafka consumer, Oracle persistence (Flyway), health aggregate, alerts, read API + SSE metrics tap, serves the dashboard build (`:8082`) | `contracts`           |

Not a Maven module: `dashboard/` — Vite + React + TypeScript mission-control UI,
served by the hub as static files (`file:dashboard/dist/`, so run the hub from the
repo root). Its own commands, its own lockfile, outside the Maven reactor.

The dependency rule: **contracts depends on nothing; services depend only on
contracts and their own layers; layers point inwards.** Never add a dependency
between the two services, never add Spring (or any framework) to `contracts`
beyond the existing Jackson annotations, and never add a new third-party
dependency without recording why in `docs/adr/`.

## Package layout (hexagonal, per service)

```
<service>/src/main/java/com/jordimarcal/telemetry/<service>/
├── domain/          # pure: aggregates, entities, domain decisions (no framework imports)
├── application/     # use cases + ports (interfaces the outside world must implement)
└── infrastructure/  # Spring lives ONLY here
    ├── api/         # REST in (gateway)
    ├── readapi/     # REST out (hub read side)
    ├── kafka/       # listeners, producers, DLT plumbing
    ├── oracle/      # JDBC stores implementing application ports
    └── config/      # composition roots: UseCaseConfig, Kafka configs, @SpringBootApplication
```

Hard rules:

- `domain` and `application` classes are **plain Java**: no `@Service`,
  no `@Component`, no `@Autowired`, no Spring imports. They are wired by
  explicit `@Configuration` composition roots in `infrastructure/config`
  (see `UseCaseConfig`).
- Ports are interfaces in `application` (`TelemetryStore`, `AlertPublisher`,
  `HealthRepository`, `Sleeper`, …); implementations live in `infrastructure`
  and never leak back (no `JdbcTemplate`, `KafkaTemplate` or HTTP types
  inside `application`/`domain`).
- REST controllers are thin: translate DTO ⇄ contract types, call the use case,
  map the `Result` to an HTTP status via `fold`, and get out.

## Value objects are rich

Every domain concept with rules is a `record` that **validates itself in its
compact constructor and fails fast** (e.g. `AdapterId`:

- `Objects.requireNonNull` for required parts, `IllegalArgumentException`
  with a message that includes the offending value.
- Pattern/shape constants as `private static final Pattern`.
- `@JsonValue` on the component and `@JsonCreator(mode = DELEGATING)` on the
  constructor so Jackson serializes the VO as its primitive.
- A boundary-friendly factory `static Result<Vo, ValidationError> parse(String raw)`
  that catches the constructor's exceptions and returns typed errors.
- Override `toString()` to return the wrapped value.

Never pass raw `String`/`int` across layers when a VO exists. Never add setters,
never let a VO reach an invalid state. Use cases decide **nothing** by
inspecting raw fields — they hand VOs/aggregates to the code that owns the rule.

## Result pattern (expected failures ≠ exceptions)

Use the sealed `contracts.Result<T, E>` (`Result.ok` / `Result.err`) for
**expected** outcomes: validation, duplicates, unknown profiles. Exceptions are
reserved for the **exceptional**: broker down, DB unreachable, programming bugs.
This was decided in ADR-0002 — don't relitigate it per PR.

Conventions:

- Consume a `Result` with `fold(...)`, or with pattern matching and record
  deconstruction:
  ```java
  switch (telemetryStore.append(event)) {
      case Result.Err(var duplicate) -> { /* log and stop */ }
      case Result.Ok(var ignored) -> { }
  }
  ```
- Use `_` (unnamed patterns/variables, Java 25) when the value is irrelevant.
- At the HTTP edge, `fold` maps ok → 2xx payload and err → `400` + `ApiError`
  listing `ValidationError`s (see `TelemetryController`).
- Never throw for validation; never swallow a `Result.Err` silently — at minimum
  log at the appropriate level (`debug` for duplicates, `warn` for alerts).

## Tell, Don't Ask

Domain aggregates **decide**; the application layer **executes** what they
decide. Model to copy: `AdapterHealth.observe(TelemetryEvent)` returns a
`HealthEffect` (the next immutable state + optional `AlertEvent`). The use case
(`ProcessTelemetryUseCase`) never inspects `status`/streak fields itself — it
tells the aggregate what happened, persists `effect.next()`, and publishes
`effect.alertToPublish()` if present.

- Aggregates are immutable records; state changes are new instances.
- Decision constants (thresholds, reasons) live in the domain, not in use cases.
- If you find a use case full of `if (x.getStatus() == …)`, stop: that logic
  belongs in a domain object returning an effect.

## Idempotency & data rules

- The database is the referee: `TELEMETRY_EVENT` dedupes by primary key; the
  consumer treats duplicates as a normal `Result.Err` (debug log, no error).
- Alert ids are **derived** from the triggering event id
  (`AlertEvent.forTrigger`) so replays never create a second alert. Preserve this.
- Migrations are Flyway scripts in `telemetry-hub/src/main/resources/db/migration`
  (`V*__*.sql`); never edit an applied migration, add a new one.
- Kafka: payloads are JSON strings on `adapter.telemetry.v1` / `adapter.alerts.v1`
  (names in `contracts.TopicNames`), keyed by `adapterId`; hub consumer uses
  `ErrorHandlingDeserializer` + `DefaultErrorHandler` with retries → DLT
  `adapter.telemetry.v1.dlt`. Don't change topics/partitioning casually.

## TDD: how we test

Red → green → refactor. No production code without a failing test first. Test
names are descriptive camelCase sentences, no `@DisplayName` needed
(e.g. `twoDownsRaiseNoAlert_thirdDownRaisesExactlyOne`). Levels:

| Level              | What                                                           | Tooling |
|--------------------|----------------------------------------------------------------|---------|
| Unit (pure)        | `Result`, VOs, `AdapterHealth`, seeded `TrafficGenerator`       | JUnit 5, **no Spring**, plain assertions |
| Application        | Use cases against **in-memory fakes of the ports** (hand-written, no Mockito) | JUnit 5 |
| Web slice          | Controllers, `Result` → status/error mapping                   | `@WebMvcTest` + `MockMvc` + `@MockitoBean` |
| Kafka integration  | Producer→consumer roundtrip, DLT routing                       | `@EmbeddedKafka` (runs in `mvn package`, named `*IT` but included by surefire) |
| Oracle integration | Idempotent stores against real Oracle                          | Testcontainers (`OracleStoresIT`, Docker required, **excluded** from the default build) |

Notes:

- Fakes over mocks for ports: a fake implements the interface with a `Map`/list.
  Mockito only at the web-slice edge (`@MockitoBean` for use cases).
- Determinism matters: the simulator is seeded so `overload` replays duplicate
  ids on purpose; tests rely on that — don't replace seeds with randomness.
- Time is passed in (`Instant now` parameters, `Sleeper` port), never
  `Instant.now()` buried in domain/application logic.

## Commands

```bash
# Full build: unit tests + EmbeddedKafka integration tests (no Docker needed)
mvn -q package

# One module / one test
mvn -q -pl telemetry-hub -am package
mvn -q -pl adapter-gateway test -Dtest=TrafficGeneratorTest

# Oracle Testcontainers IT (needs `docker compose up -d --wait` first)
mvn -q -pl telemetry-hub test -Dtest='OracleStoresIT'

# Infrastructure (Kafka + Oracle Free) and the verified end-to-end demo
docker compose up -d --wait
./demo.sh

# Dashboard (not in the Maven reactor; run from the repo root)
npm --prefix dashboard install        # once
npm --prefix dashboard run build      # dashboard/dist/, served by the hub at /
npm --prefix dashboard test           # Vitest + Testing Library

# Run the services
java -jar adapter-gateway/target/adapter-gateway-0.1.0-SNAPSHOT.jar    # :8081
java -jar telemetry-hub/target/telemetry-hub-0.1.0-SNAPSHOT.jar       # :8082
```

Gateway `:8081` (`POST /api/v1/telemetry`, `POST /api/v1/telemetry/simulate?profile=…`),
hub `:8082` (`GET /api/v1/adapters`, `GET /api/v1/adapters/{id}`, `GET /api/v1/stream`
SSE, `GET /api/v1/metrics/snapshot`, `POST /api/v1/demo/reset` (demo-only:
truncates the 3 Oracle tables + zeroes in-memory counters, never rewinds
`seq`), dashboard at `/`). Dashboard data is same-origin
with the hub API — it must only call hub URLs relative to its own origin (`/api/…`).
The gateway is reached only through the gateway base URL: `VITE_GATEWAY_URL` at
build time (default `http://localhost:8081`), and the gateway's CORS allowlists
exactly `http://localhost:8082` for that control path.

## Java 25 style

- Records for all data; sealed interfaces for closed hierarchies (`Result`,
  events); pattern matching in `switch` with record deconstruction.
- `_` for unused bindings/variables.
- Text blocks for SQL and JSON fixtures; `static final` constants in caps.
- Constructor injection only (often explicit constructor in composition roots);
  no field injection, no Lombok, no `@Autowired` unless unavoidable in tests.
- `var` for locals when the type is obvious from the right-hand side.
- Javadoc on public domain/application types explaining **intent and invariants**
  (see `AdapterId`, `AdapterHealth`); no comment noise inside methods.
- Timestamps are `Instant` (UTC); ids are `UUID` (stored as RAW(16) in Oracle —
  pack/unpack via `ByteBuffer`, as in `AdapterReadController`).

## Code quality: keep SonarQube clean (proactive)

This project is scanned by SonarQube (rules surface in the IDE as `java:S<nnn>` /
`docker:S<nnn>`). `mvn -q package` runs the build and tests but **not** Sonar, so
don't wait for a CI gate — follow these conventions as you write code so nothing
needs a cleanup pass later. Language level is **Java 25** (unnamed variables `_`,
`Math.clamp`, records, `switch` expressions all available).

### Logging

- **Never** `System.out`/`System.err`. Use the SLF4J logger (`LoggerFactory`,
  Logback binding via `spring-boot-starter-logging`), also inside
  `application`/`domain` — they may import SLF4J, nothing else (see
  `ProcessTelemetryUseCase`).
- Pass the throwable as the last arg: `log.error("msg", e)` — never
  `log.error(e.getMessage())`.
- Defer expensive args: `log.debug("{}", () -> expensive())` (lambda /
  `Supplier`), don't precompute the string.
- Levels are already meaningful: `debug` for duplicates, `info` for processed
  events, `warn` for alerts; reserve `ERROR` for real failures.

### Resources & exceptions

- Wrap every `AutoCloseable` (stores, pools, short-lived containers) in
  **try-with-resources**. Test-scoped containers use `@Testcontainers` +
  `@Container` instead (see `OracleStoresIT`, Testcontainers 2.x BOM) — that is
  the accepted exception; document any further deviation.
- Unused caught exception params → `_` (Java 25): `catch (RuntimeException _)`.
- Empty override methods need a one-line comment explaining why they're empty
  (Sonar `S1186`).

### APIs & data types

- No wildcard return types (Sonar `S1452`): return
  `ResponseEntity<TelemetryEvent>`, not `ResponseEntity<?>` —
  `TelemetryController.receive` and `SimulatorController.simulate` still carry
  it; fix it when you touch them.
- Prefer `record` for immutable value objects (as everywhere in `contracts`).
- A constructor with >7 params → group them into a parameter record or a factory
  (see `TelemetryEvent.of`); no Lombok `@Builder`.
- Clamp with `Math.clamp(value, min, max)`, not `Math.max(min, Math.min(value, max))`.

### Control flow & complexity

- Avoid `break`/`continue`/named labels in loops (Sonar `S135`); extract a
  helper predicate and `return` early.
- Keep methods under the cognitive-complexity budget (Sonar `S3776`): extract
  helpers (see `AdapterHealth.observeDown`) instead of nesting loops/branches.
- Remove unused locals/fields (Sonar `S1481`); a local created only to satisfy
  a signature must be dropped.

### Tests

- Where AssertJ is on the classpath (service modules via
  `spring-boot-starter-test`; `contracts` stays on plain JUnit assertions),
  use the fluent form, not `.size()`/`.keySet()`/`.hashCode()` intermediates:
  `hasSize(n)`, `containsKeys(...)`, `containsEntry(k, v)`,
  `hasSameHashCodeAs(other)`, `hasToString(...)`. Chain
  `assertThat(a).isEqualTo(b).hasSameHashCodeAs(b)` (Sonar `S5838`/`S5853`).
- Hoist constant / expensive values (`"x".repeat(129)`, `URI.create(...)`,
  `Duration.of(...)`) out of lambdas (Sonar `S5778`).
- No `Thread.sleep` in tests (Sonar `S2925`) and no
  `try { … } catch (…) { fail(…); }` (Sonar `S8714`): use **Awaitility**
  (`await().atMost(TIMEOUT).untilAsserted(...)`), already the pattern in
  `TelemetryListenerIT`. The only production sleep lives behind the `Sleeper`
  port (`GatewayApplication.pace`) — keep it that way.
- Repeated reject/accept cases → `@ParameterizedTest` + `@ValueSource`
  (Sonar `S5976`).

### Docker

- Pin `docker-compose.yml` base images by **digest**, not floating tags:
  `apache/kafka:4.1.2@sha256:…`, `gvenzl/oracle-free:23-slim-faststart@sha256:…`
  (Sonar `S8431`). The tag may stay as documentation but the digest is what's
  enforced — `console:latest` is exactly what to avoid.

## Documentation

- Big decisions get an ADR in `docs/adr/NNNN-*.md` (new dependency, alternative
  trade-offs like Result vs exceptions, outbox vs dual-write).
- `docs/plan.md` is the spec/plan of record; `docs/plans/` holds dated plans.
- Update the README (architecture diagram, testing table) when you change
  endpoints, topics, tables or test strategy.

## If you get stuck

- Re-read the relevant section of `docs/` (`docs/plan.md`, the ADRs in
  `docs/adr/`, the dated plans in `docs/plans/`).
- If a tool doesn't behave as expected, **do not invent a workaround**:
  document the block in a dated plan under `docs/plans/` (create one if none
  exists) and stop the session.

## When reviewing your own work (definition of done)

- [ ] New logic was driven by a failing test at the right level.
- [ ] `domain`/`application` still import zero framework classes.
- [ ] Expected failures are `Result`, not exceptions; no silent `Err`.
- [ ] Decisions live in domain aggregates (Tell, Don't Ask), not in use cases.
- [ ] Every new domain concept with rules is a rich, self-validating VO.
- [ ] `mvn -q package` passes; Oracle ITs still pass if you touched persistence.
- [ ] No Sonar red flags introduced (System.out, wildcard returns, `Thread.sleep`
  in tests, unused locals, floating Docker tags…).
- [ ] No new dependency without an ADR; contracts stayed dependency-free.
