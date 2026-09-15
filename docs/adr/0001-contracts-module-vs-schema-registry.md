# ADR-0001: Shared contracts module vs Schema Registry

Date: 2026-09-15 · Status: Accepted

## Context

The gateway and the hub exchange `TelemetryEvent` and `AlertEvent` JSON over Kafka.
Both services need the same definition of these messages, and the hub deserializes
with a `JsonDeserializer` pinned to `com.jordimarcal.telemetry.contracts.TelemetryEvent`.
The standard Kafka ecosystem answer is Confluent Schema Registry with Avro/Protobuf:
schemas live in a registry, producers/consumers resolve them at runtime, and
incompatible changes are rejected by the registry's compatibility rules. That adds a
piece of infrastructure (a registry service, serializers per language, CI checks
against it) that a two-service demo does not need, and it only covers Kafka — the
read API and any future consumer would still want plain Java types.

## Decision

Keep the event types and value objects in a dedicated `contracts` Maven module with
zero external dependencies, shared as an ordinary jar by both services. Payloads are
plain JSON; consumers deserialize by explicit configured type (`spring.json.value.default.type`,
`spring.json.use.type.headers: false`) and trust only the contracts package. Schema
evolution is versioned at the topic level (`adapter.telemetry.v1`): a breaking change
publishes to `v2` instead of mutating the existing topic.

## Consequences

- Both sides compile against the same types; a breaking change fails the build, not
  production. No extra runtime infrastructure.
- There is no registry-enforced compatibility gate: discipline (and the topic-version
  escape hatch) replaces it. Interoperability with non-Java consumers means
  hand-maintaining their models.
- The hub ignores type headers, so a producer that adds fields does not break it;
  removal/renaming of fields it reads is only caught by tests.

## When to revisit

When a third service (especially outside the JVM) consumes the topics, when schemas
evolve frequently, or when an organization-wide data-governance requirement appears —
that is the point where Confluent Schema Registry (or JSON Schema published next to
the contracts) earns its keep.
