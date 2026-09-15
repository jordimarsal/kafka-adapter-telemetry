# 04 · kafka-adapter-telemetry — Pla d'alt nivell

> **Pitch:** una pipeline de telemetria d'adaptadors d'API inspirada en Open Gateway
> (Telefónica): adaptadors de països diferents reporten estat i latència via Kafka,
> i un hub els agrega, persisteix a Oracle de manera **idempotent** i aixeca **alertes**
> quan un adaptador acumula caigudes consecutives. Dos microserveis petits en
> **Java 25 + Spring Boot 4.1**, hexagonals, amb Kafka i Oracle reials a Docker.

---

## 1. Per què aquest projecte (narrativa d'entrevista)

| Pregunta d'entrevista | Resposta que aquest projecte demostra amb codi |
|---|---|
| "Has fet event-driven de veritat?" | Producer/consumer Spring Kafka, claus de partició per ordenació per adaptador, DLT amb retries i backoff |
| "Com evites duplicats?" | Idempotència amb PK a Oracle (`EVENT_ID`) i alertes amb identificador determinista |
| "Com estructures un microservei?" | Hexagonal per servei: domini pur sense Spring, ports propis, adapters a les vores |
| "Paradigmes de Java modern?" | Records com a VOs rics, `sealed interface Result`, pattern matching, virtual threads |
| "I si arriba trànsit massa alt o dades brutes?" | Perfils de simulació (fins a `overload` amb duplicats i JSON corrupte) que deixen veure lag, idempotència i DLT en viu |

El domini no és arbitrari: és una versió miniatura del sistema d'operació de ~90
adaptadors en 4 països que vaig automatitzar a Open Gateway — puc parlar-ne amb
experiència de primera mà.

---

## 2. Serveis

| Servei | Responsabilitat | Bordes |
|---|---|---|
| **adapter-gateway** (port 8081) | Rep telemetria via REST i publica a Kafka; genera trànsit de demostració segons perfils | HTTP in, Kafka out |
| **telemetry-hub** (port 8082) | Consumeix telemetria, persisteix a Oracle de manera idempotent, manté l'estat d'entitat per adaptador, avalua la regla d'alerta, publica alertes i exposa l'API de lectura | Kafka in, Oracle, Kafka out, HTTP read |

Cap servei comparteix base de dades amb l'altre. El contracte compartit és el mòdul
`contracts` (el "llenguatge" del sistema: esdeveniments, VOs i `Result`).

---

## 3. Principis aplicats (visibles al codi)

1. **Hexagonal per servei**: `domain` + `application` (purs, sense Spring) i
   `infrastructure` (REST, Kafka, Oracle, config). Els ports defineixen la frontera;
   els tests d'aplicació usen fakes, mai mocks de xarxa.
2. **Patró Result**: els fluxos esperables (validació, perfil desconegut, esdeveniment
   duplicat) retornen `Result<T, E>` (`sealed` + `fold`); les excepcions es reserven
   per allò excepcional (broker caigut, BD inaccessible).
3. **Value Objects rics**: `AdapterId`, `Country`, `LatencyMs`, `Status` — records amb
   invariants al constructor compacte i **comportament** (`isSlow()`, `isHealthy()`),
   mai dades nues.
4. **Tell, Don't Ask**: l'estat per adaptador viu dins l'agregat `AdapterHealth`; el
   cas d'ús li **explica** què ha passat (`observe(event)`) i l'agregat respon amb
   l'efecte (`HealthEffect(next, alertToPublish)`). Ningú llegueix camps per decidir fora.
5. **Idempotència per construcció**: PK sobre `EVENT_ID` (duplicat → ignorat) i
   `alert_id` determinista derivat de l'event trigger (replay → cap alerta doble).
6. **I/O als bordes**: validació, regles d'alerta i generació de trànsit són funcions
   pures i deterministes (seed); només publishers/listeners/repositories toquen xarxa.

---

## 4. Topics de Kafka

| Topic | Key | Contingut | Decisió de disseny |
|---|---|---|---|
| `adapter.telemetry.v1` | `adapterId` | `TelemetryEvent` JSON | Key per adaptador → ordre per entitat; nom versionat per evolucionar l'esquema |
| `adapter.alerts.v1` | `adapterId` | `AlertEvent` JSON | Sortida del hub; demo de pipeline multi-topic |
| `adapter.telemetry.v1.dlt` | — | Missatge original + capçaleres d'error | Poison pills (JSON corrupte) i errors de processament després de 2 retries amb backoff |

Per què mòdul compartit i no Schema Registry: YAGNI per a 2 serveis; l'ADR-001 ho
documenta com a pas natural de creixement.

---

## 5. Oracle (Free 23ai en Docker)

Imatge `gvenzl/oracle-free:23-slim-faststart`; migracions amb **Flyway**.

| Taula | Rol | Idempotència |
|---|---|---|
| `TELEMETRY_EVENT` | log crú de telemetria | `PK (EVENT_ID)` → duplicat rebutjat per la BD |
| `ADAPTER_HEALTH` | estat corrent per adaptador: `consecutive_down`, `alert_active`, `last_seen` | `MERGE` upsert |
| `ADAPTER_ALERT` | històric d'alertes | `PK (ALERT_ID)` + `UNIQUE (TRIGGER_EVENT_ID)` — determinista en replay |

**Regla d'alerta** (funció pura dins `AdapterHealth`): 3r `DOWN` consecutiu → una
alerta, **només en la transició**; `UP` reseteja el comptador i tanca l'alerta;
`DEGRADED` no altera la ratxa de `DOWN`.

Trade-off documentat (ADR-003): publicar alerta a Kafka i persistir-la és una
dual-write sense outbox; assumida per ser una demo, amb l'outbox transaccional
com a evolució natural.

---

## 6. Perfils de trànsit (la demo)

`POST /api/v1/telemetry/simulate?profile=…` — generador **determinista (seed)**,
comptat limitat, execució en virtual threads, resposta amb resum:

| Perfil | Events | Ritme | Ratios | Què demostra |
|---|---|---|---|---|
| `low` | 20 | 1/s | 5% DOWN | flux net de cap a cap |
| `moderate` | 100 | 10/s | 5% DEGRADED | agregació per key, MERGE upsert |
| `high` | 500 | 50/s | 15% DOWN, 10% DEGRADED | **alertes**: 3 DOWN consecutius → 1 alerta per adaptador |
| `overload` | 2.000 | 200/s | 5% DOWN, 2% **duplicats**, 1% **JSON corrupte** | idempotència en viu + **DLT** + consumer lag |

Cada perfil és un Value Object (`TrafficProfile`) amb factory que retorna
`Result` — perfil desconegut → 400 amb la llista de perfils vàlids.

---

## 7. Gestió d'errors

- **Consumer** (`telemetry-hub`): `DefaultErrorHandler` amb backoff fix (2 retries)
  → `DeadLetterPublishingRecoverer` cap a la DLT; `ErrorHandlingDeserializer` fa que
  el JSON corrupte també acabi a la DLT en lloc de bloquejar la partició.
- **Producer** (`adapter-gateway`): publica JSON cru com a `String` (el payload
  corrupte ha d'arribar a Kafka tal qual per demostrar la DLT); validació prèvia
  retorna `Result`, el controller tradueix `Err` → `400`.
- **Idempotència**: consumir dos cops el mateix event no duplica res — la BD és
  l'àrbitre, no la memòria del consumer.

---

## 8. Testing (TDD)

| Nivell | Què | Eina |
|---|---|---|
| Unitari (purs) | `Result`, VOs, `AdapterHealth` (taula completa de transicions), `TrafficGenerator` (determinisme amb seed) | JUnit 5, sense Spring |
| Aplicació | casos d'ús amb fakes in-memory dels ports | JUnit 5 |
| Integració Kafka | roundtrip producer→consumer amb JSON i DLT | `@EmbeddedKafka` |
| Integració Oracle | Flyway, MERGE, idempotència | Testcontainers (`oracle-free`) |

---

## 9. Stack i versions

| Peça | Versió | Nota |
|---|---|---|
| Java | 25 (Corretto) | records, sealed, pattern matching, virtual threads |
| Spring Boot | 4.1.1 | suport oficial Java 17–26; línia 3.5 EOL juny 2026 |
| Spring Kafka | (gestionat per Boot) | broker Kafka 4.x KRaft single-node (`apache/kafka`) |
| Oracle | `gvenzl/oracle-free:23-slim-faststart` | usuari d'app `telemetry` |
| Flyway | + `flyway-database-oracle` | obligatori des de Flyway 10 (mòduls per BD) |
| Testcontainers | `oracle-free` module | única IT que demana Docker |
| Maven | multi-mòdul | parent + `contracts` + 2 serveis |

---

## 10. Fases

| # | Fase | Fet quan |
|---|---|---|
| 0 | Scaffold Maven + Compose | `docker compose up` verd; `mvn validate` verd |
| 1 | `contracts`: Result, VOs, esdeveniments | tests de domini verds |
| 2 | Gateway: REST + producer + simulador de perfils | roundtrip amb consola; `simulate` publica el que toca |
| 3 | Hub: consumer + Oracle idempotent | duplicats no dupliquen (IT Testcontainers) |
| 4 | Hub: alertes + DLT + API lectura | 3 DOWN → 1 alerta; corruptes a DLT; lectura funciona |
| 5 | README + ADRs + demo e2e | demo documentada i verificada de zero |

Pla d'implementació detallat (tasques petites, TDD): `docs/plans/2026-09-14-kafka-adapter-telemetry.md`.

> **Estat (2026-09-14):** Fases 0–4 fetes i revisades (últim commit `7f767ca`; detall i deltas a l'estat del pla d'implementació). Pendent: Fase 5 — README, ADRs i demo e2e verificada. Recordar afegir `spring-boot-starter-kafka` al gateway abans de la demo.

---

## 11. ADRs previstos

1. **ADR-001**: mòdul `contracts` compartit vs Schema Registry (elecció: mòdul; registre com a evolució).
2. **ADR-002**: `Result` propi vs llibreries (Vavr/Result4j) vs excepcions (elecció: sealed propi de ~20 línies).
3. **ADR-003**: dual-write d'alertes vs outbox transaccional (elecció: assumir-ho documentat).

## 12. Fora d'abast (MVP)

Schema Registry, outbox transaccional, mètriques Prometheus/Grafana, autenticació,
múltiples instàncies del hub, Kubernetes. Tot queda apuntat com a conversa
d'evolució, no com a codi.
