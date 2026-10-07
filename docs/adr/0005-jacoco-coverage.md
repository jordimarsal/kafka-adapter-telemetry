# ADR-0005: JaCoCo + Vitest coverage, imported by SonarQube

Date: 2026-10-07 · Status: Accepted

## Context

The homelab SonarQube scan (`~/homelab/sonarqube/analyze.sh`, sonar-scanner CLI)
reported `coverage: 0.0`: SonarQube never computes coverage itself, it imports
reports produced by the test run, and this project generated none. The scanner
was also drowning in noise from `docs/diagrams/*.html` (archify exports with
~14k lines of vendored JS each), which alone accounted for 3.122 of 3.155 issues
and an 87.5% "duplication" density.

Alternatives considered: running the SonarScanner for Maven plugin
(`mvn org.sonarsource.scanner.maven:sonar-maven-plugin:sonar`) — rejected here
because the homelab workflow is scanner-CLI based and shared across projects;
or feeding surefire's execution counts without an agent — rejected because
line-level coverage needs instrumentation.

## Decision

Wire `org.jacoco:jacoco-maven-plugin` 0.8.15 (first-class Java 25 support) into
the parent pom for every module:

- `prepare-agent` at the default phase instruments the test JVM via `argLine`
  (surefire picks it up; no module overrides `argLine`).
- `report` explicitly bound to the `test` phase, so a plain `mvn -q package`
  leaves `target/site/jacoco/jacoco.xml` in every module — the artifact
  SonarQube imports.

The repo-root `sonar-project.properties` tells the scanner where the reports
and the module bytecode live, marks `src/test/java` as tests, and excludes the
generated docs HTML from analysis. It is a build-scoped tool: no runtime
dependency, `contracts` stays dependency-free, no jar grows.

The dashboard needed the same treatment: SonarQube counts its executable lines
in the coverage denominator, so without a report the whole TS side reads as
0%. `@vitest/coverage-v8` (devDependency, same major as vitest) is wired into
the existing `npm --prefix dashboard test` (`vitest run --coverage`) with an
`lcov` reporter writing `dashboard/coverage/lcov.info`, imported via
`sonar.javascript.lcov.reportPaths`.

## Consequences

- SonarQube shows real per-module coverage after any `mvn -q package`, and
  dashboard coverage after `npm --prefix dashboard test` (always emits
  `dashboard/coverage/lcov.info`).
- JaCoCo must track the class-file version of new JDKs; bump `jacoco.version`
  in the parent pom when upgrading Java (0.8.15 ↔ Java 25).
- `@vitest/coverage-v8` must track the vitest major; both are dev/build-scoped.
- The `docs/diagrams/**` and `docs/demo.html` exclusions mean those files are
  invisible to SonarQube — acceptable because they are generated artifacts of
  the documentation, not maintained code.
