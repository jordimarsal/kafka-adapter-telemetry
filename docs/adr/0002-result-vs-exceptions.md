# ADR-0002: Own sealed Result type vs libraries or exceptions

Date: 2026-09-15 · Status: Accepted

## Context

Expected failures are part of this pipeline's normal flow: invalid adapter ids,
unknown simulation profiles, duplicate events. Java's default tool for failure is
the exception, which hides "this can happen" in unchecked control flow, makes
success paths invisible in signatures, and is easy to forget to catch. The
alternatives are pulling in a library (Vavr, functional-java) or JDK types such as
`Optional` (which cannot carry the error) — either way, a dependency or a poor fit
for something the domain models in three lines.

## Decision

Implement a tiny sealed `Result<T, E>` in the `contracts` module: an interface
permits exactly `Ok(value)` and `Err(error)` records, with `isOk`, `orElseThrow`,
`error` and a `fold(onOk, onErr)` function. Every expected-outcome API returns it —
`AdapterId.parse`, `TrafficProfile.named`, telemetry ingestion — and controllers/
consumers translate `Err` into HTTP 400 or DLT routing at the edge. Exceptions stay
reserved for the exceptional: broker down, database unreachable.

## Consequences

- Call sites are forced to handle both branches (`fold` is exhaustive over the
  sealed hierarchy); success and failure are visible in every signature.
- No dependency: ~70 lines, fully covered by unit tests, zero learning curve for
  readers who know the pattern.
- It is deliberately minimal — no `map`/`flatMap` chaining, no typed error
  hierarchies. If call sites start nesting folds, that friction is the signal that
  richer composition is needed. Checked-exception-style boilerplate at the edges is
  accepted as the price of explicitness.

## When to revisit

When error flows need composition (chained fallible steps) or when the codebase
already carries Vavr (or a successor) for other reasons — adopt library types then
rather than growing a homegrown monad.
