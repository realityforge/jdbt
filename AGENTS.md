# AGENTS

## Purpose

This repository hosts the Java port of Ruby `dbt` with parity-first behavior and Java-native conventions.

## Architecture Overview

The system is implemented as a single Gradle module with this target package structure:

- `org.realityforge.jdbt.cli`: command line parsing and command dispatch.
- `org.realityforge.jdbt.config`: `jdbt.yml` parsing, validation, and immutable models.
- `org.realityforge.jdbt.repository`: `repository.yml` parsing and merge semantics.
- `org.realityforge.jdbt.files`: filesystem and artifact resolution, index ordering, duplicate checks.
- `org.realityforge.jdbt.runtime`: command orchestration and behavior parity logic.
- `org.realityforge.jdbt.db`: SQL driver abstraction and engine implementations.
- `org.realityforge.jdbt.packaging`: deterministic ZIP packaging and artifact loading.

## Source of Truth

- Ruby implementation: `vendor/dbt/lib/**`.
- Ruby behavior baseline: `vendor/dbt/test/**`.
- Project requirements and execution state: `plans/jdbt/**`.

## Non-Negotiable Workflow

1. Keep `plans/jdbt/**` and this file aligned with code changes.
2. Use minimal diffs that preserve parity unless a deliberate divergence is documented.
3. Run targeted checks while iterating.
4. Run full required gates before returning control to the user.
5. Update task status and evidence in `plans/jdbt/20-task-board.yaml`.
6. Commit each completed step only after full gates pass.

## Required Full Gates

Run this command before handoff and before step-completion commits:

`./gradlew clean spotlessCheck check fatJar`

## Commit Rules

- One commit per completed step.
- Commit message explains behavior or process impact.
- Do not batch unrelated steps into one commit.
- Do not skip required gates.

## Coding Standards

- Java 17 baseline.
- Nullability is strict from day one with JSpecify and NullAway.
- Error Prone diagnostics are build-blocking.
- Formatting is enforced via palantir-java-format.
- Prefer deterministic behavior (ordering, timestamps, output stability).

## Testing Standards

- Ruby tests define minimum parity behavior.
- Add Java tests beyond parity for failure paths and diagnostics.
- Coverage gates are mandatory (line >= 85%, branch >= 75%).
- Default test runs must not require a live database.
