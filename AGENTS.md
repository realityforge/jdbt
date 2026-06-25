## Purpose

This repository hosts the Java port of Ruby `dbt` with parity-first behavior and Java-native conventions.

## Architecture Overview

The system is implemented as a Bazel Java project with package-owned `BUILD.bazel` files and this target package structure:

- `org.realityforge.jdbt.cli`: command line parsing and command dispatch.
- `org.realityforge.jdbt.config`: `jdbt.yml` parsing, validation, and immutable models.
- `org.realityforge.jdbt.repository`: `repository.yml` parsing and merge semantics.
- `org.realityforge.jdbt.files`: filesystem and artifact resolution, index ordering, duplicate checks.
- `org.realityforge.jdbt.runtime`: command orchestration and behavior parity logic.
- `org.realityforge.jdbt.db`: SQL driver abstraction and engine implementations.
- `org.realityforge.jdbt.packaging`: deterministic ZIP packaging and artifact loading.

## Required Full Gates

Run this command before handoff and before step-completion commits:

`tools/check.sh`

## Commit Rules

- Use $commit skill for commits.
- Do not batch unrelated steps into one commit.
- Do not skip required gates prio.

## Coding Standards

- Java 17 baseline.
- Nullability is strict with JSpecify and NullAway.
- Error Prone diagnostics are build-blocking.
- Formatting is enforced via palantir-java-format.
- Bazel targets must list source files explicitly; do not use `glob()`.
- Each source directory owns its own `BUILD.bazel`; targets must not list source files from child, sibling, or parent directories.
- Prefer deterministic behavior (ordering, timestamps, output stability).
- Default test runs must not require an external database, an in-memory database is ok.
