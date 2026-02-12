# jdbt Requirements

## Mission

Port Ruby `dbt` in `vendor/dbt` to Java while preserving behavior-critical semantics and adopting Java ecosystem conventions.

## Locked Decisions

- Java 17 baseline.
- Single-module Gradle project.
- Maven coordinates: `org.realityforge:jdbt`.
- Java package root: `org.realityforge.jdbt`.
- CLI framework: picocli.
- YAML parser: SnakeYAML Engine.
- Nullability: JSpecify annotations + NullAway, strict from day one.
- Static analysis: Error Prone + NullAway.
- Formatter: palantir-java-format.
- CI platform: GitHub Actions.

## Configuration Model

- Project config file is fixed and non-configurable: `jdbt.yml`.
- `jdbt.yml` defines a single implicit database at the top level; top-level `defaults` and `databases` are not supported.
- `searchDirs` is not supported in `jdbt.yml`.
- The only search directory is the directory containing `jdbt.yml`.
- `repository.yml` remains first-class and layout-compatible.
- Runtime instance settings are supplied by CLI args only.
- No instance settings file in v1.
- Runtime defaults are hardcoded in code, not declared in `jdbt.yml`.
- No templating support in config, fixture, or SQL loading.

## CLI Contract

- Java-style commands with explicit flags.
- Import resume option: `--resume-at`.
- Explicit runtime connection flags for source and target:
  - `--target-*`
  - `--source-*`
- Supported password inputs:
  - `--password`
  - `--password-env`
  - `--password-stdin`

## Initial Command Set

- `status`
- `create`
- `drop`
- `migrate`
- `import`
- `create-by-import`
- `load-dataset`
- `up-module-group`
- `down-module-group`
- `package-data`
- `dump-fixtures`

Excluded for initial delivery:

- `query`
- `backup`
- `restore`

## Database Support Scope

- SQL Server (`mssql-jdbc`).
- PostgreSQL (`postgresql`) in Phase 2.

## Compatibility and Divergence

- Ruby behavior in `vendor/dbt/test/**` is minimum parity baseline.
- Intentional divergence: no templating support.
- Intentional divergence: module-group import performs all deletes first (fixes known Ruby TODO behavior).

## Packaging

- Use JDK ZIP APIs.
- Use `STORED` entries.
- Deterministic output is mandatory:
  - stable entry ordering,
  - fixed timestamps,
  - reproducible bytes for same input.

## Quality Gates

- Required full gate command: `./gradlew clean spotlessCheck check fatJar`.
- Coverage thresholds:
  - line >= 85%
  - branch >= 75%

## Process Requirements

- Keep `plans/jdbt/**` and `AGENTS.md` aligned with implementation.
- Run targeted checks while iterating.
- Run full gates before handoff.
- Commit each completed step only after full gates pass.
