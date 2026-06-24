# jdbt Requirements

## Mission

Port Ruby `dbt` in `vendor/dbt` to Java while preserving behavior-critical semantics and adopting Java ecosystem conventions.

## Locked Decisions

- Java 17 baseline.
- Bazel 9.1.1 build with package-owned `BUILD.bazel` files.
- Maven coordinates: `org.realityforge:jdbt`.
- Java package root: `org.realityforge.jdbt`.
- CLI framework: picocli.
- YAML parser: SnakeYAML Engine.
- Nullability: JSpecify annotations + NullAway, strict from day one.
- Static analysis: Error Prone + NullAway.
- Formatter: palantir-java-format.
- CI platform: GitHub Actions.
- Bazel targets list source files explicitly; `glob()` is not allowed.
- Every Java source directory owns its own `BUILD.bazel`; targets do not list source files from child, sibling, or parent directories.

## Configuration Model

- Project config file is fixed and non-configurable: `jdbt.yml`.
- `jdbt.yml` defines a single implicit database at the top level; top-level `defaults` and `databases` are not supported.
- `searchDirs` is not supported in `jdbt.yml`.
- `resourcePrefix` is not supported in `jdbt.yml` (classpath resource loading is out of scope).
- The only search directory is the directory containing `jdbt.yml`.
- `repository.yml` remains first-class and layout-compatible.
- Runtime connection settings are supplied by CLI args only.
- SQL Server database file-placement and maintenance settings are supplied by `jdbt.yml` keys:
  `dataPath`, `logPath`, `forceDrop`, `deleteBackupHistory`, `reindexOnImport`, and `shrinkOnImport`.
- No instance settings file in v1.
- Runtime defaults are hardcoded in code, not declared in `jdbt.yml`.
- No templating support in config or fixtures; SQL source supports deterministic token replacement via declared `filterProperties`.
- `filterProperties` keys are strict, declared in `jdbt.yml`, and optionally constrained with `supportedValues`.
- Reserved import-only SQL tokens are tool-provided and non-overridable: `__SOURCE__`, `__TARGET__`, `__TABLE__`.
- SQL Server import flows support assert macros in import SQL: `ASSERT_ROW_COUNT(...)`, `ASSERT_DATABASE_VERSION(...)`, and `ASSERT_UNCHANGED_ROW_COUNT()`.

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
- SQL-executing commands accept repeatable `--property key=value` for declared filter properties only.

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

- Required full gate command: `tools/check.sh`.
- Coverage thresholds:
  - line >= 85%
  - branch >= 75%

## Process Requirements

- Keep `plans/jdbt/**` and `AGENTS.md` aligned with implementation.
- Run targeted checks while iterating.
- Run full gates before handoff.
- Commit each completed step only after full gates pass.
