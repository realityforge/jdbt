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

## Export Fixtures Requirements

- `export-fixtures` exports repository-declared fixture data from a live target database.
- The command accepts a required Java properties file parameter.
- Each properties key identifies a clean/unquoted repository table or sequence name, for example `Rose.tblOrgUnit`.
- Each properties value is the export SQL for that table or sequence.
- Empty properties values select the default SQL generator for that table or sequence.
- `export-fixtures` accepts repeatable `--property key=value` and applies declared `filterProperties` using the existing strict validation rules.
- Filter replacement is applied to custom export SQL before query execution; generated default SQL does not contain filter tokens.
- The properties file uses Java `.properties` syntax loaded as UTF-8 with duplicate-key detection.
- Properties parsing preserves standard `.properties` comments, escaping, separators, and line continuations; duplicate keys fail fast.
- Output order is repository order, not properties file order.
- Unknown properties keys fail fast.
- Clean object names must be unique across all repository tables and sequences before any export starts; duplicate clean names fail fast.
- Default table SQL shape: `SELECT * FROM <table_name> ORDER BY <primary_key_column_1> ASC[, <primary_key_column_n> ASC]`.
- Default table primary-key columns are SQL-ready column identifiers ordered by column name ascending for Ruby parity.
- Tables without a primary key fail fast when default SQL is requested.
- Default sequence SQL returns the current sequence value for the repository sequence.
- Custom table SQL must return zero or more rows with nonblank, unique column labels.
- Custom sequence SQL must return exactly one row with exactly one column.
- Empty table result sets emit an empty YAML mapping.
- Table row field order follows JDBC result-set column order.
- Fixture scalar normalization:
  - null values are omitted,
  - strings, numbers, and booleans are emitted unchanged,
  - `java.sql.Timestamp` and `java.time.LocalDateTime` emit `dd MMM yyyy HH:mm:ss` with English month names for SQL Server parity, using their local fields as-is,
  - `java.time.Instant` and non-SQL `java.util.Date` values emit `dd MMM yyyy HH:mm:ss` in UTC,
  - `java.time.OffsetDateTime` and `java.time.ZonedDateTime` values emit `dd MMM yyyy HH:mm:ss` using the source value's own offset/zone local fields,
  - `java.sql.Date` and `java.time.LocalDate` emit `yyyy-MM-dd`,
  - `java.sql.Time` and `java.time.LocalTime` emit `HH:mm:ss`.
- Generated fixture filenames use existing fixture naming: `<base>/<module>/<fixtureDirName>/<clean-table-name>.yml`.
- Fixture output is deterministic:
  - exports follow repository module ordering, with tables then sequences in repository order,
  - rows follow supplied SQL or generated primary-key ordering,
  - table YAML row keys are `r1`, `r2`, and so on,
  - fields within each row follow JDBC result-set column order,
  - sequence YAML is emitted as a scalar value,
  - null fields are omitted to match existing fixture load semantics,
  - exported YAML does not use `!omap` tags.
- Default tests must use fake/in-memory drivers and must not require SQL Server or PostgreSQL services.

## Open Questions Register

- id: Q-01
  status: resolved
  question: What output base directory should the CLI use for generated fixture files?
  context: Ruby `dump_database_to_fixtures` receives a `base_fixture_dir`; the Java CLI placeholder has no output parameter, and the new request only names the properties file parameter.
  options:
    - Add optional `--output-dir`, defaulting to the directory containing `jdbt.yml`.
    - Make output directory a required second positional parameter.
    - Always write into the directory containing `jdbt.yml`.
  tradeoffs: Optional `--output-dir` preserves the requested one-parameter common path while retaining Ruby's explicit base-directory control. A required second parameter is explicit but changes the requested command shape. Always writing into the project tree is simple but removes a safe scratch-output workflow.
  recommended_default: Add optional `--output-dir`, defaulting to the directory containing `jdbt.yml`.
  user_decision: Add optional `--output-dir`, defaulting to the directory containing `jdbt.yml`.
  artifacts_updated: plans/jdbt/00-requirements.md, plans/jdbt/10-implementation-plan.md, plans/jdbt/20-task-board.yaml

- id: Q-02
  status: resolved
  question: Should `dump-fixtures` export repository sequences as well as tables?
  context: Ruby exports both table fixtures and sequence fixtures. The requested properties file maps table names to SQL and the examples only show tables.
  options:
    - Export only table fixtures listed in the properties file.
    - Export table fixtures listed in the properties file and also export all repository sequences by default.
    - Extend the properties file to include sequences with a separate SQL contract.
  tradeoffs: Table-only behavior matches the new properties contract exactly. Exporting all sequences preserves Ruby parity but may surprise users who only listed tables. Extending the properties contract increases scope.
  recommended_default: Export only table fixtures listed in the properties file for this change.
  user_decision: Support both table and sequence fixtures using clean repository object names in the properties file.
  artifacts_updated: plans/jdbt/00-requirements.md, plans/jdbt/10-implementation-plan.md, plans/jdbt/20-task-board.yaml, plans/jdbt/30-compatibility-matrix.md

- id: Q-03
  status: resolved
  question: How should properties keys that do not match repository tables be handled?
  context: Existing fixture/import file handling is strict and rejects unexpected files. The new properties file can name arbitrary table or sequence keys.
  options:
    - Fail fast on any unknown key.
    - Ignore unknown keys.
    - Warn and continue.
  tradeoffs: Fail-fast behavior matches existing repository strictness and catches typos. Ignoring or warning risks silently missing fixtures in automated runs.
  recommended_default: Fail fast on any unknown key.
  user_decision: Fail fast on any unknown table or sequence key.
  artifacts_updated: plans/jdbt/00-requirements.md, plans/jdbt/10-implementation-plan.md, plans/jdbt/20-task-board.yaml

- id: Q-04
  status: resolved
  question: Should the Java command be named `dump-fixtures` or `export-fixtures`?
  context: The Java CLI currently declares `dump-fixtures` as an unimplemented placeholder. The requested rename changes the command surface before implementation.
  options:
    - Rename the command to `export-fixtures` and do not keep the old placeholder.
    - Add `export-fixtures` while keeping `dump-fixtures` as an alias.
  tradeoffs: Renaming without an alias keeps the product command surface clean before the placeholder becomes supported. Keeping an alias preserves a name that has never worked and increases surface area.
  recommended_default: Rename to `export-fixtures` without keeping `dump-fixtures`.
  user_decision: Rename this action to `export-fixtures`.
  artifacts_updated: plans/jdbt/00-requirements.md, plans/jdbt/10-implementation-plan.md, plans/jdbt/20-task-board.yaml, plans/jdbt/30-compatibility-matrix.md

- id: Q-05
  status: resolved
  question: What timezone policy should fixture scalar normalization use for zone-bearing temporal values?
  context: Ruby parity only shows unzoned `java.sql.Timestamp`/`Time` conversion. Java JDBC/query APIs can expose `Instant`, `OffsetDateTime`, `ZonedDateTime`, and non-SQL `java.util.Date`, which require an explicit zone policy for deterministic formatting.
  options:
    - Use UTC for instant-like values and source-local fields for offset/zone-bearing values.
    - Use the JVM default timezone for all instant-like values.
    - Preserve database/session timezone behavior implicitly.
  tradeoffs: UTC for instant-like values is deterministic across machines. Source-local formatting preserves explicit offset/zone-bearing values without converting them. JVM or session timezone behavior can vary between environments.
  recommended_default: Use UTC for `Instant` and non-SQL `java.util.Date`; preserve source-local fields for `OffsetDateTime` and `ZonedDateTime`; format unzoned timestamp/local-date-time values as-is.
  user_decision: Implement the recommended deterministic timezone policy.
  artifacts_updated: plans/jdbt/00-requirements.md, plans/jdbt/10-implementation-plan.md, plans/jdbt/20-task-board.yaml

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
- `export-fixtures`

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
