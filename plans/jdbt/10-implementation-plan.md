# jdbt Implementation Plan

## Phase Sequence

1. Project foundation and quality toolchain.
2. CI gate automation with early quality feedback.
3. Persistent project planning artifacts.
4. Config and model layer (`jdbt.yml`, `repository.yml`).
5. Repository merge and ordering semantics.
6. Filesystem and artifact resolution semantics.
7. Runtime orchestration core commands.
8. Import orchestration and resume behavior.
9. Migration orchestration and migration table semantics.
10. Deterministic packaging (`package-data`) with reproducibility verification.
11. CLI command integration and argument validation.
12. Documentation and parity closure.
13. PostgreSQL support (Phase 2).
14. End-user documentation (`README.md` and usage guide).
15. Remove top-level `defaults` from `jdbt.yml`; use hardcoded runtime defaults.
16. Publish reusable Agent Skill for the delivery workflow.
17. Package skill for standalone distribution and document adoption.
18. Remove `searchDirs` config and fix search root to `jdbt.yml` directory.
19. Mark all source and test packages as `@NullMarked`.
20. Enable requested Error Prone checks and remediate violations in production and test code.
21. Flatten `jdbt.yml` to a single implicit database configuration model.
22. Enable additional requested Error Prone checks, suppress accepted `UnusedException`, and fix `UnnecessarilyFullyQualified` violations.
23. Prefer `var` for local declarations where type inference remains clear and safe.
24. Enable Picnic Error Prone `JUnitClassModifiers` and disable `StaticImport` noise.
25. Extend `var` preference to try-with-resources and foreach declarations where inference is safe.
26. Add `filterProperties` config model with strict validation and ordered declaration semantics.
27. Add strict CLI `--property` parsing and propagate filter values through command/runtime interfaces.
28. Apply declared `filterProperties` replacements in SQL execution flow and enforce required/supported values.
29. Restrict import runtime tokens to `__SOURCE__`, `__TARGET__`, `__TABLE__` with non-overridable tool-provided values.
30. Document `filterProperties`, strict CLI behavior, and reserved import token semantics.
31. Port SQL Server import assert macro expansion for ASSERT_ROW_COUNT, ASSERT_DATABASE_VERSION, and ASSERT_UNCHANGED_ROW_COUNT.
32. Document SQL Server-only import assert macros and parity notes.
33. Remove unsupported `resourcePrefix` config surface and document classpath-loading divergence.
34. Convert build, CI, and quality gates to Bazel 9.1.1 with explicit package-owned targets.
35. Close parity review gaps around `!omap`, artifact layout, content schema hash, standalone import order, SQL Server metadata/maintenance, and import failure guidance.
36. Add filesystem transcript integration tests and H2-backed database integration tests.
37. Implement `export-fixtures` with properties-file object selection/custom SQL and deterministic YAML output.

## Delivery Approach

- Implement one task at a time with minimal diffs.
- Validate each task with targeted checks while iterating.
- Before task completion:
  - run full required gates,
  - update `plans/jdbt/20-task-board.yaml`,
  - commit one step.

## High-Risk Areas

- File ordering and index behavior across merged sources.
- Duplicate basename detection and conflict diagnostics.
- Import selection precedence for fixture/sql/default paths.
- Migration release-cut behavior (`*_Release-<version>.sql`).
- Deterministic ZIP output reproducibility.

## Design Constraints

- Preserve parity by default.
- Document every intentional divergence in the compatibility matrix.
- Keep default test runs database-free.
- Keep artifact outputs deterministic.

## Required Full Gates

`tools/check.sh`

## Export Fixtures Delivery Plan (Draft)

Plan status: accepted.

### Phase Sequence

1. Resolve CLI/output/table-selection decisions in `00-requirements.md`.
2. Add the driver query/export surface:
   - row-returning query API,
   - SQL-ready primary-key metadata API ordered by column name ascending,
   - fail-fast no-primary-key handling for default table SQL,
   - sequence default SQL/value support with exactly-one-row/one-column validation,
   - JDBC value normalization for fixture-safe YAML scalars with the exact temporal formats and timezone policy from requirements,
   - SQL Server and PostgreSQL JDBC implementations,
   - no-op/recording implementations for tests.
3. Add runtime export behavior:
   - load UTF-8 Java properties with duplicate-key detection,
   - export in repository order, not properties file order,
   - validate keys against a unique clean-name index of repository tables and sequences,
   - parse `--property` values through the existing strict declared-filter rules,
   - generate default SQL for empty property values,
   - apply declared filter replacements to custom export SQL before query execution,
   - query table rows and emit fixture YAML with `rN` keys, omitted nulls, and no `!omap`,
   - preserve JDBC result-set column order within row YAML,
   - query sequence values and emit scalar YAML,
   - fail fast on unknown keys, duplicate clean names, duplicate result labels, invalid sequence result shape, and no-PK default table exports.
4. Wire CLI/runner/docs:
   - add properties filename parameter,
   - add optional `--output-dir` defaulting to the directory containing `jdbt.yml`,
   - expose the command as `export-fixtures`,
   - remove the unimplemented `dump-fixtures` command surface rather than retaining an alias,
   - update README and user guide from placeholder to implemented command.
5. Validate with targeted Bazel tests and `tools/check.sh`.

### High-Risk Areas

- YAML emission must stay compatible with the existing fixture loader; mitigation is round-trip coverage for table YAML, sequence YAML, empty table exports, stable `rN` keys, omitted nulls, fixture-safe temporal/numeric/string scalars, no `!omap` tags, and byte-stable output.
- Default SQL needs driver-specific primary-key and sequence metadata but must remain deterministic; mitigation is SQL Server/PostgreSQL metadata unit coverage for composite keys and runtime tests using recording drivers.
- Clean-name keys can collide after quote/bracket/space stripping; mitigation is a repository-wide clean-name index that rejects non-unique keys before export starts.
- `.properties` parsing can silently overwrite duplicate keys if implemented with plain `Properties`; mitigation is a duplicate-detecting loader with explicit parser tests.
- CLI rename must remove the old placeholder path; mitigation is CLI tests for `export-fixtures` and for `dump-fixtures` no longer dispatching.
- Export SQL participates in declared filter replacement; mitigation is CLI/runtime coverage for `--property` parsing, required/default/supported-value validation, and deterministic custom SQL replacement.
- Byte-stable YAML depends on field order and temporal conversion; mitigation is result-set column-order preservation and explicit scalar-format tests.
- Zone-bearing temporal values need deterministic formatting; mitigation is UTC for `Instant`/non-SQL `java.util.Date`, source-local fields for `OffsetDateTime`/`ZonedDateTime`, and tests for each category.

### Validation Plan

- Targeted tests:
  - CLI dispatch for `export-fixtures` with properties path, target options, explicit `--output-dir`, default output directory, and repeatable `--property`.
  - CLI rejection/non-dispatch for the removed `dump-fixtures` command.
  - Runtime export tests for custom/default table SQL, custom/default sequence SQL, declared filter replacement, required/default/supported filter values, unknown keys, duplicate clean keys, duplicate property keys, no-PK default table export, duplicate result labels, row field order, empty table exports, and invalid sequence result shape.
  - Driver tests for SQL Server and PostgreSQL primary-key metadata, composite key rendering, sequence default SQL, row query/value normalization, and exact temporal fixture formats.
  - YAML round-trip tests through the existing fixture loader for table and sequence outputs.
- Full gate: `tools/check.sh`.

### Decision Log

- Q-01: accepted optional `--output-dir`, defaulting to the directory containing `jdbt.yml`; CLI wiring and docs will use this destination contract.
- Q-02: accepted support for both tables and sequences using clean repository object names in the properties file.
- Q-03: accepted fail-fast validation for unknown table or sequence keys.
- Q-04: accepted rename from `dump-fixtures` to `export-fixtures`; no legacy alias is planned unless later requested.
- Q-05: accepted deterministic temporal timezone policy: UTC for `Instant`/non-SQL `java.util.Date`, source-local fields for `OffsetDateTime`/`ZonedDateTime`, and as-is formatting for unzoned timestamp/local-date-time values.
