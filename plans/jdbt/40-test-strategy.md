# jdbt Test Strategy

## Objectives

- Match Ruby behavior as baseline parity.
- Add stronger Java-side tests for diagnostics, validation, and determinism.
- Keep default test execution independent from live databases.

## Test Layers

1. Unit tests
   - config parsing and validation,
   - repository merge rules,
   - index ordering and duplicate detection,
   - SQL filter and batching logic.
2. Filesystem integration tests
   - realistic directory trees,
   - pre/post artifact overlays,
   - fixture/import directory strictness,
   - dataset load sequencing,
   - failure and `--resume-at` restart behavior transcript checks.
3. Runtime orchestration tests (driver fakes)
   - command flow ordering,
   - migration bookkeeping,
   - import resume behavior via `--resume-at`.
4. Packaging tests
   - deterministic zip ordering,
   - fixed timestamps,
   - byte-for-byte reproducibility checks,
   - packaged artifact round-trip through `postDbArtifacts`,
   - generated package-data zip executing import hooks and import SQL through runtime artifact loading.
5. In-memory database integration tests
   - runtime execution against H2-backed JDBC,
   - real SQL/fixture insert verification without external services.
6. Driver parity tests
   - SQL Server drop/create SQL for backup-history deletion, force-drop, version metadata, and data/log paths,
   - SQL Server import maintenance toggles for reindex/statistics and shrink behavior,
   - PostgreSQL driver-specific standard import constraints.

## Coverage Policy

- Line coverage >= 85%.
- Branch coverage >= 75%.
- Coverage gates are enforced by `tools/check.sh` using Bazel LCOV output.

## Parity Mapping

- Maintain explicit mapping from Ruby tests in `vendor/dbt/test/**` to Java tests.
- Track parity status in `plans/jdbt/30-compatibility-matrix.md`.

## External DB Guarantee

- Default test suite must not require network or an external live database.
- Database interactions are validated through fakes, deterministic fixtures, and an in-memory H2 database.

## CI Gate Order

1. `tools/update_java_deps.sh`
2. `bazel run //:buildifier_check`
3. `tools/java_format.sh check`
4. `bazel build //...`
5. `bazel test //...`
6. `bazel coverage //src/test/java/org/realityforge/jdbt:all_tests --combined_report=lcov`
7. `tools/check_coverage.py bazel-out/_coverage/_coverage_report.dat 0.85 0.75`
