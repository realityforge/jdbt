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
   - dataset load sequencing.
3. Runtime orchestration tests (driver fakes)
   - command flow ordering,
   - migration bookkeeping,
   - import resume behavior via `--resume-at`.
4. Packaging tests
   - deterministic zip ordering,
   - fixed timestamps,
   - byte-for-byte reproducibility checks.

## Coverage Policy

- Line coverage >= 85%.
- Branch coverage >= 75%.
- Coverage gates are enforced in `check`.

## Parity Mapping

- Maintain explicit mapping from Ruby tests in `vendor/dbt/test/**` to Java tests.
- Track parity status in `plans/jdbt/30-compatibility-matrix.md`.

## Non-DB Guarantee

- Default test suite must not require network or a live database.
- Database interactions are validated through fakes and deterministic fixtures.

## CI Gate Order

1. `spotlessCheck`
2. compile (`compileJava`, `compileTestJava`) with Error Prone + NullAway
3. `test`, `jacocoTestReport`, `jacocoTestCoverageVerification`
4. `fatJar`
