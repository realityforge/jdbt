# jdbt Completion Report

Date: 2026-06-25

## Scope

This report covers the Bazel conversion, parity-review rounds against Ruby `vendor/dbt`, fixes made from those reviews, test expansion, and remaining known gaps.

## Build and CI Changes

- Replaced the Gradle build with Bazel 9.1.1 pinned in `.bazelversion`.
- Added root `MODULE.bazel`, `.bazelrc`, package-owned `BUILD.bazel` files, dependency lock files, and generated third-party Java dependency targets.
- Removed Gradle wrapper and Gradle build files.
- Added `tools/check.sh` as the required local/CI gate:
  - dependency regeneration check,
  - buildifier check,
  - palantir-java-format check,
  - `bazel build //...`,
  - `bazel test //...`,
  - Bazel LCOV coverage with line >= 85% and branch >= 75%.
- Added GitHub Actions CI that installs Java 17, sets up Bazel, and runs `tools/check.sh`.
- Verified the Bazel structural rules with a local script: no `glob()` usage and Java source entries are same-directory filenames.

## Parity Review Rounds

### Round 1 Findings Fixed

- Ruby `!omap` repository parsing:
  - Added SnakeYAML support for `!omap` and `tag:yaml.org,2002:omap`.
  - Added repository loader coverage for Ruby-style tagged omap module declarations.
- `package-data` artifact layout:
  - Fixed package-data zips to root payload under `data/`.
  - Added loader round-trip coverage through `postDbArtifacts`.
- Schema hash:
  - Replaced repository-metadata-only hashing with Ruby-style ordered path plus MD5-content hashing for SQL, fixtures, import files, hooks, and migrations.
- Standalone import order and delete behavior:
  - Standalone imports now follow `imports.<key>.modules` order.
  - Standalone imports delete per module before importing that module.
  - Module-group imports retain the documented Java divergence: all deletes before imports.
- Import failure diagnostics:
  - Table and sequence import failures now include the clean object name and exact `--resume-at=<name>` guidance.
- SQL Server driver basics:
  - Added version extended property on create.
  - Added broader schema-object drop behavior.
  - Added post-import reindex/statistics maintenance.

### Round 2 Finding Fixed

- SQL Server create/drop/import maintenance was incomplete and under-documented:
  - Added `jdbt.yml` keys `dataPath`, `logPath`, `forceDrop`, `deleteBackupHistory`, `reindexOnImport`, and `shrinkOnImport`.
  - Defaults now match Ruby SQL Server behavior:
    - `forceDrop: false`,
    - `deleteBackupHistory: true`,
    - `reindexOnImport: true`,
    - `shrinkOnImport: false`.
  - Drop now sets deadlock priority high, deletes backup history by default, only forces `SINGLE_USER` when requested, and uses conditional drop SQL.
  - Create now supports optional SQL Server data/log file placement and writes version metadata when configured.
  - Import maintenance now honors reindex/statistics and shrink settings.
  - Added focused driver and config tests for those behaviors.

### Round 3 Status

Round 3 found no major Java parity or Bazel-rule blocker. The review specifically rechecked the SQL Server options fixed from Round 2, confirmed `.bazelversion` pins Bazel 9.1.1, found no `glob()` usage in BUILD files, and found package-owned Java source lists. Its remaining findings were closure work: run `tools/check.sh`, record the final evidence, and commit.

## Tests Added or Expanded

- Filesystem transcript integration:
  - custom create directories,
  - custom dataset directories,
  - fixture ordering,
  - import failure and restart behavior using `--resume-at`.
- Package-data integration:
  - real zip creation,
  - `data/` entry validation,
  - artifact loader round-trip,
  - generated package-data zip executing import hooks and import SQL through runtime artifact loading.
- Real in-memory database integration:
  - H2-backed runtime create and fixture load test.
- SQL Server driver tests:
  - data/log path create SQL,
  - version metadata,
  - backup-history deletion default,
  - force-drop behavior,
  - reindex/statistics maintenance,
  - shrink maintenance.
- Existing unit tests were updated for Bazel strict deps and the metadata-aware driver API.

## Intentional Divergences

- `jdbt.yml` is a single implicit database config keyed as `default`; Ruby-style top-level `defaults` and `databases` are rejected.
- `searchDirs` is not configurable; assets resolve from the directory containing `jdbt.yml`.
- `resourcePrefix`/classpath asset loading is unsupported; filesystem roots and zip artifacts are supported.
- No ERB/template processing in config or fixtures; SQL filtering is explicit through declared `filterProperties`.
- Import resume uses explicit `--resume-at`, not `IMPORT_RESUME_AT`.
- Java import tokens use `__SOURCE__`, `__TARGET__`, and `__TABLE__`; mixed Ruby token forms are not supported.
- Module-group import intentionally deletes all selected module rows before import to resolve the Ruby TODO behavior; standalone import follows Ruby sequencing.
- PostgreSQL cross-database standard import SQL is not generated; explicit import SQL is required when source and target differ.
- `query`, `backup`, and `restore` are excluded from the Java CLI scope.

## Remaining Known Gaps

- No default test uses a live external SQL Server or PostgreSQL instance. Driver SQL is validated with focused unit tests, fake drivers, and H2 integration.
- SQL Server backup/restore command surfaces remain out of scope.

## Evidence Collected

- `./gradlew clean spotlessCheck check fatJar`: passed before build conversion as baseline.
- `bazel build //src/main/java/org/realityforge/jdbt:jdbt`: passed.
- `bazel build //src/main/java/org/realityforge/jdbt:jdbt_bin_deploy.jar`: passed.
- `bazel test //src/test/java/org/realityforge/jdbt:all_tests`: passed.
- `bazel coverage //src/test/java/org/realityforge/jdbt:all_tests --combined_report=lcov`: passed with line 88.38%, branch 81.70%.
- `bazel test //src/test/java/org/realityforge/jdbt/config:config_tests //src/test/java/org/realityforge/jdbt/db:db_tests //src/test/java/org/realityforge/jdbt/runtime:runtime_tests`: passed after SQL Server parity fixes.
- `bazel test //src/test/java/org/realityforge/jdbt/cli:cli_tests`: passed after package-artifact execution coverage.
- `tools/check.sh`: passed; `bazel build //...` succeeded, 9 Bazel test targets passed, coverage succeeded with line 88.78% and branch 81.61%.
