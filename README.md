# jdbt

`jdbt` is a Java implementation of the Ruby `dbt` workflow for managing database schema and data lifecycle tasks.

This project keeps parity-first behavior with the Ruby reference while using Java-native tooling (Bazel, picocli, JDBC, strict static analysis).

## Current status

- Supported runtime drivers: SQL Server and PostgreSQL.
- Supported CLI commands:
  - `status`
  - `validate-project`
  - `create`
  - `create-with-dataset`
  - `drop`
  - `migrate`
  - `import`
  - `create-by-import`
  - `load-dataset`
  - `up-module-group`
  - `down-module-group`
  - `package-data`
  - `emit-standard-imports`
  - `verify-constraints`
  - `export-fixtures`

## Build

```bash
tools/check.sh
```

The check verifies generated dependency files without rewriting them. After changing
`third_party/java/dependencies.yml` or `tools/java-format/dependencies.yml`, regenerate the checked-in Bazel
outputs and lockfile with `tools/update_java_deps.sh`.

Run the CLI through Bazel:

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- --help
```

Build a runnable deploy jar:

```bash
bazel build //src/main/java/org/realityforge/jdbt:jdbt_bin_deploy.jar
```

## Quick start

1. Create `jdbt.yml` in the working directory, or select its directory with `--project-dir`.
2. Create `repository.yml` in the same directory as `jdbt.yml`.
3. Arrange module SQL/YAML files under your database layout.
4. Run a command, for example:

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- status
```

## Minimal config example

`jdbt.yml`

```yaml
migrations: true
datasets: [seed]
imports:
  default:
    modules: [Core]
moduleGroups:
  all:
    modules: [Core]
    importEnabled: true
filterProperties:
  environment:
    pattern: __ENVIRONMENT__
    default: dev
    supportedValues: [dev, test, prod]
forceDrop: false
deleteBackupHistory: true
reindexOnImport: true
shrinkOnImport: false
```

`repository.yml`

```yaml
modules:
  Core:
    schema: Core
    tables:
      - name: '[Core].[tblExample]'
        columns: ['[ID]', '[Name]']
      - name: '[Core].[tblDeploymentSetting]'
        columns: ['[ID]', '[Value]']
        rowSource: deployment
    sequences: []
```

## Documentation

- User guide: [`docs/user-guide.md`](docs/user-guide.md)
- Canonical terms: [`docs/glossary/README.md`](docs/glossary/README.md)
- Database Import specification: [`docs/specs/database-imports.md`](docs/specs/database-imports.md)
- Repository Metadata decision: [`docs/adr/0001-repository-metadata-for-standard-imports.md`](docs/adr/0001-repository-metadata-for-standard-imports.md)
- Planning and parity tracking: `plans/jdbt/`
- Agent workflow constraints: `AGENTS.md`

## Notes

- Configuration file name is fixed: `jdbt.yml`.
- `jdbt.yml` defines a single implicit database at top-level keys.
- Top-level `defaults` and `databases` are not supported in `jdbt.yml`; runtime defaults are hardcoded.
- `resourceRoot` selects one resource tree relative to the project directory and defaults to `.`; `searchDirs` remains
  unsupported.
- `jdbt --project-dir PATH validate-project` validates manifests and selected resources without connecting to a
  database.
- `resourcePrefix` is not supported; Java runtime does not load database assets from classpath resources.
- SQL source filtering is driven by declared `filterProperties` and optional `--property key=value` overrides.
- Filter properties are strict: only declared keys are accepted; missing `default` means the property is required.
- Import-only reserved tokens are tool-provided and not overridable: `__SOURCE__`, `__TARGET__`, `__TABLE__`.
- Fixture export is driven by a Java properties file keyed by clean table/sequence names; empty values use generated default SQL.
- SQL Server creation and import SQL supports `ASSERT_DATABASE_VERSION(...)`. Creation checks the current database; import checks the source and target databases.
- SQL Server import SQL additionally supports `ASSERT_ROW_COUNT(...)` and `ASSERT_UNCHANGED_ROW_COUNT()`.
- Database-version assertions are expanded during `create`, `create-with-dataset`, the creation phases of `create-by-import`, and SQL Server import flows. Row-count assertions remain import-only.
- Import resume uses `--resume-at` (not environment variables).
- `emit-standard-imports` is an offline SQL Server command; it requires no database credentials and emits tokenized Standard Import Scripts from Repository Metadata.
- SQL Server database file placement and maintenance settings are configured in `jdbt.yml` with `dataPath`, `logPath`, `forceDrop`, `deleteBackupHistory`, `reindexOnImport`, and `shrinkOnImport`.
