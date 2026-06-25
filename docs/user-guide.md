# jdbt User Guide

## Prerequisites

- Java 17+
- A checked-out repository containing your `jdbt.yml` and SQL/YAML resources
- Database access for the driver you choose (`sqlserver` or `postgres`)

Build the runnable jar:

```bash
bazel build //src/main/java/org/realityforge/jdbt:jdbt_bin_deploy.jar
```

Run help:

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- --help
```

## Configuration files

### `jdbt.yml`

`jdbt.yml` is required and must exist in the current working directory when you run `jdbt`.

Top-level keys:

- `upDirs`
- `downDirs`
- `finalizeDirs`
- `preCreateDirs`
- `postCreateDirs`
- `datasets`
- `datasetsDirName`
- `preDatasetDirs`
- `postDatasetDirs`
- `fixtureDirName`
- `migrations`
- `migrationsAppliedAtCreate`
- `migrationsDirName`
- `version`
- `dataPath`
- `logPath`
- `forceDrop`
- `deleteBackupHistory`
- `reindexOnImport`
- `shrinkOnImport`
- `preDbArtifacts`
- `postDbArtifacts`
- `filterProperties`
- `imports`
- `moduleGroups`

`jdbt.yml` no longer supports a top-level `defaults` key.

Classpath resource loading is not supported; jdbt resolves database assets from the project filesystem root and configured zip artifacts.

Runtime defaults are hardcoded and currently match Ruby-compatible defaults for:

- `upDirs`
- `downDirs`
- `finalizeDirs`
- `preCreateDirs`
- `postCreateDirs`
- `preImportDirs`
- `postImportDirs`
- `importDir`
- `datasetsDirName`
- `preDatasetDirs`
- `postDatasetDirs`
- `fixtureDirName`
- `migrationsDirName`
- `indexFileName`
- default database key (`default`)
- default import key (`default`)

`jdbt.yml` defines configuration for a single implicit database keyed as `default`.

Unknown keys are rejected.

Search directory behavior is fixed:

- jdbt uses exactly one search directory
- that directory is where `jdbt.yml` is located
- `searchDirs` is not a supported key in `jdbt.yml`

#### `imports`

`imports.<importKey>` supports:

- `modules`
- `dir`
- `preImportDirs`
- `postImportDirs`

If `modules` is missing, all repository modules are used.

#### `moduleGroups`

`moduleGroups.<groupKey>` supports:

- `modules` (required)
- `importEnabled`

#### `filterProperties`

`filterProperties.<propertyKey>` supports:

- `pattern` (required)
- `default` (optional; if omitted, the property is required at runtime)
- `supportedValues` (optional list; if present, runtime values must match one entry exactly)

Rules:

- filter property keys are strict; only declared keys are accepted via CLI `--property`.
- reserved keys `sourceDatabase`, `targetDatabase`, and `table` are tool-provided and cannot be declared.
- reserved patterns `__SOURCE__`, `__TARGET__`, and `__TABLE__` cannot be declared.
- replacement order is deterministic and follows declaration order in `jdbt.yml`.

#### SQL Server settings

These keys mirror Ruby SQL Server runtime behavior and are ignored by non-SQL Server drivers:

- `dataPath` (optional): base directory for the `.mdf` file in `CREATE DATABASE`.
- `logPath` (optional): base directory for the `.ldf` file in `CREATE DATABASE`.
- `forceDrop` (default: `false`): set the database to `SINGLE_USER` with rollback before drop.
- `deleteBackupHistory` (default: `true`): delete MSDB backup history before drop.
- `reindexOnImport` (default: `true`): run SQL Server reindex/statistics maintenance after import.
- `shrinkOnImport` (default: `false`): shrink the database after each imported module, then reindex module tables when `reindexOnImport` is enabled.

### `repository.yml`

`repository.yml` defines module ordering, table ordering, sequence ordering, and optional schema overrides.

Supported shapes:

- map style
- list/omap style

Map style example:

```yaml
modules:
  Core:
    schema: Core
    tables:
      - '[Core].[tblA]'
    sequences: []
  Billing:
    tables:
      - '[Billing].[tblInvoice]'
    sequences: []
```

List style example:

```yaml
modules:
  - Core:
      schema: Core
      tables:
        - '[Core].[tblA]'
      sequences: []
  - Billing:
      tables:
        - '[Billing].[tblInvoice]'
      sequences: []
```

If `schema` is omitted, the module name is used.

## Directory conventions

The default logical directories (unless overridden) are:

- up: `.`, `types`, `views`, `functions`, `stored-procedures`, `misc`
- down: `down`
- finalize: `triggers`, `finalize`
- pre-create hooks: `db-hooks/pre`
- post-create hooks: `db-hooks/post`
- import hooks: `import-hooks/pre`, `import-hooks/post`
- fixtures: `fixtures`
- datasets root: `datasets`
- migrations root: `migrations`
- index file: `index.txt`

`index.txt` controls ordering when present.

All module and hook paths are resolved relative to the directory containing `jdbt.yml`.

## CLI usage

Global options available on subcommands:

- `--database <databaseKey>` (optional compatibility flag; only `default` is accepted)
- `--driver <sqlserver|postgres>` (default: `sqlserver`)
- `--property <key=value>` (repeatable; available on SQL-executing commands)

If `--database` is omitted, `default` is used.

### Connection options

Commands that target a live database require target connection options:

- `--target-host`
- `--target-port` (default value: `1433`)
- `--target-database`
- `--target-username`
- exactly one target password source:
  - `--password <value>`
  - `--password-env <ENV_VAR>`
  - `--password-stdin`

Import commands also require source connection options:

- `--source-host`
- `--source-port` (default value: `1433`)
- `--source-database`
- `--source-username`
- exactly one source password source:
  - `--source-password <value>`
  - `--source-password-env <ENV_VAR>`
  - `--source-password-stdin`

Note: when using PostgreSQL, provide `--target-port 5432` and `--source-port 5432` as needed.

### Commands

`status`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- status
```

`create`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- create \
  --driver sqlserver \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

Optional: `--no-create` (skip drop/create, run create flow against existing database).

`drop`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- drop \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`migrate`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- migrate \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`import`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- import \
  --import default \
  --resume-at Core.tblA \
  --target-host localhost --target-port 1433 \
  --target-database TargetDb --target-username sa --password-env TARGET_PASS \
  --source-host localhost --source-port 1433 \
  --source-database SourceDb --source-username sa --source-password-env SOURCE_PASS
```

Optional: `--module-group <groupKey>`.

Import-only reserved SQL tokens:

- `__SOURCE__` resolves to source database name
- `__TARGET__` resolves to target database name
- `__TABLE__` resolves to the current import table/sequence

These values are tool-provided during import/create-by-import and cannot be supplied via `--property`.

Import-only SQL Server assert macros:

- `ASSERT_ROW_COUNT(<expression>)`
- `ASSERT_DATABASE_VERSION(<expression>)`
- `ASSERT_UNCHANGED_ROW_COUNT()`

These macros are expanded only during `import` and `create-by-import` when the active driver is `sqlserver`.

`create-by-import`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- create-by-import \
  --import default \
  --target-host localhost --target-port 1433 \
  --target-database TargetDb --target-username sa --password-env TARGET_PASS \
  --source-host localhost --source-port 1433 \
  --source-database SourceDb --source-username sa --source-password-env SOURCE_PASS
```

Optional: `--resume-at <tableOrSequence>`, `--no-create`.

`load-dataset`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- load-dataset seed \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`up-module-group` / `down-module-group`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- up-module-group all \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- down-module-group all \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`package-data`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- package-data \
  --output ./build/data.zip
```

`export-fixtures`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- export-fixtures ./fixtures.properties \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS \
  --output-dir ./exported-fixtures
```

The properties file selects repository tables and sequences by clean/unquoted object name:

```properties
Rose.tblOrgUnit=SELECT * FROM Rose.tblOrgUnit WHERE DeletedAt IS NULL ORDER BY Id ASC
Rose.tblResource=
Rose.tblAttribute=
Rose.tblResourceSeq=
```

Non-empty values are used as custom export SQL. Empty table values use generated SQL ordered by primary-key columns. Empty sequence values use driver-generated SQL for the current sequence value.

If `--output-dir` is omitted, files are written under the directory containing `jdbt.yml`. Output uses the normal fixture layout:

```text
<output-dir>/<module>/<fixtureDirName>/<clean-object-name>.yml
```

`export-fixtures` accepts repeatable `--property key=value` and applies declared `filterProperties` to custom export SQL.

## Artifacts and packaging

- `package-data` creates deterministic zip output.
- `repository.yml` is embedded in package data.
- Artifacts referenced by `preDbArtifacts` and `postDbArtifacts` must be zip files with `data/repository.yml` and relevant `data/**` entries.

## Driver-specific behavior notes

- SQL Server supports generated standard import SQL that references source and target databases.
- SQL Server drop always sets deadlock priority high and deletes backup history by default; `forceDrop` controls whether it forces `SINGLE_USER`.
- SQL Server create uses `dataPath`/`logPath` when supplied and writes `DatabaseSchemaVersion` extended metadata when `version` is configured.
- PostgreSQL standard cross-database import SQL is intentionally not generated; use explicit import SQL files when source and target differ.

## Troubleshooting

- `Unable to locate database '<key>' ...`: only `default` is supported as the database key; omit `--database` or pass `--database default`.
- `Unable to locate import definition by key ...`: pass `--import`, or define an import named `default` in `jdbt.yml`.
- `Unknown key 'searchDirs'`: remove `searchDirs` from `jdbt.yml`; path resolution is fixed to the `jdbt.yml` directory.
- Bazel startup or dependency errors: run `tools/update_java_deps.sh`, then `tools/check.sh`.
