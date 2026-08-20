# jdbt User Guide

The [jdbt glossary](glossary/README.md) defines the canonical structure, import, Row Source, and fixture terms used here. [Database Imports](specs/database-imports.md) is the durable behavior specification.

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

`jdbt.yml` is required in the project directory. The project directory defaults to the current working directory and
can be selected explicitly for every command with `--project-dir PATH`. `repository.yml` and configured database
artifacts are resolved relative to that project directory.

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
- `resourceRoot`

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

Resource-root behavior is fixed:

- jdbt uses exactly one search directory
- `resourceRoot` selects that directory and defaults to `.`
- a relative `resourceRoot` is resolved against the project directory
- module SQL, fixtures, imports, migrations, and configured hook directories resolve beneath `resourceRoot`
- `searchDirs` is not a supported key in `jdbt.yml`

This permits small generated projects to keep `jdbt.yml` and a closure-only `repository.yml` together while reading
canonical database resources from a shared tree:

```yaml
resourceRoot: ../..
```

Run `validate-project` to load the manifests, resolve and hash the selected resources, and reject invalid projects
without opening a database connection:

```bash
jdbt --project-dir database/test-profiles/Mail validate-project
```

Path categories are deliberately distinct:

- `resourceRoot` and configured artifact paths are project-directory-relative when not absolute
- logical resource directories in `jdbt.yml` are resource-root-relative
- CLI path arguments and output paths are caller-working-directory-relative
- SQL Server `dataPath` and `logPath` refer to the database server filesystem

#### `imports`

`imports.<importKey>` supports:

- `modules`
- `dir`
- `preImportDirs`
- `postImportDirs`

If `modules` is missing, all Database Modules in Repository Metadata are used.

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

The Repository Descriptor, `repository.yml`, defines Database Module ordering, table ordering, ordered SQL columns, physical index identities, Row Sources, sequence ordering, and optional schema overrides.

Supported shapes:

- map style
- list/omap style

Map style example:

```yaml
modules:
  Core:
    schema: Core
    tables:
      - name: '[Core].[tblA]'
        columns: ['[ID]', '[Name]']
        indexes: ['[PK_A]', '[IX_A_Name]']
    sequences: []
  Billing:
    tables:
      - name: '[Billing].[tblInvoice]'
        columns: ['[InvoiceID]', '[Amount]']
        indexes: ['[PK_Invoice]']
        rowSource: deployment
    sequences: []
```

List style example:

```yaml
modules:
  - Core:
      schema: Core
      tables:
        - name: '[Core].[tblA]'
          columns: ['[ID]', '[Name]']
          indexes: ['[PK_A]', '[IX_A_Name]']
      sequences: []
  - Billing:
      tables:
        - name: '[Billing].[tblInvoice]'
          columns: ['[InvoiceID]', '[Amount]']
          indexes: ['[PK_Invoice]']
          rowSource: deployment
      sequences: []
```

Each table requires a qualified `name`, a non-empty ordered list of unique quoted SQL `columns`, and an ordered list of unique quoted physical SQL `indexes`. The index list may be empty. Optional `rowSource` is `import` or `deployment` and defaults to `import`. If `schema` is omitted, the Database Module name is used.

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

Options available on database-executing subcommands:

- `--database <databaseKey>` (optional compatibility flag; only `default` is accepted)
- `--driver <sqlserver|postgres>` (default: `sqlserver`)
- `--property <key=value>` (repeatable; available on SQL-executing commands)

If `--database` is omitted, `default` is used.

The offline `emit-standard-imports` command has its own credential-free option set documented below.

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

`create-with-dataset`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- create-with-dataset seed \
  --driver sqlserver \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

Optional: `--no-create` (skip drop/create, run create-with-dataset flow against existing database).

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

SQL Server expands `ASSERT_DATABASE_VERSION(<expression>)` in SQL executed by `create`, `create-with-dataset`, and the creation phases of `create-by-import`. During creation it requires the current database's `DatabaseSchemaVersion` extended property to equal the expression.

Import SQL supports these SQL Server assert macros:

- `ASSERT_ROW_COUNT(<expression>)`
- `ASSERT_DATABASE_VERSION(<expression>)`
- `ASSERT_UNCHANGED_ROW_COUNT()`

During `import` and the import phase of `create-by-import`, the database-version assertion requires the source database not to equal the expression and the target database to equal it. Row-count assertions remain import-only. All assert macros require the active driver to be `sqlserver`.

Database Import processes only Import Row Source tables. It selects an Import Fixture before Explicit Import SQL, then falls back to Standard Import. Deployment Row Source tables are not deleted, imported, or valid `--resume-at` targets. SQL Server determines identity handling from live target metadata and performs the identity toggle and import on the same JDBC session.

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

`emit-standard-imports`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- emit-standard-imports \
  --import default \
  --output-dir ./tmp/imports \
  --replace
```

This SQL Server-only command requires no database credentials. It emits a Standard Import Script for every Import Row Source table and every sequence in the selected Import Definition, regardless of checked-in Import Fixtures or Explicit Import SQL. Omit `--import` to use the configured default Import Definition. Omit `--output-dir` to use `<database-project>/tmp/imports`.

Relative output paths resolve from the Database Project. A non-empty custom output requires `--replace`. The destination may not be a symbolic link, filesystem root, Database Project, or an ancestor of the Database Project.

Output paths are:

```text
<output-dir>/<module>/import/<clean-qualified-object-name>.sql
```

Table scripts use ordered Repository Metadata columns and retain `__TARGET__` and `__SOURCE__` tokens. They deliberately omit `IDENTITY_INSERT`; live identity handling belongs to Database Import runtime behavior.

`verify-constraints`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- verify-constraints \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS \
  --schema Core \
  --check-query "EXEC [Analysis].[spPerformChecks]"
```

`--schema` may be repeated. For SQL Server, each schema runs `<schema>.spCheckConstraints` and the command fails if
any row is returned. Other drivers return no schema constraint rows unless they implement a native equivalent.

`--check-query` may be repeated. Each query must return zero rows; any returned row is reported as a failed check.

`export-fixtures`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- export-fixtures ./fixtures.properties \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS \
  --output-dir ./exported-fixtures
```

The properties file selects repository tables and sequences by clean/unquoted object name:

```properties
Core.tblOrgUnit=SELECT * FROM Core.tblOrgUnit WHERE DeletedAt IS NULL ORDER BY Id ASC
Core.tblResource=
Core.tblAttribute=
Core.tblResourceSeq=
```

Non-empty values are used as custom export SQL. Empty table values use generated SQL ordered by primary-key columns. Empty sequence values use driver-generated SQL for the current sequence value.

If `--output-dir` is omitted, files are written under the directory containing `jdbt.yml`. Output uses the normal fixture layout:

```text
<output-dir>/<module>/<fixtureDirName>/<clean-object-name>.yml
```

Use `--dataset <datasetKey>` to write dataset fixtures instead:

```text
<output-dir>/<module>/<datasetsDirName>/<datasetKey>/<clean-object-name>.yml
```

`export-fixtures` accepts repeatable `--property key=value` and applies declared `filterProperties` to custom export SQL.

`export-database-statistics`

```bash
bazel run //src/main/java/org/realityforge/jdbt:jdbt_bin -- export-database-statistics \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS \
  --output ./database-statistics.csv
```

This SQL Server-only command writes approximate row counts for every modeled table and physical index. The account needs `VIEW DEFINITION` on the database. Database-only objects are ignored; missing or unusable modeled objects fail the export without replacing an existing file. See the [Database Statistics Export specification](specs/database-statistics.md) for query, validation, CSV, and atomic-output semantics.

## Artifacts and packaging

- `package-data` creates a deterministic Database Artifact zip.
- The merged Repository Descriptor is embedded as `data/repository.yml` without losing table columns, index identities, or Row Sources.
- Database Artifacts referenced by `preDbArtifacts` and `postDbArtifacts` must contain `data/repository.yml` and relevant `data/**` entries.

## Driver-specific behavior notes

- SQL Server supports Standard Import across source and target databases and offline Standard Import Script emission.
- SQL Server drop always sets deadlock priority high and deletes backup history by default; `forceDrop` controls whether it forces `SINGLE_USER`.
- SQL Server create uses `dataPath`/`logPath` when supplied and writes `DatabaseSchemaVersion` extended metadata when `version` is configured.
- PostgreSQL Standard Import across databases and Standard Import Script emission are intentionally unsupported; use Explicit Import SQL when source and target differ.

## Troubleshooting

- `Unable to locate database '<key>' ...`: only `default` is supported as the database key; omit `--database` or pass `--database default`.
- `Unable to locate import definition by key ...`: pass `--import`, or define an import named `default` in `jdbt.yml`.
- `Unknown key 'searchDirs'`: remove `searchDirs` and configure the singular `resourceRoot` instead.
- `resourceRoot ... is not a directory`: correct the path relative to the selected project directory.
- Bazel startup or dependency errors: run `tools/update_java_deps.sh`, then `tools/check.sh`.
