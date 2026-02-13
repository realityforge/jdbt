# jdbt User Guide

## Prerequisites

- Java 17+
- A checked-out repository containing your `jdbt.yml` and SQL/YAML resources
- Database access for the driver you choose (`sqlserver` or `postgres`)

Build the runnable jar:

```bash
./gradlew clean fatJar
```

Run help:

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar --help
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
- `resourcePrefix`
- `preDbArtifacts`
- `postDbArtifacts`
- `filterProperties`
- `imports`
- `moduleGroups`

`jdbt.yml` no longer supports a top-level `defaults` key.

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
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar status
```

`create`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar create \
  --driver sqlserver \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

Optional: `--no-create` (skip drop/create, run create flow against existing database).

`drop`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar drop \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`migrate`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar migrate \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`import`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar import \
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

`create-by-import`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar create-by-import \
  --import default \
  --target-host localhost --target-port 1433 \
  --target-database TargetDb --target-username sa --password-env TARGET_PASS \
  --source-host localhost --source-port 1433 \
  --source-database SourceDb --source-username sa --source-password-env SOURCE_PASS
```

Optional: `--resume-at <tableOrSequence>`, `--no-create`.

`load-dataset`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar load-dataset seed \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`up-module-group` / `down-module-group`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar up-module-group all \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar down-module-group all \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`package-data`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar package-data \
  --output ./build/data.zip
```

`dump-fixtures`

- Currently declared but not implemented; invoking it throws an explicit runtime error.

## Artifacts and packaging

- `package-data` creates deterministic zip output.
- `repository.yml` is embedded in package data.
- Artifacts referenced by `preDbArtifacts` and `postDbArtifacts` must be zip files with `data/repository.yml` and relevant `data/**` entries.

## Driver-specific behavior notes

- SQL Server supports generated standard import SQL that references source and target databases.
- PostgreSQL standard cross-database import SQL is intentionally not generated; use explicit import SQL files when source and target differ.

## Troubleshooting

- `Unable to locate database '<key>' ...`: only `default` is supported as the database key; omit `--database` or pass `--database default`.
- `Unable to locate import definition by key ...`: pass `--import`, or define an import named `default` in `jdbt.yml`.
- `Unknown key 'searchDirs'`: remove `searchDirs` from `jdbt.yml`; path resolution is fixed to the `jdbt.yml` directory.
- Fat jar startup security/signature errors: rebuild with `./gradlew clean fatJar` using current build config.
