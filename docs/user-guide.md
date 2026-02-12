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

- `defaults`
- `databases`

`databases` must contain at least one entry.

#### `defaults` keys

All keys are optional.

- `searchDirs`
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
- `defaultDatabase`
- `defaultImport`

Ruby-compatible defaults are applied when values are not provided.

#### `databases.<key>` keys

- `searchDirs` (required directly or via defaults)
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
- `imports`
- `moduleGroups`

Unknown keys are rejected.

#### `imports`

`databases.<db>.imports.<importKey>` supports:

- `modules`
- `dir`
- `preImportDirs`
- `postImportDirs`

If `modules` is missing, all repository modules are used.

#### `moduleGroups`

`databases.<db>.moduleGroups.<groupKey>` supports:

- `modules` (required)
- `importEnabled`

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

## CLI usage

Global options available on subcommands:

- `--database <databaseKey>`
- `--driver <sqlserver|postgres>` (default: `sqlserver`)

If `--database` is omitted, `defaults.defaultDatabase` is used.

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
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar status --database default
```

`create`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar create \
  --database default \
  --driver sqlserver \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

Optional: `--no-create` (skip drop/create, run create flow against existing database).

`drop`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar drop \
  --database default \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`migrate`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar migrate \
  --database default \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`import`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar import \
  --database default \
  --import default \
  --resume-at Core.tblA \
  --target-host localhost --target-port 1433 \
  --target-database TargetDb --target-username sa --password-env TARGET_PASS \
  --source-host localhost --source-port 1433 \
  --source-database SourceDb --source-username sa --source-password-env SOURCE_PASS
```

Optional: `--module-group <groupKey>`.

`create-by-import`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar create-by-import \
  --database default \
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
  --database default \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`up-module-group` / `down-module-group`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar up-module-group all \
  --database default \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar down-module-group all \
  --database default \
  --target-host localhost --target-port 1433 \
  --target-database MyDb --target-username sa --password-env DB_PASS
```

`package-data`

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar package-data \
  --database default \
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

- `No database specified via --database ...`: set `defaults.defaultDatabase` or pass `--database`.
- `Unable to locate import definition by key ...`: set `defaults.defaultImport`, pass `--import`, or define the import key in `jdbt.yml`.
- `Duplicate copies of repository.yml found in database search path`: ensure only one local `repository.yml` is present across `searchDirs`.
- Fat jar startup security/signature errors: rebuild with `./gradlew clean fatJar` using current build config.
