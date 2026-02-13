# jdbt

`jdbt` is a Java implementation of the Ruby `dbt` workflow for managing database schema and data lifecycle tasks.

This project keeps parity-first behavior with the Ruby reference while using Java-native tooling (Gradle, picocli, JDBC, strict static analysis).

## Current status

- Supported runtime drivers: SQL Server and PostgreSQL.
- Supported CLI commands:
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
  - `dump-fixtures` (declared, not implemented yet)

## Build

```bash
./gradlew clean fatJar
```

Runnable jar:

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar --help
```

## Quick start

1. Create `jdbt.yml` in the working directory.
2. Create `repository.yml` in the same directory as `jdbt.yml`.
3. Arrange module SQL/YAML files under your database layout.
4. Run a command, for example:

```bash
java -jar ./build/libs/jdbt-0.1-SNAPSHOT-all.jar status
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
```

`db/repository.yml`

```yaml
modules:
  Core:
    schema: Core
    tables:
      - '[Core].[tblExample]'
    sequences: []
```

## Documentation

- User guide: `docs/user-guide.md`
- Skill adoption guide: `skills/ADOPTION.md`
- Planning and parity tracking: `plans/jdbt/`
- Agent workflow constraints: `AGENTS.md`

## Notes

- Configuration file name is fixed: `jdbt.yml`.
- `jdbt.yml` defines a single implicit database at top-level keys.
- Top-level `defaults` and `databases` are not supported in `jdbt.yml`; runtime defaults are hardcoded.
- `searchDirs` is not configurable; jdbt always searches from the directory containing `jdbt.yml`.
- SQL source filtering is driven by declared `filterProperties` and optional `--property key=value` overrides.
- Filter properties are strict: only declared keys are accepted; missing `default` means the property is required.
- Import-only reserved tokens are tool-provided and not overridable: `__SOURCE__`, `__TARGET__`, `__TABLE__`.
- SQL Server import SQL additionally supports assert macros in import files: `ASSERT_ROW_COUNT(...)`, `ASSERT_DATABASE_VERSION(...)`, and `ASSERT_UNCHANGED_ROW_COUNT()`.
- Import assert macros are expanded only for SQL Server driver import flows (`import` and `create-by-import`).
- Import resume uses `--resume-at` (not environment variables).
