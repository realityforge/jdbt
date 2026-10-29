# jdbt Glossary

These terms are canonical for jdbt configuration, runtime behavior, and documentation. Qualifiers are intentional where a shorter word is overloaded.

## Structure and packaging

### Database Project

The directory containing `jdbt.yml`, the local `repository.yml`, and project-owned database assets. Relative project paths resolve from this directory.

### Repository Descriptor

The `repository.yml` file in a [Database Project](#database-project) or [Database Artifact](#database-artifact). It serializes [Repository Metadata](#repository-metadata); it is not a source-code repository.

### Repository Metadata

The ordered, merged description of [Database Module](#database-module)s, schema overrides, tables, table columns, [Row Source](#row-source)s, and sequences. Jdbt composes it from pre-artifact descriptors, the local descriptor, then post-artifact descriptors.

### Database Module

A named, ordered unit of database ownership containing an optional schema override plus ordered tables and sequences. This is distinct from a Bazel, Java, or Ruby module.

### Database Artifact

A deterministic zip consumed through `preDbArtifacts` or `postDbArtifacts`. It contains `data/repository.yml` and the database assets owned by its [Database Module](#database-module)s.

### Resource Root

The single local filesystem tree selected by `resourceRoot`. Jdbt resolves project-owned database assets beneath this root and combines them with configured [Database Artifact](#database-artifact)s.

### Database Resource

A resolved database asset backed either by a file beneath the [Resource Root](#resource-root) or an entry in a [Database Artifact](#database-artifact). Its origin, logical path, display name, and content remain typed runtime data rather than an encoded path string.

### Database Contribution

An ordered database-level directory of SQL resources that establishes deployment-owned data after ordinary data or dataset establishment and before structural finalization. Contributions run once during fresh, dataset-backed, and import-backed creation, but not during [Migration](#migration) or late-table import recovery.

## Database evolution

### Migration

An ordered `.sql` [Database Resource](#database-resource) beneath the configured `migrationDir`. Its base filename is
its durable identity and its exact UTF-8 content determines its checksum.

### Release Migration

A [Migration](#migration) named with a `Release-<version>` suffix after its ordering prefix. It identifies the point
represented by a live database's `DatabaseSchemaVersion` when [Migration State](#migration-state) is first initialized.

### Migration State

The target database's unique record of applied [Migration](#migration)s, including each Migration's identity,
checksum, and application time.

## Import behavior

### Import Definition

A named entry under `imports` in `jdbt.yml`. It selects ordered [Database Module](#database-module)s and the directories containing import hooks and per-object overrides.

### Database Import

The create-by-import phase that transfers rows and sequence positions from a source database to a target database according to an [Import Definition](#import-definition). Jdbt exposes no standalone import command.

### Standard Import

The generated fallback for an [Import Row Source](#import-row-source) table or sequence when its
[Import Definition](#import-definition) supplies neither an [Import Fixture](#import-fixture) nor
[Explicit Import SQL](#explicit-import-sql). SQL Server uses live target columns at runtime, while offline
[Standard Import Script](#standard-import-script)s use ordered [Repository Metadata](#repository-metadata) columns.

### Explicit Import SQL

A per-table or per-sequence `.sql` file in an [Import Definition](#import-definition) directory. For an [Import Row Source](#import-row-source) object it overrides [Standard Import](#standard-import).

### Late Import

The import of an [Import Row Source](#import-row-source) table postponed until after
[Database Contribution](#database-contribution)s and import-only pre-late hooks, but before structural finalization.

### Standard Import Script

An offline, reusable SQL template emitted from [Repository Metadata](#repository-metadata) by `emit-standard-imports`. It uses `__TARGET__` and `__SOURCE__` database tokens and contains no live identity handling.

## Row ownership

### Row Source

The [Repository Metadata](#repository-metadata) property that assigns responsibility for establishing a table's rows. Its values are `import` and `deployment`; omission in a [Repository Descriptor](#repository-descriptor) means `import`.

### Import Row Source

A table whose rows participate in [Database Import](#database-import). Runtime selection is [Import Fixture](#import-fixture), then [Explicit Import SQL](#explicit-import-sql), then [Standard Import](#standard-import).

### Deployment Row Source

A table whose rows are established by [Initial Fixture](#initial-fixture)s, deployment hooks or other deployment mechanisms, or intentionally left empty. [Database Import](#database-import) does not delete, import, resume at, or emit a [Standard Import Script](#standard-import-script) for it.

## YAML row data

### Initial Fixture

A table or sequence YAML file in the [Database Project](#database-project)'s configured `fixtureDirName`. It is loaded during database creation and is valid only for deployment-owned table rows.

### Import Fixture

A per-table or per-sequence YAML file in an [Import Definition](#import-definition) directory. For an [Import Row Source](#import-row-source) object it overrides [Explicit Import SQL](#explicit-import-sql) and [Standard Import](#standard-import).

### Dataset Fixture

A table or sequence YAML file under a named dataset. It is loaded only by an explicit dataset command and may target either [Row Source](#row-source) because datasets are operator-requested data, not lifecycle ownership.

The durable behavior behind these terms is specified in [Database Imports](../specs/database-imports.md) and
[Database Import Timing](../specs/database-import-timing.md). [Migration](#migration) behavior is specified in
[Database Migrations](../specs/database-migrations.md).
