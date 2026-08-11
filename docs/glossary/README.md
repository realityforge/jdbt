# jdbt Glossary

These terms are canonical for jdbt configuration, runtime behavior, and documentation. Qualifiers are intentional where a shorter word is overloaded.

## Structure and packaging

### Database Project

The directory containing `jdbt.yml`, the local `repository.yml`, and project-owned database assets. Relative project paths resolve from this directory.

### Repository Descriptor

The `repository.yml` file in a [Database Project](#database-project) or [Database Artifact](#database-artifact). It serializes [Repository Metadata](#repository-metadata); it is not a source-code repository.

### Repository Metadata

The ordered, merged description of [Database Modules](#database-module), schema overrides, tables, table columns, [Row Sources](#row-source), and sequences. Jdbt composes it from pre-artifact descriptors, the local descriptor, then post-artifact descriptors.

### Database Module

A named, ordered unit of database ownership containing an optional schema override plus ordered tables and sequences. This is distinct from a Bazel, Java, or Ruby module.

### Database Artifact

A deterministic zip consumed through `preDbArtifacts` or `postDbArtifacts`. It contains `data/repository.yml` and the database assets owned by its [Database Modules](#database-module).

## Import behavior

### Import Definition

A named entry under `imports` in `jdbt.yml`. It selects ordered [Database Modules](#database-module) and the directories containing import hooks and per-object overrides.

### Database Import

The runtime operation that transfers rows and sequence positions from a source database to a target database according to an [Import Definition](#import-definition). Use this term when distinguishing the operation from its configuration or files.

### Standard Import

The generated fallback for an [Import Row Source](#import-row-source) table or sequence when its [Import Definition](#import-definition) supplies neither an [Import Fixture](#import-fixture) nor [Explicit Import SQL](#explicit-import-sql). For a SQL Server table it is an `INSERT ... SELECT` across source and target databases using columns discovered from live target metadata. Offline [Standard Import Scripts](#standard-import-script) instead use the ordered [Repository Metadata](#repository-metadata) columns.

### Explicit Import SQL

A per-table or per-sequence `.sql` file in an [Import Definition](#import-definition) directory. For an [Import Row Source](#import-row-source) object it overrides [Standard Import](#standard-import).

### Standard Import Script

An offline, reusable SQL template emitted from [Repository Metadata](#repository-metadata) by `emit-standard-imports`. It uses `__TARGET__` and `__SOURCE__` database tokens and contains no live identity handling.

## Row ownership

### Row Source

The [Repository Metadata](#repository-metadata) property that assigns responsibility for establishing a table's rows. Its values are `import` and `deployment`; omission in a [Repository Descriptor](#repository-descriptor) means `import`.

### Import Row Source

A table whose rows participate in [Database Import](#database-import). Runtime selection is [Import Fixture](#import-fixture), then [Explicit Import SQL](#explicit-import-sql), then [Standard Import](#standard-import).

### Deployment Row Source

A table whose rows are established by [Initial Fixtures](#initial-fixture), deployment hooks or other deployment mechanisms, or intentionally left empty. [Database Import](#database-import) does not delete, import, resume at, or emit a [Standard Import Script](#standard-import-script) for it.

## YAML row data

### Initial Fixture

A table or sequence YAML file in the [Database Project's](#database-project) configured `fixtureDirName`. It is loaded during database creation and is valid only for deployment-owned table rows.

### Import Fixture

A per-table or per-sequence YAML file in an [Import Definition](#import-definition) directory. For an [Import Row Source](#import-row-source) object it overrides [Explicit Import SQL](#explicit-import-sql) and [Standard Import](#standard-import).

### Dataset Fixture

A table or sequence YAML file under a named dataset. It is loaded only by an explicit dataset command and may target either [Row Source](#row-source) because datasets are operator-requested data, not lifecycle ownership.

The durable behavior behind these terms is specified in [Database Imports](../specs/database-imports.md).
