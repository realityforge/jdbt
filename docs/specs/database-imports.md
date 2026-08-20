# Database Imports

This specification defines jdbt's durable row-ownership, [Database Import](../glossary/README.md#database-import), and [Standard Import Script](../glossary/README.md#standard-import-script) behavior.

## Repository contract

Each table in a [Repository Descriptor](../glossary/README.md#repository-descriptor) is an object with:

- a qualified `name`;
- a non-empty, ordered list of unique quoted SQL `columns`; and
- a mandatory ordered list of unique quoted physical SQL `indexes`; and
- an optional `rowSource`, whose values are `import` and `deployment` and whose default is `import`.

[Database Module](../glossary/README.md#database-module), table, column, and sequence order are significant. [Repository Metadata](../glossary/README.md#repository-metadata) is composed module-atomically in pre-artifact, local-project, then post-artifact order. [Database Artifact](../glossary/README.md#database-artifact) packaging must preserve the complete merged model.

## Row Source behavior

An [Import Row Source](../glossary/README.md#import-row-source) table participates in Database Import. Within the selected [Import Definition](../glossary/README.md#import-definition) its row input precedence is:

1. [Import Fixture](../glossary/README.md#import-fixture);
2. [Explicit Import SQL](../glossary/README.md#explicit-import-sql);
3. [Standard Import](../glossary/README.md#standard-import).

A [Deployment Row Source](../glossary/README.md#deployment-row-source) table is established by deployment assets, an external deployment mechanism, or deliberate absence of rows. Database Import must not delete it, import it, select it through `--resume-at`, or emit a Standard Import Script for it. A deployment asset is not required merely to justify this classification.

[Dataset Fixtures](../glossary/README.md#dataset-fixture) are independent of [Row Source](../glossary/README.md#row-source). They load only after an operator explicitly selects a dataset and may target either Row Source.

## Contradictory inputs

Before a create or import flow mutates a database, jdbt rejects selected-project contradictions:

- an [Initial Fixture](../glossary/README.md#initial-fixture) targeting an Import Row Source table;
- an Import Fixture or Explicit Import SQL targeting a Deployment Row Source table;
- both an Import Fixture and Explicit Import SQL targeting the same table; or
- `--resume-at` naming a Deployment Row Source table.

Diagnostics identify the Import Definition or asset and the affected table.

## SQL Server identity behavior

Identity preservation is runtime behavior, not Standard Import Script content. SQL Server queries live target metadata for each imported table. When that table has an identity column, jdbt enables `IDENTITY_INSERT` before insertion and disables it after a successful table import; a failed Database Import closes the target session.

The identity statements and imported rows execute on the same target JDBC session. Explicit Import SQL and Standard Import temporarily select the SQL Server control catalog on that session and restore the original target catalog after success or failure. Operations that cannot assume an existing target database retain a dedicated control connection.

## Offline Standard Import Scripts

The SQL Server-only command is:

```text
jdbt emit-standard-imports [--import <key>] [--output-dir <path>] [--replace]
```

It requires no database credentials. Without `--import`, it uses the configured default Import Definition. It emits every Import Row Source table and every ordered sequence in the selected Database Modules, even when checked-in overrides exist. Each file is:

```text
<output>/<module>/import/<clean-qualified-object-name>.sql
```

Table content is the driver's plain ordered `INSERT ... SELECT` using `__TARGET__` and `__SOURCE__`. Identity statements are deliberately absent because identity is determined from live target metadata when Database Import runs.

The default output is `<database-project>/tmp/imports`. Relative custom output paths resolve from the [Database Project](../glossary/README.md#database-project). A non-empty custom output requires `--replace`. Jdbt validates the canonical destination before staging, rejects a symbolic-link destination, and forbids the filesystem root, Database Project, and all ancestors of the Database Project. It writes a complete staged tree before replacing the destination.

The metadata ownership rationale is recorded in [ADR 0001](../adr/0001-repository-metadata-for-standard-imports.md).
