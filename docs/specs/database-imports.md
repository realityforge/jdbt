# Database Imports

This specification defines jdbt's durable row-ownership, [Database Import](../glossary/README.md#database-import), and [Standard Import Script](../glossary/README.md#standard-import-script) behavior. Opt-in timing is defined by the [Database Import Timing Specification](database-import-timing.md).

## Repository contract

Each table in a [Repository Descriptor](../glossary/README.md#repository-descriptor) is an object with:

- a qualified `name`;
- a non-empty, ordered list of unique quoted SQL `columns`; and
- a mandatory ordered list of unique quoted physical SQL `indexes`; and
- an optional `rowSource`, whose values are `import` and `deployment` and whose default is `import`.

The descriptor's `modules` value is a plain YAML map. Ordered-map tags and list-shaped module maps are invalid. YAML row-data files also reject ordered-map tags; ordinary mappings retain insertion order.

[Database Module](../glossary/README.md#database-module), table, column, and sequence order are significant. [Repository Metadata](../glossary/README.md#repository-metadata) is composed module-atomically in pre-artifact, local-project, then post-artifact order. [Database Artifact](../glossary/README.md#database-artifact) packaging must preserve the complete merged model.

## Resource resolution

Jdbt has one local [Resource Root](../glossary/README.md#resource-root). For a given logical path, local files take precedence over post-artifacts, which take precedence over pre-artifacts. Resolution produces a typed [Database Resource](../glossary/README.md#database-resource); downstream operations must not infer its origin by parsing a path string. Artifact indexes participate in ordering without changing that precedence.

## Row Source behavior

An [Import Row Source](../glossary/README.md#import-row-source) table participates in Database Import. Within the selected [Import Definition](../glossary/README.md#import-definition) its row input precedence is:

1. [Import Fixture](../glossary/README.md#import-fixture);
2. [Explicit Import SQL](../glossary/README.md#explicit-import-sql);
3. [Standard Import](../glossary/README.md#standard-import).

A [Deployment Row Source](../glossary/README.md#deployment-row-source) table is established by deployment assets, an external deployment mechanism, or deliberate absence of rows. Database Import must not delete it, import it, select it through `--resume-at`, or emit a Standard Import Script for it. A deployment asset is not required merely to justify this classification.

[Dataset Fixtures](../glossary/README.md#dataset-fixture) are independent of [Row Source](../glossary/README.md#row-source). They load only after an operator explicitly selects a dataset and may target either Row Source.

## Contradictory inputs

Before a create-by-import flow mutates a database, jdbt rejects selected-project contradictions:

- an [Initial Fixture](../glossary/README.md#initial-fixture) targeting an Import Row Source table;
- an Import Fixture or Explicit Import SQL targeting a Deployment Row Source table;
- both an Import Fixture and Explicit Import SQL targeting the same table;
- ordinary and late import assets targeting the same table;
- a configured required import file that is absent from the resolved import plan;
- a normal Import Row Source table after a late table has started within the same Database Module; or
- `--resume-at` naming a Deployment Row Source table.

Diagnostics identify the Import Definition or asset and the affected database object.

Before opening the target connection, jdbt resolves the selected modules, tables, sequences, and override resources once into an immutable import plan. Validation and execution consume that same plan. An unknown `--resume-at` value and conflicting fixture/SQL overrides for either a table or sequence therefore fail before database mutation.

## Contribution and late-import lifecycle

`contributionDirs` is an optional ordered database-level list. Each directory contains [Database Contribution](../glossary/README.md#database-contribution) SQL and is independent of any Import Definition. Contributions execute:

- after schema-up during fresh creation;
- after dataset fixtures and post-dataset hooks during dataset-backed creation;
- after ordinary imports and `postImportDirs` during import-backed creation; and
- before structural finalizers.

Migration never executes contributions.

An Import Definition may set ordered `preLateImportDirs`, one `lateImportDir`, and `requiredFiles`. A table with an explicit SQL or YAML resource in `lateImportDir` is removed from the ordinary phase and imported exactly once in the late phase. Late tables retain Repository Descriptor order. Within each selected Database Module, late Import Row Source tables must form a suffix of that module's imported tables; this rejects an ordinary table that would be moved ahead of an earlier deferred predecessor without adding a second dependency model. Every path in `requiredFiles` must resolve to a hook, fixture, or SQL override selected by the immutable import plan; an absent required file rejects the import before the target connection opens. Assets not required by an Import Definition remain available to other definitions that share the same resource directories.

The import-backed phase order is:

1. pre-import hooks;
2. ordinary tables and sequences;
3. post-import hooks;
4. Database Contributions;
5. import-only pre-late hooks;
6. Late Imports;
7. database import maintenance; and
8. structural finalizers and post-create hooks for create-by-import.

Create-by-import is jdbt's only source-to-target import command. `--no-create` leaves the database itself in place but
still executes the create-by-import schema, import, contribution, finalization, and post-create lifecycle against it.

`--resume-at` is recovery for the actual table or sequence whose import failed. It is valid only when the source,
target, Database Artifacts, configuration, filters, and all work before the named frontier are unchanged. It is not a
general partial-import selector and jdbt does not prove that an arbitrary target is a valid checkpoint.

An ordinary-table resume skips the already completed ordinary prefix, clears and retries the named object and ordinary
suffix, then executes post-import hooks, Database Contributions, pre-late hooks, Late Imports, maintenance, and creation
finalization. A late-table resume skips pre-import hooks, ordinary transfers, post-import hooks, Database Contributions,
pre-late hooks, and completed late tables. It clears and retries the named late table and remaining late suffix, then
executes maintenance and creation finalization. Contributions therefore execute once during a valid create-by-import
lifecycle rather than being replayed by late recovery.

All configured contribution, pre-late, and late resources participate in project hashing and Database Artifact packaging. Projects that omit these optional keys retain the ordinary import lifecycle.

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

The default output is `<database-project>/tmp/imports`. Relative custom output paths resolve from the Database Project. A non-empty custom output requires `--replace`. Jdbt validates the canonical destination before staging, rejects a symbolic-link destination, and forbids the filesystem root, Database Project, and all ancestors of the Database Project. It writes a complete staged tree before replacing the destination.

The metadata ownership rationale is recorded in [ADR 0001](../adr/0001-repository-metadata-for-standard-imports.md).
