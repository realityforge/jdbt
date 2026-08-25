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

## Structured import timing

The `import` and `create-by-import` commands accept optional `--timing-output <path>` timing. Without this option, jdbt
must not create timing output or change its existing human-readable output. An absolute output path is used directly;
a relative path resolves from the [Database Project](../glossary/README.md#database-project). Jdbt creates missing
parent directories and opens or truncates the output before database mutation.

The output is one UTF-8 newline-delimited JSON document per terminal operation. One file represents one invocation,
and each completed line must be flushed so a failed invocation retains a valid prefix. Every version 1 document has
these required fields in deterministic order:

- integer `schema_version` with value `1`;
- one-based integer `sequence` in terminal-observation order;
- string `operation_id`;
- string or null `parent_operation_id`;
- string `kind`;
- `status` with value `succeeded` or `failed`; and
- nonnegative integer `elapsed_microseconds`.

Consumers of a supported schema version must tolerate additional fields. Removing a required field or changing its
meaning, units, identity semantics, or enum semantics requires a schema-version increment.

Operation identities are stable across equivalent executions and use typed paths composed from canonical logical
names. Components that would conflict with path syntax are percent-encoded. Identities must not contain runtime
parameters, connection metadata, physical filesystem paths, or SQL values. Parentage is defined only by
`parent_operation_id`; consumers must not infer it by splitting `operation_id`.

The stable operation kinds are `command`, `phase`, `module`, `table_clear`, `table_transfer`, `sequence_transfer`,
`maintenance`, `sql_directory`, `sql_file`, `sql_batch`, `analysis_corruption_check`, and
`analysis_constraint_check`. Timing covers only executed operations. A resumed Database Import emits its executed
suffix with the same operation identities and emits no synthetic skipped observations.

Elapsed time uses a monotonic clock and integer microseconds. Observation sequence is deterministic terminal order,
not a wall-clock timeline, and parent elapsed time may include child time and incidental overhead. Consumers must not
sum nested elapsed times.

Timing output must contain no credentials, connection metadata, SQL text, parameters, source or target data, exception
messages, or physical filesystem paths. A timing failure must fail an otherwise successful timed command. When the
database operation already failed, that failure remains primary and timing failures may be attached only as secondary
diagnostic context. After a timed SQL Server execution failure, jdbt must attempt to recover database-local timing on
the same target session before release. An unusable session or secondary drain, validation, output, or cleanup failure
must not prevent failed Java operations from being observed or replace the primary database error. A nested SQL
operation that fails before returning its result is represented by its enclosing failed observations rather than a
synthetic leaf observation.

For a timed SQL Server import, jdbt sets the read-only session-context key `jdbt.import.timing` to the internal protocol
value `jdbt.timing.v1` on the target connection before import SQL runs. It installs `[dbo].[tblImportTiming]` in the
target database and rejects a concurrent timed operation against that database. Successful timing is drained and
cleared while the table remains installed; timing recovered after a database failure remains available for diagnosis.
SQL may return terminal timing rows only with the exact ordered columns `Protocol`, `Ordinal`, `OperationId`,
`ParentOperationId`, `Kind`, `Status`, and `ElapsedMicroseconds` and their version 1 JDBC types. The internal protocol
version is independent of the public NDJSON schema version.

Jdbt traverses every JDBC result and update count during timed SQL Server execution. A result is considered timing only
when its first column is the `Protocol` marker; jdbt then requires the complete allowlisted shape and
`jdbt.timing.v1` value. Other result sets are closed without advancing their cursor or reading any row value. Missing
timing results are valid for database projects that do not implement this protocol. An unsupported version, malformed
shape or value, duplicate identity, non-contiguous ordinal, invalid kind, status, elapsed time, identity, or parent
fails the timed command.

SQL timing rows are terminal postorder. Their ordinals start at one within the emitting SQL batch. The sole Analysis
root is `phase/final-validation`; its internal parent token `__JDBT_ACTIVE_SQL_BATCH__` is replaced with the active
`sql_batch` identity before NDJSON is written and must never appear in public output. The accepted Analysis identities
are `phase/final-validation`, `phase/corruption-checks`, `analysis-corruption/<owner>/<check-key>`,
`phase/constraint-checks`, `module/constraint-check/<schema>`, and `analysis-constraint/<direct-key>`, with canonical
percent encoding and the parentage defined by those phases.

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
