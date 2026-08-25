# Database Import Timing Specification

## Purpose

This specification defines jdbt's opt-in structured timing output for a
[Database Import](../glossary/README.md#database-import). Database-provided observations use the separate
[SQL Server Import Timing Protocol Specification](sql-server-import-timing-protocol.md).

## Requirements

1. The `import` and `create-by-import` commands must enable timing only when passed `--timing-output <path>`. Without
   that option, jdbt must not create timing output or change its human-readable output.
2. An absolute timing path must be used directly, while a relative path must resolve from the
   [Database Project](../glossary/README.md#database-project). Jdbt must create missing parent directories and open or
   truncate the output before database mutation.
3. One invocation must emit one UTF-8 newline-delimited JSON file containing one document per terminal operation. Each
   completed document must be flushed so a failed invocation retains a valid prefix.
4. Every version 1 document must contain, in deterministic order, integer `schema_version` value `1`, one-based integer
   `sequence`, string `operation_id`, string or null `parent_operation_id`, string `kind`, `status` value `succeeded` or
   `failed`, and nonnegative integer `elapsed_microseconds`.
5. Consumers of a supported schema version must tolerate additional fields. Removing a required field or changing its
   meaning, units, identity semantics, or enum semantics requires a schema-version increment.
6. Operation identities must remain stable across equivalent executions and use typed paths composed from canonical
   logical names. Conflicting path components must be percent-encoded, and parentage must be determined only by
   `parent_operation_id`.
7. Operation identities must not contain runtime parameters, connection metadata, physical filesystem paths, or SQL
   values.
8. The stable operation kinds are `command`, `phase`, `module`, `table_clear`, `table_transfer`, `sequence_transfer`,
   `maintenance`, `sql_directory`, `sql_file`, `sql_batch`, `analysis_corruption_check`, and
   `analysis_constraint_check`.
9. Timing must cover only executed operations. A resumed Database Import must emit its executed suffix with the same
   operation identities and no synthetic skipped observations.
10. Elapsed time must use a monotonic clock and integer microseconds. Sequence must represent deterministic terminal
    order rather than a wall-clock timeline; parent elapsed time may include child time and incidental overhead, so
    consumers must not sum nested elapsed times.
11. Timing output must contain no credentials, connection metadata, SQL text, parameters, source or target data,
    exception messages, or physical filesystem paths.
12. A timing failure must fail an otherwise successful timed command. When the database operation has already failed,
    it must remain primary and timing failures may appear only as secondary diagnostic context.
13. After a timed SQL Server execution failure, jdbt must attempt to recover database-local timing on the same target
    session before release. An unusable session or secondary drain, validation, output, or cleanup failure must not
    prevent failed Java operations from being observed or replace the primary database error.
14. A nested SQL operation that fails before returning its result must be represented by its enclosing failed
    observations rather than a synthetic leaf observation.
