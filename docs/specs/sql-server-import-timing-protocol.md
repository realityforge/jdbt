# SQL Server Import Timing Protocol Specification

## Purpose

This specification defines the versioned interoperability contract through which a SQL Server
[Database Project](../glossary/README.md#database-project) provides nested observations to jdbt's
[Database Import Timing](database-import-timing.md).

## Requirements

1. Before timed import SQL runs, jdbt must set read-only session-context key `jdbt.import.timing` to protocol value
   `jdbt.timing.v1` on the target connection.
2. Jdbt must install `[dbo].[tblImportTiming]` in the target database and reject a concurrent timed operation against
   that database.
3. Successful timing must be drained and cleared while `[dbo].[tblImportTiming]` remains installed. Timing recovered
   after a database failure must remain available for diagnosis.
4. SQL may contribute terminal timing only through the ordered columns `Protocol`, `Ordinal`, `OperationId`,
   `ParentOperationId`, `Kind`, `Status`, and `ElapsedMicroseconds`, whose version 1 JDBC types are respectively
   `VARCHAR`, `BIGINT`, `VARCHAR`, `VARCHAR`, `VARCHAR`, `VARCHAR`, and `BIGINT`.
5. The SQL protocol version must remain independent of the public NDJSON schema version.
6. Jdbt must recognize a protocol result regardless of its position among other JDBC results and update counts. A
   result whose first column is `Protocol` must have the complete allowlisted shape and `jdbt.timing.v1` value; other
   result sets must be closed without reading row values. A database project may omit protocol results.
7. An unsupported protocol version, malformed shape or value, duplicate identity, non-contiguous ordinal, invalid
   kind, status, elapsed time, identity, or parent must fail the timed command.
8. SQL timing rows must use terminal postorder and ordinals beginning at one within the emitting SQL batch.
9. The sole Analysis root must be `phase/final-validation`. Its internal parent token
   `__JDBT_ACTIVE_SQL_BATCH__` must be replaced with the active `sql_batch` identity and must not appear in public
   output.
10. Analysis operation identities must be `phase/final-validation`, `phase/corruption-checks`,
    `analysis-corruption/<owner>/<check-key>`, `phase/constraint-checks`, `module/constraint-check/<schema>`, and
    `analysis-constraint/<direct-key>`, using canonical percent encoding and phase-defined parentage.
