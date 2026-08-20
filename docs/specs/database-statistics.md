# Database Statistics Export

This specification defines jdbt's durable [Database Statistics Export](../glossary/README.md#database-statistics-export) behavior.

## Repository contract

Every table in a [Repository Descriptor](../glossary/README.md#repository-descriptor) has a mandatory `indexes` list. Entries are unique quoted physical index names. Descriptor producers include model-derived primary keys as well as explicitly modeled indexes. [Database Artifact](../glossary/README.md#database-artifact) packaging preserves these identities without loss.

## Command contract

`export-database-statistics` accepts the standard database key, driver, target connection, and target password-source options plus a required `--output` file. SQL Server is supported; other drivers fail before connecting. The target principal needs database-level `VIEW DEFINITION`.

The command issues one SQL Server catalog query. It sums `sys.partitions.rows` by object and index, uses the heap or clustered storage row as the table count, and uses each named index's own aggregate as its index count. The values are approximate and may have small cross-index skew during concurrent database activity. A filtered index therefore reports only its qualifying entries.

## Model validation

The export contains only modeled tables and indexes. Database-only objects are ignored. Before writing output, the command reports modeled objects that are missing, duplicated, disabled, hypothetical, unsupported, partitionless, inaccessible, or have invalid counts. Independent validation failures are collected where practical.

Supported modeled index storage is SQL Server heap, clustered rowstore, and nonclustered rowstore. A new storage type requires explicit collector support; it must not silently emit zero.

## CSV contract

The exact header is:

```text
object_type,schema,table,index,metric,value
```

Identifiers are natural, unquoted database identifiers and are matched case-sensitively to Repository Metadata. Tables are ordered by schema and table. Each table row precedes its indexes, which are alphabetically ordered. Table rows have an empty `index`; every row uses the metric `approximate_row_count` and a non-negative integer value.

Output is deterministic escaped CSV encoded as UTF-8 with LF endings. It contains no connection metadata, credentials, summaries, or capture timestamp.

## File safety

The complete query, validation, and render finish before replacement. The command creates missing parent directories, writes a sibling temporary file, and atomically replaces the required output path. It does not fall back to a non-atomic move. Query, validation, rendering, or move failure preserves an existing output file and closes the database connection. Success prints one concise line identifying the row count and output path.

The metadata ownership decision is recorded in [ADR 0002](../adr/0002-model-database-statistics-through-repository-metadata.md).
