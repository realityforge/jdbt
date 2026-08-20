# ADR 0002: Model Database Statistics through Repository Metadata

- Status: Accepted
- Date: 2026-08-18

## Context

Developers need useful table and index cardinalities when their workstations cannot access production. A live catalog query can discover physical objects, but exporting every object would allow DBA-added or experimental indexes to enter a source-controlled artifact and would not detect a modeled index missing from a database.

Domgen already owns Rose's intended SQL table and index identities. Jdbt already loads and merges [Repository Metadata](../glossary/README.md#repository-metadata), owns database-driver connections, and packages metadata into [Database Artifacts](../glossary/README.md#database-artifact).

## Decision

Repository Metadata is the static inventory for [Database Statistics Export](../glossary/README.md#database-statistics-export):

- every table descriptor contains a mandatory list of quoted physical index names;
- producers include model-derived primary-key indexes and explicitly modeled indexes;
- Database Artifact packaging preserves the list;
- jdbt queries live driver metadata, validates every modeled identity, and ignores unmodeled objects; and
- jdbt owns validation, deterministic CSV rendering, connection cleanup, and atomic replacement behind one command.

Only index identity belongs in the descriptor for this capability. Definitions such as keys, included columns, uniqueness, filters, and clustering remain outside the contract until a schema-definition use case requires them.

## Consequences

- Exported statistics describe the intended architecture while retaining live approximate cardinalities.
- A missing or unusable modeled index fails visibly instead of disappearing from output.
- Repository Descriptor producers must emit primary keys explicitly even when their DDL generator synthesizes them separately.
- The table-object descriptor shape is a hard contract; omitted index lists are rejected rather than treated as legacy metadata.
- Driver-specific catalog and permission behavior remains in jdbt rather than project wrappers.

## Rejected alternatives

**Export every live index.** Rejected because database-only objects would make the artifact environment-dependent and missing modeled indexes would be invisible.

**Generate a project-specific Java collector.** Rejected because it would duplicate jdbt's connection, descriptor-loading, packaging, and command infrastructure.

**Serialize complete index definitions now.** Rejected because row-count export needs identity only; broader metadata would enlarge the contract without serving the current behavior.
