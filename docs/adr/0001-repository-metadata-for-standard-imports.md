# ADR 0001: Repository Metadata supplies offline Standard Import generation

- Status: Accepted
- Date: 2026-08-11

## Context

Jdbt owns database-driver behavior and [Database Import](../glossary/README.md#database-import) orchestration. Offline [Standard Import Script](../glossary/README.md#standard-import-script) generation needs ordered table columns and row-ownership classifications but must not connect to a live database. In Rose, Domgen already owns those static SQL model facts.

The earlier scalar-table [Repository Descriptor](../glossary/README.md#repository-descriptor) did not carry enough information to generate [Standard Imports](../glossary/README.md#standard-import) offline. Reimplementing SQL generation in Domgen or Rose would duplicate driver behavior. Querying a database would make a source-generation command credential-dependent and would not describe packaged [Database Artifacts](../glossary/README.md#database-artifact).

## Decision

The [Repository Metadata](../glossary/README.md#repository-metadata) contract is the seam between the static model producer and jdbt:

- each Repository Descriptor table carries its qualified name, ordered quoted SQL columns, physical index identities, and [Row Source](../glossary/README.md#row-source);
- Domgen emits those facts from its SQL model;
- jdbt merges them from pre-artifacts, the local [Database Project](../glossary/README.md#database-project), and post-artifacts;
- Database Artifact packaging serializes the merged model without loss; and
- jdbt uses its SQL Server driver to generate Standard Import Scripts from that model.

Identity seed, increment, and identity-column details are not Repository Metadata requirements for emission. Live target metadata remains authoritative for identity handling during Database Import.

## Consequences

- Standard Import Script emission is deterministic, offline, and credential-free.
- Packaged Database Artifacts are sufficient inputs for downstream emission.
- Driver SQL remains in jdbt, while Domgen remains the source of static schema facts.
- Repository Descriptor producers and consumers use the cohesive table-object form; there is no scalar-table compatibility path.
- Row Source changes affect data lifecycle but do not independently change jdbt's physical schema hash.

## Rejected alternatives

**Generate scripts in Domgen or Rose.** Rejected because it duplicates jdbt driver SQL and does not naturally compose Database Artifacts.

**Discover columns from a live database.** Rejected because it requires credentials, makes output depend on external state, and cannot represent an artifact before deployment.

**Embed SQL Server identity statements in emitted files.** Rejected because identity handling depends on live target metadata and session state and must apply consistently to [Import Fixtures](../glossary/README.md#import-fixture), [Explicit Import SQL](../glossary/README.md#explicit-import-sql), and Standard Import.
