# Supply offline Standard Import generation through Repository Metadata

[Repository Metadata](../glossary/README.md#repository-metadata) is the boundary between the static model producer and
jdbt's database-driver behavior. Domgen supplies the schema facts; jdbt composes them across
[Database Artifact](../glossary/README.md#database-artifact)s and owns driver-specific
[Standard Import Script](../glossary/README.md#standard-import-script) generation, keeping emission offline and
credential-free without duplicating SQL generation in each consumer.

We rejected live-database discovery because it requires credentials and cannot describe an artifact before deployment.
Generating SQL in Domgen or Rose would duplicate jdbt's driver behavior and bypass artifact composition. Identity
handling remains a runtime driver responsibility because it depends on the live target and must apply consistently to
every [Database Import](../glossary/README.md#database-import) [Row Source](../glossary/README.md#row-source).

The [Database Imports Specification](../specs/database-imports.md) owns the descriptor shape, composition, row ownership,
and import behavior.
