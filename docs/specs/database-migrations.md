# Database Migrations Specification

## Purpose

This specification defines how a [Database Project](../glossary/README.md#database-project) identifies and records
migrations in a target database.

## Requirements

1. A migration's base filename is its sole persisted identity within the target database.
2. Migration state must not include a Database Project or database-configuration namespace.
3. When reading migration state created by an older jdbt version, a matching `Migration` value is applied regardless
   of any legacy `Namespace` value.
4. Newly created migration state stores the migration name and application time without a `Namespace` field.
