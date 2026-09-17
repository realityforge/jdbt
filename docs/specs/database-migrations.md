# Database Migrations Specification

## Purpose

This specification defines how a [Database Project](../glossary/README.md#database-project) identifies, bootstraps,
executes, and records [Migrations](../glossary/README.md#migration) in a target database.

## Requirements

1. Migration support is present when the configured `migrationDir` contains ordered `.sql` files; it has no separate
   enablement flag.
2. A Migration's base filename is its sole persisted identity within the target database.
3. [Migration State](../glossary/README.md#migration-state) must store each unique Migration identity, the SHA-256
   checksum of its exact UTF-8 file content, and its application time.
4. Migration State must not include a Database Project or database-configuration namespace. State created by an
   older jdbt version without checksums is incompatible and must be rejected.
5. Jdbt must acquire an exclusive target-database migration lock before inspecting or changing Migration State. A
   concurrent migration operation that cannot acquire the lock must fail without executing a Migration.
6. Database creation must initialize Migration State with every current Migration after structural finalization and
   before post-create hooks, without executing the Migration files, because the newly created schema already represents
   those changes.
7. An explicit migration operation with no Migration files must fail before opening the target database.
8. When Migration State does not exist, jdbt must read the live target database's `DatabaseSchemaVersion`, find the
   matching [Release Migration](../glossary/README.md#release-migration), and atomically initialize Migration State
   through that Release Migration without executing those files. A missing version or Release Migration must fail
   without creating Migration State.
9. After Migration State exists, release markers must not suppress execution. Jdbt must consider every Migration in
   order and execute each unrecorded Migration.
10. A recorded Migration whose current checksum differs from its stored checksum must fail before its SQL executes.
11. Each executed Migration and its Migration State record must commit in the same database transaction or roll back
    together.
12. A Migration failure must stop the operation before any later Migration executes.
