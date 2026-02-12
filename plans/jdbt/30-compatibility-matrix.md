# jdbt Compatibility Matrix

This matrix tracks parity against Ruby `dbt` implementation and tests.

## Legend

- `pending`: not implemented yet.
- `in_progress`: currently being implemented.
- `done`: implemented and validated with tests.
- `intentional_divergence`: accepted behavior difference.

## Runtime Commands

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Status command | `runtime.rb#status` | `status` | done | Runtime flow and CLI wiring implemented with SQL Server execution path. |
| Create command | `runtime.rb#create` | `create` | done | Runtime flow and CLI wiring implemented with SQL Server execution path. |
| Drop command | `runtime.rb#drop` | `drop` | done | Runtime flow and CLI wiring implemented with SQL Server execution path. |
| Migrate command | `runtime.rb#migrate` | `migrate` | done | Runtime flow and CLI wiring implemented with migration tracking semantics. |
| Import command | `runtime.rb#database_import` | `import` | done | Runtime flow and CLI wiring implemented with --resume-at behavior; PostgreSQL cross-database standard import requires explicit SQL files. |
| Create by import | `runtime.rb#create_by_import` | `create-by-import` | done | Runtime flow and CLI wiring implemented with resume-aware create-skip semantics. |
| Load dataset | `runtime.rb#load_dataset` | `load-dataset` | done | Runtime flow and CLI wiring implemented. |
| Module group up/down | `runtime.rb#up_module_group`, `#down_module_group` | `up-module-group`, `down-module-group` | done | Runtime flow and CLI wiring implemented with reverse-down semantics. |
| Dump fixtures | `runtime.rb#dump_database_to_fixtures` | `dump-fixtures` | pending | No live DB required in default tests. |
| Package data | `runtime.rb#package_database_data` | `package-data` | done | Deterministic ZIP writer, package-data assembly, and CLI wiring implemented. |

## Data and File Semantics

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Project config parsing | `config.rb`, definition classes | `JdbtProjectConfigLoader` | done | Strict key validation with hardcoded runtime defaults (no top-level `defaults` key). |
| Repository config parsing | `repository_definition.rb#from_yaml` | `RepositoryConfigLoader` | done | Supports omap-style list and map-style module declarations. |
| Repository merge order | `runtime.rb#perform_load_database_config` + `repository_definition.rb#merge!` | Config load pipeline | done | Merge semantics implemented and wired into CLI runtime loading pipeline. |
| Index ordering | `runtime.rb#collect_files` | File resolver | done | Index entries first with lexical fallback and runtime integration implemented. |
| Duplicate basename rejection | `runtime.rb#collect_files` | File resolver | done | Duplicate basename checks implemented with diagnostics. |
| Fixture resolution | `runtime.rb#collect_fixtures_from_dirs` | Fixture loader | done | Strict rejection of unexpected fixture/sql files implemented and exercised in runtime flows. |
| Import file strictness | `runtime.rb#import` | Import resolver | done | Unknown import files are rejected in runtime resolver. |
| SQL batch splitting | `runtime.rb#run_sql_batch` | SQL executor | done | `GO` separator behavior implemented in runtime SQL batch execution. |
| Migration release-cut behavior | `runtime.rb#perform_migration` | Migration engine | done | Release-boundary skip/mark behavior implemented in runtime migration flow. |

## Database Drivers

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| SQL Server driver | `drivers/dialect/sql_server.rb` | `SqlServerDbDriver` | done | JDBC-backed execution path with migration and import support methods. |
| PostgreSQL driver | `drivers/dialect/postgres.rb` | `PostgresDbDriver` | done | JDBC-backed execution path with migration support and postgres-specific SQL generation. |

## Intentional Divergences

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Template processing | ERB usage in config/fixture loading | No templating | intentional_divergence | Permanent policy: deterministic plain data only. |
| Import resume mechanism | `IMPORT_RESUME_AT` env var | `--resume-at` CLI option | intentional_divergence | Equivalent behavior with explicit CLI input. |
| Module-group delete ordering | TODO noted in `test_runtime_basic.rb` | All deletes before imports | intentional_divergence | Planned behavioral fix. |
| Query command | `runtime.rb#query` helper | No end-user CLI command | intentional_divergence | Removed from Java CLI command surface. |
| Backup/restore command surface | Optional Ruby tasks | Excluded from initial Java delivery | intentional_divergence | Candidate future scope if requested. |
| PostgreSQL cross-database standard import | SQL Server style cross-db default import SQL | Explicit import SQL required | intentional_divergence | PostgreSQL does not support cross-database table access without extensions. |
| Config defaults declaration | Top-level `defaults` in `config.rb` | Hardcoded runtime defaults | intentional_divergence | `jdbt.yml` only accepts top-level `databases`. |
