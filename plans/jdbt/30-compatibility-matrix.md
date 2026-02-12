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
| Status command | `runtime.rb#status` | `status` | in_progress | Runtime status implemented; CLI integration pending. |
| Create command | `runtime.rb#create` | `create` | in_progress | Core orchestration implemented with fixture load support. |
| Drop command | `runtime.rb#drop` | `drop` | in_progress | Control-connection drop flow implemented. |
| Migrate command | `runtime.rb#migrate` | `migrate` | in_progress | Runtime migrate flow implemented; CLI wiring and full parity checks pending. |
| Import command | `runtime.rb#database_import` | `import` | in_progress | Runtime import sequencing and resume behavior implemented; CLI wiring pending. |
| Create by import | `runtime.rb#create_by_import` | `create-by-import` | in_progress | Runtime flow implemented with resume-aware create-skip semantics. |
| Load dataset | `runtime.rb#load_dataset` | `load-dataset` | in_progress | Core dataset orchestration implemented; CLI wiring pending. |
| Module group up/down | `runtime.rb#up_module_group`, `#down_module_group` | `up-module-group`, `down-module-group` | in_progress | Core up/down orchestration implemented with reverse-down semantics. |
| Dump fixtures | `runtime.rb#dump_database_to_fixtures` | `dump-fixtures` | pending | No live DB required in default tests. |
| Package data | `runtime.rb#package_database_data` | `package-data` | in_progress | Deterministic ZIP writer and package-data assembly implemented; CLI wiring pending. |

## Data and File Semantics

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Project config parsing | `config.rb`, definition classes | `JdbtProjectConfigLoader` | done | Strict key validation and Ruby-compatible defaults model. |
| Repository config parsing | `repository_definition.rb#from_yaml` | `RepositoryConfigLoader` | done | Supports omap-style list and map-style module declarations. |
| Repository merge order | `runtime.rb#perform_load_database_config` + `repository_definition.rb#merge!` | Config load pipeline | in_progress | Merge semantics implemented; runtime loading integration pending. |
| Index ordering | `runtime.rb#collect_files` | File resolver | in_progress | Index entries first, then lexical fallback implemented; runtime integration pending. |
| Duplicate basename rejection | `runtime.rb#collect_files` | File resolver | in_progress | Duplicate basename checks implemented in resolver with diagnostics. |
| Fixture resolution | `runtime.rb#collect_fixtures_from_dirs` | Fixture loader | in_progress | Strict rejection of unexpected fixture/sql files implemented in resolver. |
| Import file strictness | `runtime.rb#import` | Import resolver | in_progress | Unknown import files now rejected in runtime resolver; broader parity checks pending. |
| SQL batch splitting | `runtime.rb#run_sql_batch` | SQL executor | pending | `GO` separator behavior preserved. |
| Migration release-cut behavior | `runtime.rb#perform_migration` | Migration engine | in_progress | Release-boundary skip/mark behavior implemented in runtime migration flow. |

## Intentional Divergences

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Template processing | ERB usage in config/fixture loading | No templating | intentional_divergence | Permanent policy: deterministic plain data only. |
| Import resume mechanism | `IMPORT_RESUME_AT` env var | `--resume-at` CLI option | intentional_divergence | Equivalent behavior with explicit CLI input. |
| Module-group delete ordering | TODO noted in `test_runtime_basic.rb` | All deletes before imports | intentional_divergence | Planned behavioral fix. |
| Query command | `runtime.rb#query` helper | No end-user CLI command | intentional_divergence | Removed from Java CLI command surface. |
| Backup/restore command surface | Optional Ruby tasks | Excluded from initial Java delivery | intentional_divergence | Candidate future scope if requested. |
