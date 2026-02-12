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
| Status command | `runtime.rb#status` | `status` | pending | Report version, schema hash, migration support. |
| Create command | `runtime.rb#create` | `create` | pending | Includes pre/create/finalize/post hooks. |
| Drop command | `runtime.rb#drop` | `drop` | pending | Uses control DB connection behavior. |
| Migrate command | `runtime.rb#migrate` | `migrate` | pending | Migration table semantics preserved. |
| Import command | `runtime.rb#database_import` | `import` | pending | Includes per-table and per-sequence behavior. |
| Create by import | `runtime.rb#create_by_import` | `create-by-import` | pending | Skips create path when resuming partial import. |
| Load dataset | `runtime.rb#load_dataset` | `load-dataset` | pending | Pre/post dataset hooks required. |
| Module group up/down | `runtime.rb#up_module_group`, `#down_module_group` | `up-module-group`, `down-module-group` | pending | Drop order and schema mapping preserved. |
| Dump fixtures | `runtime.rb#dump_database_to_fixtures` | `dump-fixtures` | pending | No live DB required in default tests. |
| Package data | `runtime.rb#package_database_data` | `package-data` | pending | Deterministic packaging required. |

## Data and File Semantics

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Project config parsing | `config.rb`, definition classes | `JdbtProjectConfigLoader` | done | Strict key validation and Ruby-compatible defaults model. |
| Repository config parsing | `repository_definition.rb#from_yaml` | `RepositoryConfigLoader` | done | Supports omap-style list and map-style module declarations. |
| Repository merge order | `runtime.rb#perform_load_database_config` + `repository_definition.rb#merge!` | Config load pipeline | in_progress | Merge semantics implemented; runtime loading integration pending. |
| Index ordering | `runtime.rb#collect_files` | File resolver | pending | Index entries first, then lexical fallback. |
| Duplicate basename rejection | `runtime.rb#collect_files` | File resolver | pending | Error message should identify collisions. |
| Fixture resolution | `runtime.rb#collect_fixtures_from_dirs` | Fixture loader | pending | Strict rejection of unexpected fixture/sql files. |
| Import file strictness | `runtime.rb#import` | Import resolver | pending | Reject unknown files in import dirs. |
| SQL batch splitting | `runtime.rb#run_sql_batch` | SQL executor | pending | `GO` separator behavior preserved. |
| Migration release-cut behavior | `runtime.rb#perform_migration` | Migration engine | pending | Respect `*_Release-<version>.sql` semantics. |

## Intentional Divergences

| Area | Ruby Source | Java Target | Status | Notes |
| --- | --- | --- | --- | --- |
| Template processing | ERB usage in config/fixture loading | No templating | intentional_divergence | Permanent policy: deterministic plain data only. |
| Import resume mechanism | `IMPORT_RESUME_AT` env var | `--resume-at` CLI option | intentional_divergence | Equivalent behavior with explicit CLI input. |
| Module-group delete ordering | TODO noted in `test_runtime_basic.rb` | All deletes before imports | intentional_divergence | Planned behavioral fix. |
| Query command | `runtime.rb#query` helper | No end-user CLI command | intentional_divergence | Removed from Java CLI command surface. |
| Backup/restore command surface | Optional Ruby tasks | Excluded from initial Java delivery | intentional_divergence | Candidate future scope if requested. |
