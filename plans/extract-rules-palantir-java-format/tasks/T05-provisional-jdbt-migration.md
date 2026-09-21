# T05 — Provisional jdbt hard-cut migration

- Status: `complete`
- Blocked by: `T04`
- Spec coverage: `R8`, `R9`; `AC8`, `AC9`, `AC10`, `AC11`

## Delivers

Jdbt consumes the reviewed sibling module through a temporary local override, retains its developer UX and exact
formatting semantics, removes every local formatter implementation/dependency path, and leaves both repositories green
for prepublication implementation review.

## Acceptance criteria

- [x] Jdbt declares the external module, temporarily resolves it through `local_path_override`, loads root `defs.bzl`,
  and passes product/test roots plus exact write remediation to its existing `//:java_format_check`.
- [x] `tools/java_format.sh check|write` retains mode validation and passes explicit `--root=src --root=tools`; README
  documents the external watch alias and no stale local command.
- [x] `tools/java-format/`, local formatter/protobuf/Maven setup, obsolete depgen script branches, protoc option, and old
  Ahab label are removed or updated according to observed external action identity.
- [x] Full-corpus fixed-point and deliberate dirty-check/repair exercises match pre-extraction behavior and change no
  unrelated source.
- [x] Repository/module searches find no local formatter labels, implementation packages, stale generated dependencies,
  or compatibility aliases.
- [x] Both repository gates pass from clean worktrees, and all local changes are committed before prepublication review.

## Validation

- Jdbt `tools/java_format.sh check|write` plus dirty/repair exercise — proves preserved UX and semantics.
- Jdbt `tools/check.sh` — proves complete migration against the local module.
- Rules `tools/check.sh` — proves the exact candidate repository remains release-ready.
- Git diff/search/module graph inspection — proves hard-cut cleanup and dependency boundaries.

## Evidence

- Jdbt commit `bb162d0` (`build: consume shared Java formatter rules`) consumes sibling rules commit `2813316` through
  `bazel_dep` plus `local_path_override`, loads only root `defs.bzl`, preserves root `//:java_format_check`, and passes
  exact remediation `tools/java_format.sh write`.
- `tools/java_format.sh invalid` retained exit code 2 and its usage diagnostic. `check` built the external check rule;
  `write` invoked the external root alias with `--root=src --root=tools`; README now gives the corresponding external
  watcher command.
- SHA-256 inventory of every tracked Java file was identical before and after a clean one-shot write. A deliberately
  misformatted `src/main/java/org/realityforge/jdbt/Main.java` made `check` fail with its relative path and exact repair
  command, its dirty hash remained unchanged, and `write` restored the original hash.
- The complete `tools/java-format/` tree, formatter-generated MODULE block, direct protobuf dependency and protoc flag,
  formatter depgen branch, and local targets/tests were deleted. Searches outside historical plan evidence found no
  local formatter package, target, dependency file, protobuf label, or stale command.
- Ahab initially reported the new worker as an unknown module program. Its reproducibility spec now uses Ahab's
  observed normalized external identity
  `@rules_palantir_java_format//java_format/private/java/org/realityforge/rules/palantirjavaformat/palantir_java_format_worker`;
  `//tools/ahab:hermeticity.check` passes with the existing empty baseline.
- `tools/update_java_deps.sh --check` passed after reducing generation to the product dependency catalog. The Bzlmod
  graph contains direct `rules_palantir_java_format@_`; its own isolated Maven graph supplies formatter dependencies.
- Jdbt `tools/check.sh` passed Buildifier, depgen, Ahab, external format check, 58 builds, 9 tests, and coverage at
  89.49% line/78.60% branch. The sibling rules `tools/check.sh` independently passed its complete unit, smoke, watcher,
  lock, metadata, and extracted-archive gate after the migration.
- Both repositories are clean at their tracked commits; jdbt's unrelated `.bazelbsp/` and `.idea/` remain untracked and
  untouched.
