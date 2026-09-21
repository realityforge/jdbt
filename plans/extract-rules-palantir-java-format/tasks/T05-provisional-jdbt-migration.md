# T05 — Provisional jdbt hard-cut migration

- Status: `pending`
- Blocked by: `T04`
- Spec coverage: `R8`, `R9`; `AC8`, `AC9`, `AC10`, `AC11`

## Delivers

Jdbt consumes the reviewed sibling module through a temporary local override, retains its developer UX and exact
formatting semantics, removes every local formatter implementation/dependency path, and leaves both repositories green
for prepublication implementation review.

## Acceptance criteria

- [ ] Jdbt declares the external module, temporarily resolves it through `local_path_override`, loads root `defs.bzl`,
  and passes product/test roots plus exact write remediation to its existing `//:java_format_check`.
- [ ] `tools/java_format.sh check|write` retains mode validation and passes explicit `--root=src --root=tools`; README
  documents the external watch alias and no stale local command.
- [ ] `tools/java-format/`, local formatter/protobuf/Maven setup, obsolete depgen script branches, protoc option, and old
  Ahab label are removed or updated according to observed external action identity.
- [ ] Full-corpus fixed-point and deliberate dirty-check/repair exercises match pre-extraction behavior and change no
  unrelated source.
- [ ] Repository/module searches find no local formatter labels, implementation packages, stale generated dependencies,
  or compatibility aliases.
- [ ] Both repository gates pass from clean worktrees, and all local changes are committed before prepublication review.

## Validation

- Jdbt `tools/java_format.sh check|write` plus dirty/repair exercise — proves preserved UX and semantics.
- Jdbt `tools/check.sh` — proves complete migration against the local module.
- Rules `tools/check.sh` — proves the exact candidate repository remains release-ready.
- Git diff/search/module graph inspection — proves hard-cut cleanup and dependency boundaries.

## Evidence

- `pending`
