# T04 — Safe long-lived format watcher

- Status: `pending`
- Blocked by: `T01`
- Spec coverage: `R5`, `R6`; `AC7`, `AC8`

## Delivers

A normal long-lived Java process watches `src/` and `tools/` in the source workspace and sends eligible changed files
through one already-warm formatter thread, while preserving startup safety and surviving filesystem and parser errors.

## Acceptance criteria

- [ ] Startup resolves `BUILD_WORKSPACE_DIRECTORY`, registers existing directories recursively, and does not format or
  rewrite existing files.
- [ ] Create, modify, and editor-style replacement events for `.java` files are coalesced and eventually formatted;
  newly created directories are registered recursively.
- [ ] All formatting is serialized through one formatter instance and a formatter-generated event causes no further
  write when content is already formatted.
- [ ] Admission requires a normalized in-workspace path under `src/` or `tools/`, a `.java` suffix, and no symlink in
  the admitted file/directory path; rejected paths are reported and untouched.
- [ ] Invalid or partially written Java is reported without changing the file or terminating the watcher, and a later
  valid save is formatted normally.
- [ ] Watch-service overflow is reported and triggers a deterministic rescan of all eligible roots.
- [ ] No `--format-existing`, iBazel protocol, external watcher dependency, or concurrent formatter pool is added.

## Validation

- Focused watcher component tests — prove registration, admission, coalescing, error recovery, overflow handling, and
  conditional writes deterministically.
- Real-process watcher exercise with a temporary workspace — proves OS modify/replace events, new directories, invalid
  then valid saves, and absence of a rewrite loop.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `pending`
