# T04 — Safe long-lived format watcher

- Status: `complete`
- Blocked by: `T01`
- Spec coverage: `R5`, `R6`; `AC7`, `AC8`

## Delivers

A normal long-lived Java process watches `src/` and `tools/` in the source workspace and sends eligible changed files
through one already-warm formatter thread, while preserving startup safety and surviving filesystem and parser errors.

## Acceptance criteria

- [x] Startup resolves `BUILD_WORKSPACE_DIRECTORY`, registers existing directories recursively, and does not format or
  rewrite existing files.
- [x] Create, modify, and editor-style replacement events for `.java` files are coalesced and eventually formatted;
  newly created directories are registered recursively.
- [x] All formatting is serialized through one formatter instance and a formatter-generated event causes no further
  write when content is already formatted.
- [x] Admission requires a normalized in-workspace path under `src/` or `tools/`, a `.java` suffix, and no symlink in
  the admitted file/directory path; rejected paths are reported and untouched.
- [x] Invalid or partially written Java is reported without changing the file or terminating the watcher, and a later
  valid save is formatted normally.
- [x] Watch-service overflow is reported and triggers a deterministic rescan of all eligible roots.
- [x] No `--format-existing`, iBazel protocol, external watcher dependency, or concurrent formatter pool is added.

## Validation

- Focused watcher component tests — prove registration, admission, coalescing, error recovery, overflow handling, and
  conditional writes deterministically.
- Real-process watcher exercise with a temporary workspace — proves OS modify/replace events, new directories, invalid
  then valid saves, and absence of a rewrite loop.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `bazel test //tools/java-format/src/test/java/org/realityforge/jdbt/tools/javaformat:javaformat_tests` — passed,
  including startup non-mutation, real watch-service events, invalid-then-valid recovery, atomic replacement, nested
  directory registration, symlink/outside rejection, conditional writes, and overflow recovery.
- `bazel run //tools/java-format:java_format_watch` — live workspace exercise formatted a Java file created in a new
  directory, survived and reported malformed Java, formatted the recovered save and an atomic replacement, and left
  the post-format modification time stable for one second.
- `bazel build //:java_format_check` — passed with the watcher sources included in the self-hosting aggregate.
- `tools/check.sh` — passed; 10 tests passed, line coverage 89.49%, branch coverage 78.60%.
