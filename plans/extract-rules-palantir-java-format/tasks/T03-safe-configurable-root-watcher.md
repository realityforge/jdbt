# T03 — Safe configurable-root watcher

- Status: `complete`
- Blocked by: `T01`
- Spec coverage: `R3`, `R4`; `AC4`

## Delivers

Root alias `//:java_format_watch` watches one or more admitted workspace roots with one warm formatter thread and the
validated startup, event, path, failure, and overflow behavior on the new repository-neutral command surface.

## Acceptance criteria

- [x] The watcher shares T01 root admission, requires explicit roots, registers existing directories recursively, and
  never formats existing files at startup.
- [x] Create, modify, atomic replacement, and newly nested directory events are coalesced and eventually formatted;
  formatter-generated events cause no further writes when content is already formatted.
- [x] Paths outside admitted roots, symbolic links or symlink components, non-Java files, and missing files are rejected
  or ignored as specified without mutating unintended content.
- [x] Invalid/partial Java is reported without changing bytes or terminating the watcher; a later valid save succeeds.
- [x] Overflow is reported and deterministically rescans only admitted roots; filesystem registration errors do not
  terminate the loop when continued operation is safe.
- [x] Tests cover deterministic seams plus real OS events and stable process identity.

## Validation

- Focused watcher tests — prove path safety, conditional writes, recovery, and overflow deterministically.
- Real-process watcher exercise — proves native events, replacements, nested roots, invalid recovery, and reuse.
- `tools/check.sh` — required task gate.

## Evidence

- Rules commit `18b5152` (`feat: add safe configurable-root watcher`).
- The focused JUnit suite passed real native modify, create, atomic replacement, nested-directory registration,
  formatter-event coalescing, invalid-save recovery, startup non-mutation, admitted-root overflow, and unsafe-path cases.
- A real `bazel run //:java_format_watch -- --root=.watch-e2e` process left an existing dirty file untouched at startup,
  formatted later modify and nested-create events, reported invalid Java without changing it, and formatted a later valid
  save.
- `pgrep -fl PalantirJavaFormatWatchMain` reported PID `75684` before and after the event/recovery sequence, proving the
  watcher and its formatter remained warm in one process.
- `tools/check.sh` passed Buildifier, the full build/test set, self-formatting, lock enforcement, and clean tracked-state
  validation for the staged T03 tree.
