# T03 — Safe configurable-root watcher

- Status: `in_progress`
- Blocked by: `T01`
- Spec coverage: `R3`, `R4`; `AC4`

## Delivers

Root alias `//:java_format_watch` watches one or more admitted workspace roots with one warm formatter thread and the
validated startup, event, path, failure, and overflow behavior on the new repository-neutral command surface.

## Acceptance criteria

- [ ] The watcher shares T01 root admission, requires explicit roots, registers existing directories recursively, and
  never formats existing files at startup.
- [ ] Create, modify, atomic replacement, and newly nested directory events are coalesced and eventually formatted;
  formatter-generated events cause no further writes when content is already formatted.
- [ ] Paths outside admitted roots, symbolic links or symlink components, non-Java files, and missing files are rejected
  or ignored as specified without mutating unintended content.
- [ ] Invalid/partial Java is reported without changing bytes or terminating the watcher; a later valid save succeeds.
- [ ] Overflow is reported and deterministically rescans only admitted roots; filesystem registration errors do not
  terminate the loop when continued operation is safe.
- [ ] Tests cover deterministic seams plus real OS events and stable process identity.

## Validation

- Focused watcher tests — prove path safety, conditional writes, recovery, and overflow deterministically.
- Real-process watcher exercise — proves native events, replacements, nested roots, invalid recovery, and reuse.
- `tools/check.sh` — required task gate.

## Evidence

- `pending`
