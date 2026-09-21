# T01 — Shared formatter and one-shot write

- Status: `pending`
- Blocked by: `None`
- Spec coverage: `R1`, `R2`; `AC1`, `AC2`

## Delivers

A null-marked Java formatter library loads Palantir once through `FormatterService`, and a one-shot frontend discovers
and conditionally formats every workspace-owned Java file below `src/` and `tools/` in one JVM. The existing write
script becomes a thin dispatcher to that frontend.

## Acceptance criteria

- [ ] Capture the current formatter's cold/warm baseline before replacing its invocation path.
- [ ] One formatter instance returns expected Palantir output for deliberately dirty Java without spawning a process.
- [ ] Current repository sources are fixed points and focused dirty formatting/import/reflow/Javadoc cases match the
  established CLI output exactly.
- [ ] The one-shot command uses `BUILD_WORKSPACE_DIRECTORY`, deterministic file ordering, strict `src/` and `tools/`
  scope, and changes only differing non-symlink Java files.
- [ ] `tools/java_format.sh write` dispatches to the new frontend while retaining mode validation.
- [ ] Production and test sources follow directory-owned BUILD rules and share one Starlark definition of Palantir's
  required JDK module exports.

## Validation

- Focused formatter and one-shot Java tests — prove in-process output, equivalence, discovery, and conditional writes.
- `tools/java_format.sh write && git diff --exit-code` — proves the clean repository is already a fixed point.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `pending`
