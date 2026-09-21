# T05 — Integrated UX, evidence, and full gate

- Status: `pending`
- Blocked by: `T03`, `T04`
- Spec coverage: `R1`, `R3`, `R4`, `R5`, `R7`, `R8`, `R9`; `AC2`, `AC3`, `AC6`, `AC7`, `AC10`,
  `AC11`, `AC12`

## Delivers

The three developer workflows are documented and exercised together, transient performance/reuse evidence is recorded,
all obsolete formatter machinery is removed where fully replaced, and the complete repository gate establishes a clean
handoff candidate.

## Acceptance criteria

- [ ] Developer documentation identifies `tools/java_format.sh check`, `tools/java_format.sh write`, and
  `bazel run //tools/java-format:java_format_watch` with their check/write/watch semantics.
- [ ] Obsolete script enumeration/parameter-file logic and unused formatter targets are removed only when no remaining
  caller needs them; generated dependency sections remain generator-owned.
- [ ] Timings record the old cold formatter baseline plus first and subsequent worker requests and watcher formats.
- [ ] Evidence independently proves worker and watcher process reuse; timing alone is not treated as proof.
- [ ] Clean, deliberately dirty, repaired, invalid-watcher-input, and repeat-clean scenarios all behave as specified,
  with every temporary source edit restored.
- [ ] No domain spec/ADR, iBazel support, multiplexing, remote-execution promise, arbitrary formatter abstraction,
  permanent benchmark artifact, or downstream formatter API has entered the diff.
- [ ] The final worktree preserves unrelated user files and contains no debug logging, scratch files, experimental
  changes, or stale generated output.

## Validation

- Run and record the three documented workflows and their negative/recovery scenarios — proves integrated behavior.
- Inspect `git diff --check`, generated-dependency verification, and the complete diff — proves cleanup and scope.
- `tools/check.sh` — strongest required implementation and handoff gate.

## Evidence

- `pending`
