# T05 — Integrated UX, evidence, and full gate

- Status: `complete`
- Blocked by: `T03`, `T04`
- Spec coverage: `R1`, `R3`, `R4`, `R5`, `R7`, `R8`, `R9`; `AC2`, `AC3`, `AC6`, `AC7`, `AC10`,
  `AC11`, `AC12`

## Delivers

The three developer workflows are documented and exercised together, transient performance/reuse evidence is recorded,
all obsolete formatter machinery is removed where fully replaced, and the complete repository gate establishes a clean
handoff candidate.

## Acceptance criteria

- [x] Developer documentation identifies `tools/java_format.sh check`, `tools/java_format.sh write`, and
  `bazel run //tools/java-format:java_format_watch` with their check/write/watch semantics.
- [x] Obsolete script enumeration/parameter-file logic and unused formatter targets are removed only when no remaining
  caller needs them; generated dependency sections remain generator-owned.
- [x] Timings record the old cold formatter baseline plus first and subsequent worker requests and watcher formats.
- [x] Evidence independently proves worker and watcher process reuse; timing alone is not treated as proof.
- [x] Clean, deliberately dirty, repaired, invalid-watcher-input, and repeat-clean scenarios all behave as specified,
  with every temporary source edit restored.
- [x] No domain spec/ADR, iBazel support, multiplexing, remote-execution promise, arbitrary formatter abstraction,
  permanent benchmark artifact, or downstream formatter API has entered the diff.
- [x] The final worktree preserves unrelated user files and contains no debug logging, scratch files, experimental
  changes, or stale generated output.

## Validation

- Run and record the three documented workflows and their negative/recovery scenarios — proves integrated behavior.
- Inspect `git diff --check`, generated-dependency verification, and the complete diff — proves cleanup and scope.
- `tools/check.sh` — strongest required implementation and handoff gate.

## Evidence

- The former CLI baseline was 4.95 seconds cold and 1.52 seconds warm. After a Bazel restart and one invalidated
  target, the first worker-backed build took 4.14 seconds overall with a 0.54-second critical path; two subsequent
  invalidated requests took 0.25 and 0.24 seconds.
- Bazel reported creation of one singleplex `PalantirJavaFormat` worker, and its OS PID remained `17485` across both
  subsequent invalidated requests. The watcher PID remained `17595` while its first format completed in 1.83 seconds
  and subsequent formats completed in 0.94 and 1.14 seconds.
- `tools/java_format.sh check` passed cleanly, then failed a deliberately unformatted `ConfigException.java` with the
  workspace-relative path and write-command remediation while preserving checksum
  `03db593ae80c1517bcbca01971beed3aba1c3f766412c436d24680e4df2ba13c`.
- `tools/java_format.sh write` repaired that source; a second write preserved the complete Java-tree checksum
  `f7a21bea40edc55f71757a31684d6ce9b13024f8fb63f4768f6dd866c638607f`, and the following check passed.
- The T04 live watcher exercise covered invalid-input survival and recovery, replacement saves, new directories, and
  no rewrite loop; all temporary probe files and timing edits were removed afterward.
- Repository search found no remaining caller of the old `//tools/java-format:palantir_java_format` CLI target, so it
  was removed; `bazel query //tools/java-format:all` confirmed only the replacement frontends and worker remain.
- The full diff contains development tooling and README changes only; no domain spec/ADR or permanent benchmark
  artifact was added, and unrelated `.bazelbsp/` and `.idea/` state remains untouched.
- `tools/check.sh` — passed; 10 tests passed, line coverage 89.49%, branch coverage 78.60%.
