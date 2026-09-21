# T01 — Shared formatter and one-shot write

- Status: `complete`
- Blocked by: `None`
- Spec coverage: `R1`, `R2`; `AC1`, `AC2`

## Delivers

A null-marked Java formatter library loads Palantir once through `FormatterService`, and a one-shot frontend discovers
and conditionally formats every workspace-owned Java file below `src/` and `tools/` in one JVM. The existing write
script becomes a thin dispatcher to that frontend.

## Acceptance criteria

- [x] Capture the current formatter's cold/warm baseline before replacing its invocation path.
- [x] One formatter instance returns expected Palantir output for deliberately dirty Java without spawning a process.
- [x] Current repository sources are fixed points and focused dirty formatting/import/reflow/Javadoc cases match the
  established CLI output exactly.
- [x] The one-shot command uses `BUILD_WORKSPACE_DIRECTORY`, deterministic file ordering, strict `src/` and `tools/`
  scope, and changes only differing non-symlink Java files.
- [x] `tools/java_format.sh write` dispatches to the new frontend while retaining mode validation.
- [x] Production and test sources follow directory-owned BUILD rules and share one Starlark definition of Palantir's
  required JDK module exports.

## Validation

- Focused formatter and one-shot Java tests — prove in-process output, equivalence, discovery, and conditional writes.
- `tools/java_format.sh write && git diff --exit-code` — proves the clean repository is already a fixed point.
- `tools/check.sh` — required step-completion gate.

## Evidence

- Baseline on this host: after `bazel shutdown`, current CLI check `real 4.95s`; immediate warm CLI check `real 1.52s`.
- `PalantirFormatterTest` invokes Palantir's actual CLI entry point with the former `--palantir --replace` options for
  import cleanup, long-string reflow, and Javadoc fixtures, then compares the resulting files directly with both the
  in-process service output and retained goldens.
- `bazel test //tools/java-format/src/test/java/org/realityforge/jdbt/tools/javaformat:javaformat_tests` — passed.
- Two consecutive `tools/java_format.sh write` runs produced the same complete Java-source digest
  (`1a52a0561c963f0097bdd39e05ff550c99d8f091`).
- `tools/update_java_deps.sh --check` — passed.
- `tools/check.sh` — passed; 10 tests passed, line coverage 89.49%, branch coverage 78.60%.
