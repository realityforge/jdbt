# T06 — Public v0.1.0 and released-archive migration

- Status: `in_progress`
- Blocked by: `T05`, prepublication review
- Spec coverage: `R6`, `R7`, `R8`, `R9`, `R10`; `AC6`, `AC7`, `AC10`, `AC11`, `AC12`, `AC14`

## Delivers

The reviewed rules repository is public, its required GitHub matrix is green, immutable `v0.1.0` and provenance are
published, and jdbt replaces the provisional local override with the exact integrity-pinned release archive.

## Acceptance criteria

- [x] Prepublication implementation review reports `Findings: none` and records the exact approved rules commit SHA
  before the GitHub repository is created or pushed.
- [ ] Public `realityforge/rules_palantir_java_format` is created on `main` with the accepted description/features and
  reviewed commits; repository CI completes the full required matrix successfully.
- [ ] Repository immutable releases are enabled before publication; issues are enabled and wiki/projects are disabled.
- [ ] The release tag points to the recorded reviewer-approved SHA; any tracked CI-driven fix reruns both local gates and
  receives another findings-free round from the same reviewer before tagging.
- [ ] Release workflow publishes deterministic immutable `v0.1.0`, expected archive/prefix and provenance; independent
  download verifies integrity and extracted smoke consumption.
- [ ] No BCR PR or registry fork is created; templates and future automation remain present for later use.
- [ ] Jdbt replaces only `local_path_override` with `bazel_dep` 0.1.0 plus exact release `archive_override`, strip prefix,
  and SRI integrity; no sibling checkout participates in its final builds.
- [ ] Both full gates pass after release substitution, worktrees are clean except preserved jdbt user state, and the
  same reviewer confirms the final archive-driven changes with no findings.

## Validation

- GitHub required-check inspection — proves the promised matrix completed on the published commit.
- `gh release`/API plus independent archive download, integrity, tree, and smoke checks — proves immutable release state.
- Jdbt module graph/query and `tools/check.sh` with the sibling unavailable — proves real archive consumption.
- Rules `tools/check.sh` — confirms source repository remains identical to the released candidate.

## Evidence

- Implementation review round 2 reported `Findings: none` after rechecking all round-1 corrections and approved exact
  rules commit `a17824473bedba10fb530e831fe4e190ed67c56d`. At the time of approval the public GitHub repository did not exist and
  the local rules repository had no remote, so no unreviewed state had been published.
- The public repository was created at `https://github.com/realityforge/rules_palantir_java_format`, with `main`, the
  accepted description, issues enabled, wiki/projects disabled, and immutable releases verified enabled before any
  tag. Initial hosted run `35666545537` found Windows buildifier-runfiles and GNU tar `pipefail` portability defects;
  release remained blocked and no tag was created.
- Rules commit `8b5d302` corrects both hosted failures. The rules full gate and jdbt full gate passed against that exact
  local candidate. Same-reviewer round 3 reported `Findings: none` and approved exact commit
  `8b5d30285011baad90fe91f66acb98a96290429f` before it was pushed or tagged; the replacement hosted matrix remains the
  release gate.
- Hosted run `35667646527` passed all eight Ubuntu and macOS jobs. Both Windows jobs passed buildifier and compilation,
  then failed `javaformat_tests` after one watcher-await timeout because successful and error diagnostics rendered
  workspace-relative paths with Windows backslashes instead of the public forward-slash form. No tag or release was
  created. Rules candidate `cebe4635344f249c5fd9deef5c84587d8c6ce8a3` routes all in-workspace watcher diagnostics
  through a normalized forward-slash display and adds `--test_output=errors` to hosted tests. The exact rules full gate
  and jdbt full gate pass locally. Same-reviewer round 4 reported `Findings: none` and approved that exact commit before
  it was pushed or tagged; the reviewer retained only the explicit residual risk that both hosted Windows lanes must
  confirm the normalized diagnostics. A fully green replacement hosted matrix remains required before tagging.
- Hosted run `35668711929` passed all eight non-Windows jobs and both Windows `javaformat_tests`, resolving round 4's
  residual product risk. The two Windows jobs failed later because the smoke test killed the `bazel run` client rather
  than the Java watcher process, so `rm` reported its temporary workspace as busy. No tag or release was created.
  Same-reviewer round 5 rejected the first teardown candidate because it passed Bash's MSYS/Cygwin PID directly to
  native `taskkill`, which could miss or terminate the wrong process tree and then wait indefinitely. Corrected
  Round 6 then found Git for Windows' `ps` does not support the selected output option. Corrected candidate
  `884ddb006414a1265407870d13a7d564db5fda02` runs the already-built watcher executable directly, reads and validates
  its native Windows PID from MSYS/Cygwin `/proc/<pid>/winpid`, requires successful process-tree termination before
  waiting, and bounds lookup or termination failure without waiting. Focused smoke, the exact rules full gate, and the
  jdbt full gate pass locally; same-reviewer reapproval and a fully green hosted matrix remain required before tagging.
