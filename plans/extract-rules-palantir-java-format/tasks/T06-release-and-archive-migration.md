# T06 — Public v0.1.0 and released-archive migration

- Status: `complete`
- Blocked by: `T05`, prepublication review
- Spec coverage: `R6`, `R7`, `R8`, `R9`, `R10`; `AC6`, `AC7`, `AC10`, `AC11`, `AC12`, `AC14`

## Delivers

The reviewed rules repository is public, its required GitHub matrix is green, immutable `v0.1.0` and provenance are
published, and jdbt replaces the provisional local override with the exact integrity-pinned release archive.

## Acceptance criteria

- [x] Prepublication implementation review reports `Findings: none` and records the exact approved rules commit SHA
  before the GitHub repository is created or pushed.
- [x] Public `realityforge/rules_palantir_java_format` is created on `main` with the accepted description/features and
  reviewed commits; repository CI completes the full required matrix successfully.
- [x] Repository immutable releases are enabled before publication; issues are enabled and wiki/projects are disabled.
- [x] The release tag points to the recorded reviewer-approved SHA; any tracked CI-driven fix reruns both local gates and
  receives another findings-free round from the same reviewer before tagging.
- [x] Release workflow publishes deterministic immutable `v0.1.0`, expected archive/prefix and provenance; independent
  download verifies integrity and extracted smoke consumption.
- [x] No BCR PR or registry fork is created; templates and future automation remain present for later use.
- [x] Jdbt replaces only `local_path_override` with `bazel_dep` 0.1.0 plus exact release `archive_override`, strip prefix,
  and SRI integrity; no sibling checkout participates in its final builds.
- [x] Both full gates pass after release substitution, worktrees are clean except preserved jdbt user state, and the
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
  jdbt full gate pass locally. Same-reviewer round 7 reported `Findings: none` and approved that exact commit before it
  was pushed or tagged, retaining only hosted Bazel 8.4/9.2 Windows confirmation as residual risk. A fully green hosted
  matrix remains required before tagging.
- Hosted run `35671696952` passed all eight Ubuntu/macOS jobs and both Windows formatter test suites. Both Windows smoke
  jobs reached final cleanup and failed because the watcher JVM still held its workspace; GitHub Actions subsequently
  reported the orphan Java process. No tag or release was created. Same-reviewer round 8 rejected the first `jps`
  candidate because native Windows CRLF could leave a carriage return on the exact main-class field. Corrected candidate
  `3144866dbae4193cff8820241421b59293f2dd05` resolves the exact watcher main class with the installed JDK's `jps`,
  normalizes carriage returns, rejects zero or multiple matches, and terminates the native JVM PID before waiting for
  the shell launcher. Focused CRLF parsing and smoke, the exact rules full gate, and the jdbt full gate pass locally.
  Same-reviewer round 9 reported `Findings: none` and approved that exact commit for hosted validation, retaining only
  the two hosted Windows lanes as residual risk. The complete hosted matrix remains the release gate before tagging.
- Hosted run `35673248105` passed all eight non-Windows jobs and both Windows formatter test suites. Both Windows smoke
  jobs again failed only at final disposable-workspace removal, despite successful exact watcher-JVM termination; the
  Actions runner then cleaned an orphan Java process. No tag or release was created. Candidate
  `cc381c37b5fa3606f6822fe72382e0f2089c8b31` shuts down the disposable workspace's Bazel server, leaves that directory,
  and only then removes the temporary tree. Focused smoke, the exact rules full gate, and the jdbt full gate pass
  locally. Same-reviewer round 10 reported `Findings: none` and approved that exact commit for hosted validation,
  retaining only the two hosted Windows lanes as residual risk. The complete hosted matrix remains the release gate
  before tagging.
- Hosted run `35674045753` passed all eight non-Windows jobs, both Windows formatter test suites, and the original
  disposable-smoke cleanup, confirming round 10's fix. Both Windows lanes failed only in the later release-archive
  verification when its extracted `e2e/smoke` workspace was removed with a Bazel server still holding the directory.
  No tag or release was created. Candidate `ecf8be859af554be628e1f6feeed06e3f9ad2a7e` mirrors the proven cleanup in
  `tools/test_release.sh`: it shuts down that exact extracted consumer with the configured Bazel startup options,
  returns to the stable repository root, and then removes the release-test tree. The focused release test, exact rules
  full gate, and jdbt full gate pass locally. Same-reviewer round 11 reported `Findings: none` and approved that exact
  commit for hosted validation, retaining only the two hosted Windows lanes as residual risk. The complete hosted matrix
  remains the release gate before tagging.
- Hosted run `35675040878` passed nine of ten jobs, including both complete Windows lanes, resolving round 11's residual
  handle-cleanup risk. The only failure was Java 17/Bazel 9.2 on macOS x86_64, where the two supposedly deterministic
  archives differed. No tag or release was created. The archive test uses a staged raw tree object; `git archive`
  timestamps such entries with the invocation time, so back-to-back builds passed only when they fell in one clock
  second. A two-second-separated local reproduction produced different bytes. Candidate
  `fbd857332d0a9993fa50d66ccfcf361658e448d2` fixes the producer with a fixed Unix-epoch entry timestamp and adds a
  one-second separation that makes the regression test deterministic. The focused release test, exact rules full gate,
  and jdbt full gate pass locally. Same-reviewer round 12 reported `Findings: none` and approved that exact commit for
  hosted validation, retaining only macOS x86_64 confirmation of archive equality as residual risk. The complete hosted
  matrix remains the release gate before tagging.
- Hosted run `35676075377` passed all ten required jobs on reviewed commit
  `fbd857332d0a9993fa50d66ccfcf361658e448d2`, including both Windows lanes and Java 17/Bazel 9.2 on macOS x86_64. The
  annotated `v0.1.0` tag and remote `main` both peel to that exact commit. Guarded release run `35676536245` passed
  reviewed-candidate preflight, build, Sigstore/SLSA provenance attestation, and release publication.
- GitHub reports release `v0.1.0` immutable. Independent download produced SHA-256
  `f56b55c21a5a4c80e7c3adec39b83f198ed654890a1d88c5150fa3a40f065b56` and SRI
  `sha256-9WtVwhpaTIDnw63sObg/GY7WVIkKHYjFFQ+jpA8GW1Y=`; it contains only the
  `rules_palantir_java_format-0.1.0/` prefix. Offline provenance verification binds the artifact to the pinned
  `bazel-contrib/.github` reusable workflow, exact source SHA, and `refs/heads/main`. The extracted standalone smoke
  consumer built its public formatter targets successfully.
- Jdbt now declares version `0.1.0` with an `archive_override` for the exact release URL, prefix, and SRI. With the
  sibling checkout physically moved away, a fresh-output-base query resolved only the public formatter and watcher
  aliases and the complete jdbt gate passed. The sibling was restored only after verification. BCR search found no
  matching PR, and no `realityforge/bazel-central-registry` fork exists. Same-reviewer final archive-substitution review
  round 13 reported `Findings: none`, found no residual risk in the substitution, and approved the exact `MODULE.bazel`
  and evidence delta for commit and publication.
