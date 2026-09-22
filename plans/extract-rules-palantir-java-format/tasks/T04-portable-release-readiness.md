# T04 — Portable consumer, CI, and release readiness

- Status: `complete`
- Blocked by: `T02`, `T03`
- Spec coverage: `R5`, `R6`, `R7`, `R9`; `AC1`, `AC5`, `AC6`, `AC7`, `AC11`, `AC14`

## Delivers

An independent smoke consumer, complete cross-platform gates, public documentation, deterministic release packaging,
provenance workflow, and BCR-ready metadata make the local rules repository ready for prepublication review.

## Acceptance criteria

- [x] A standalone `e2e/smoke` module uses `local_path_override`, loads only root public API/aliases, carries its own
  independent Maven extension, and passes its permanently clean check target.
- [x] Disposable smoke copies prove dirty-check failure/repair, repeated-root writes, root inclusion/exclusion, watcher
  events/recovery, symlink safety, and public visibility without leaving dirty checked-in fixtures.
- [x] CI defines Java 17 lanes for Bazel 8 and 9 across Linux, macOS x86/arm64, and Windows, plus Java 21 and 25 lanes
  for Bazel 9 on Linux, with no allowed failures; local/available lanes pass before prepublication review.
- [x] README documents Bzlmod setup, public API, explicit roots, worker `.bazelrc`, pre-BCR archive override,
  troubleshooting, the bounded Java 17/21/25 compatibility matrix, later-JDK policy, and pre-1.0 policy without
  documenting private labels.
- [x] `tools/check.sh` covers buildifier, source formatting, unit tests, smoke steady state and negative exercises, Maven
  lock integrity, release archive reproducibility/layout, `.bcr` templates, and a clean diff.
- [x] `.bcr` templates contain valid maintainer/repository/test metadata and open no PR; release automation builds the
  named deterministic archive and provenance, and future Publish-to-BCR automation targets its current reusable flow.
- [x] Repository metadata/configuration files are complete and no debug, jdbt-specific, temporary, or disconnected
  compatibility path remains.

## Validation

- `tools/check.sh` under locally available supported JDK/Bazel combinations — proves local release readiness and smoke
  consumption before the complete hosted matrix.
- CI/workflow and `.bcr` schema checks — prove matrix/release/registry configuration before publication.
- Two archive builds plus checksum/tree comparison and extracted smoke run — prove deterministic consumable packaging.

## Evidence

- Rules commits `00a4773` (`feat: prepare portable formatter release`), `2813316` (`fix: normalize smoke lockfile
  mode`), and `a178244` (`fix: enforce reviewed release readiness`) add the isolated consumer, portability correction,
  CI, documentation, archive tooling, provenance release, and BCR templates.
- Rules commit `8b5d302` (`fix: make release gates cross-platform`) replaces the buildifier macro's broken
  manifest-runfiles Windows wrapper with the pinned executable's direct check mode and avoids GNU tar `SIGPIPE` under
  `pipefail` by validating a materialized archive manifest.
- The first consumer build exposed two hidden generated-protocol dependencies: Bazel's internal worker proto leaked an
  unresolved `grpc-java` mapping, and a locally generated replacement invoked native protobuf tooling that failed
  under current Xcode. A focused private codec now implements Bazel's canonical protobuf worker wire fields directly;
  root protocol tests and real worker actions pass without protobuf or consumer repository configuration.
- `e2e/smoke/test.sh` passed clean public target builds and disposable dirty-check immutability/remediation, repeated
  explicit-root repair, excluded-root preservation, live watcher startup/modify/invalid-recovery/new-directory events,
  symlink safety, and private-load rejection. The fixture's unrelated Guava Maven extension remains independently
  locked.
- The complete `tools/check.sh` gate passed locally on macOS arm64 with Java 17/Bazel 9.2, Java 21/Bazel 9.2, Java
  25/Bazel 9.2, and Java 17/Bazel 8.4. Bazel 8 uses `--lockfile_mode=off` because its Bzlmod lock schema is incompatible
  with Bazel 9; strict Maven locking and tracked-state validation remain enabled on every lane.
- The CI workflow defines ten required, non-allowed-failure lanes: Java 17 with Bazel 8.4/9.2 on Ubuntu, macOS Intel,
  macOS arm64, and Windows, plus Java 21/25 with Bazel 9.2 on Ubuntu. Hosted results remain a T06 publication gate.
- Hosted run `35666545537` exposed the two cross-platform gate defects above: both Windows lanes failed resolving the
  buildifier executable from generated runfiles, while all Ubuntu lanes failed after successful build/test/smoke work
  when GNU tar received a closed pipeline. The formatter tests and four macOS lanes passed. Both fixes then passed the
  rules and jdbt full local gates before another review round.
- Replacement hosted run `35667646527` passed all eight Ubuntu and macOS lanes and advanced both Windows lanes through
  buildifier and compilation before `javaformat_tests` timed out waiting for a forward-slash watcher diagnostic. The
  watcher rendered in-workspace relative paths with the host separator, so Windows emitted `source\\Existing.java`
  while the public diagnostic contract and tests expected `source/Existing.java`. Candidate `cebe463` normalizes only
  in-workspace displayed paths to forward slashes and adds `--test_output=errors` to the full gate so any remaining
  hosted test failure is self-diagnosing; both rules and jdbt full local gates pass against that exact commit.
- Hosted run `35668711929` confirmed the normalized watcher diagnostics: `javaformat_tests` passed in both Windows
  lanes, and all eight Ubuntu/macOS lanes passed. Both Windows lanes then failed only when the smoke trap removed its
  temporary workspace: killing the `bazel run` client left the Java watcher holding the directory open. Candidate
  `884ddb0` builds and runs the watcher executable directly, supplies the workspace environment explicitly, reads the
  validated native Windows PID from MSYS/Cygwin `/proc` before terminating the launcher process tree, and emits watcher
  logs on timeout. Focused smoke plus both full local gates pass against that exact candidate.
- Hosted run `35671696952` passed all eight Ubuntu/macOS jobs and both Windows formatter test suites, but both Windows
  smoke jobs again failed only at final workspace removal. GitHub Actions then identified the surviving orphan as a
  Java process, proving that the validated `/proc` PID belonged to the launcher rather than the watcher JVM. Candidate
  `3144866dbae4193cff8820241421b59293f2dd05` instead resolves the exact watcher main class through the installed JDK's
  `jps`, normalizes native Windows CRLF output, rejects zero or multiple matches, and terminates that native JVM PID
  before waiting for the shell launcher. Focused CRLF parsing and smoke plus both full local gates pass against that
  exact candidate; hosted Windows validation remains a T06 publication gate.
- Hosted run `35673248105` passed all eight Ubuntu/macOS jobs and both Windows formatter test suites. Both Windows smoke
  jobs still failed only when removing the disposable workspace, even after terminating the exact watcher JVM; GitHub
  again cleaned an orphan Java process afterward. This isolates the remaining handles to cleanup context, including the
  disposable Bazel server and the shell's current directory. Candidate `cc381c37b5fa3606f6822fe72382e0f2089c8b31`
  shuts down that workspace's Bazel server, changes to the stable repository root, and only then removes the temporary
  tree. Focused smoke plus both full local gates pass against that exact candidate; hosted Windows validation remains a
  T06 publication gate.
- Hosted run `35674045753` passed all eight Ubuntu/macOS jobs and both Windows formatter test suites. Both Windows lanes
  also passed the original disposable-smoke cleanup, proving candidate `cc381c37b5fa3606f6822fe72382e0f2089c8b31`
  closes that workspace correctly. They failed only later when `tools/test_release.sh` removed the extracted archive's
  `e2e/smoke` tree while its Bazel server still held it open. Candidate
  `ecf8be859af554be628e1f6feeed06e3f9ad2a7e` applies the same workspace-specific Bazel shutdown, stable-root change,
  and removal ordering to release-archive verification. The focused release test and both full local gates pass
  against that exact candidate; hosted Windows validation remains a T06 publication gate.
- Hosted run `35675040878` passed nine of ten jobs, including both complete Windows lanes, proving candidate
  `ecf8be859af554be628e1f6feeed06e3f9ad2a7e` closes both disposable Bazel workspaces correctly. The sole failure was
  Java 17/Bazel 9.2 on macOS x86_64: its two archives of the same staged tree differed because `git archive` gives a raw
  tree object the current time, and the two fast invocations only appeared deterministic when they happened within one
  clock second. The failure reproduces locally when the invocations are separated by two seconds. Candidate
  `fbd857332d0a9993fa50d66ccfcf361658e448d2` assigns every archive entry the fixed Unix-epoch timestamp and deliberately
  separates the two regression-test builds by one second. The focused release test and both full local gates pass
  against that exact candidate; hosted matrix validation remains a T06 publication gate.
- Hosted run `35676075377` passed all ten required Java/Bazel/platform jobs on exact reviewed commit
  `fbd857332d0a9993fa50d66ccfcf361658e448d2`, including both Windows lanes and the macOS x86_64 lane that exposed the
  raw-tree timestamp defect. This closes the hosted portability gate without reducing the accepted matrix.
- The setup-bazel action is pinned to commit `8cb04a772ab4c1eb984e9c1b493a182e96c5e425`, verified as tag `0.19.0`.
  Release preflight requires the semantic-version tag, explicitly reviewed SHA, and current `origin/main` to be the
  same commit, then queries the successful push CI run and requires each of the ten named matrix jobs exactly once.
- Independent raw protobuf-wire golden vectors cover canonical worker requests/responses, every supported unknown-field
  wire type, truncated framing, oversized messages, and malformed length varints without round-tripping the codec.
- The gate built two byte-identical `rules_palantir_java_format-v0.1.0.tar.gz` archives from the staged tree, verified
  the sole `rules_palantir_java_format-0.1.0/` prefix, extracted one, and built its smoke consumer and public formatter.
- `yq` parsed all CI/release/publish and BCR YAML; Python parsed both JSON templates; the pinned release workflow is
  `release_ruleset.yaml@v7.7.0`, the dormant manual BCR workflow is `publish.yaml@v1.5.0`, and no BCR PR was opened.
- `rg` found no jdbt identity, temporary path, debug marker, or extra public Starlark symbol in the rules repository;
  `git diff --check` and the staged clean-diff gate passed.
