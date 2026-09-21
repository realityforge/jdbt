# Extract Rules Palantir Java Format Spec

## Source

- Shared understanding: completed five-round `$grill-me` conversation explicitly confirmed by the user on
  2026-09-21.
- Repository evidence: the formatter delivery on jdbt `main`, `tools/java-format/`, `tools/java_format.sh`, root
  `java_format_check`, worker/watch tests, `.bazelrc`, Ahab policy, module dependency graph, and full jdbt gate.
- External evidence: current Bazel Central Registry policies and schemas, Bazel rules deployment guidance,
  `bazel-contrib/rules-template`, `publish-to-bcr`, RealityForge repository/release conventions, and disposable Bzlmod
  prototypes for rules_jvm_external and Bazel's worker protocol.

## Problem

Jdbt owns a complete Palantir Java Format integration that is useful to other Bazel repositories but is coupled to
jdbt's labels, package names, source roots, dependency generator, documentation, and release lifecycle. Copying that
implementation would duplicate a persistent worker, watcher, dependency closure, safety behavior, and validation in
every consumer, while leaving no independently versioned rules module suitable for the Bazel Central Registry.

## Required outcome

`realityforge/rules_palantir_java_format` is a public, independently tested Bzlmod repository with an immutable
`v0.1.0` release. It provides a small stable API for target-granular checks and safe explicit-root write/watch commands,
passes its promised Bazel/Java/platform matrix, and contains the metadata and automation needed for a later BCR
submission. Jdbt consumes the released archive with an integrity-pinned override, retains its existing developer
commands, removes the local formatter implementation, passes its full gate, and is pushed upstream.

## Scope

- In scope: a new public GitHub repository; Apache-2.0 licensing and Peter Donald copyright notice; Bzlmod module and
  isolated pinned Maven closure;
  shared formatter, one-shot writer, persistent worker check, filesystem watcher, focused tests, independent consumer
  smoke module, CI matrix, deterministic release archive and provenance, `.bcr` templates, future Publish-to-BCR
  workflow, immutable `v0.1.0`, jdbt migration through an archive override, cleanup, review, and publication.
- Out of scope: opening the BCR pull request; legacy WORKSPACE support; Bazel 7; formatter abstraction; alternative
  format styles; exclusion globs; multiplex workers; remote-execution guarantees; Stardoc; IDE-specific integration;
  compatibility shims for jdbt's local labels; and changes to jdbt database behavior.

## Constraints

- The public module and repository are named `rules_palantir_java_format`; upstream `MODULE.bazel` contains neither a
  version nor a compatibility level, while the first GitHub release is `0.1.0`.
- The only intended public API is root `defs.bzl` symbol `java_format_check` plus root executable aliases
  `//:java_format` and `//:java_format_watch`. Implementation targets, provider, aspect, worker, Maven labels, and Java
  packages remain private.
- Write and watch require one or more repeatable, workspace-relative `--root=PATH` arguments. Roots are normalized,
  sorted, deduplicated, in-workspace, non-symlink directories. No implicit `src` or `tools` roots exist.
- Formatting is Palantir style only. Rules releases pin the formatter version; consumers cannot select it in `0.1.0`.
- Check actions treat source files as immutable and write only declared markers. Direct target ownership and traversal
  remain limited to `deps`, `runtime_deps`, `exports`, and `tests`; generated and external Java are excluded.
- The worker is deterministic, protobuf-based, singleplex, reusable, cancellation-aware, and capable of ordinary local
  fallback. Consumers opt into worker reuse through documented mnemonic-specific `.bazelrc` settings.
- The watcher preserves startup non-mutation, one formatting thread, conditional writes, debouncing, recursive new
  directory registration, invalid-input recovery, symlink/outside rejection, and overflow rescan behavior.
- The dependency baseline is rules_jvm_external 7.1, rules_java 9.9.0, and Palantir Java Format 2.93.0. A uniquely
  named, locked Maven repository is internal to the module and requires no consumer configuration. The worker uses a
  small repository-owned codec for Bazel's canonical protobuf wire contract rather than compiling Bazel-internal proto
  targets in consumers.
- The supported JDK set for `0.1.0` is Java 17, 21, and 25. CI runs Java 17 across Bazel 8/9 on Linux, macOS x86/arm64,
  and Windows, plus focused Bazel 9 Linux lanes for Java 21 and 25. Later JDK versions are unsupported until added to
  CI. Release is blocked unless every required lane passes; failures are fixed rather than silently narrowing scope.
- The release archive is deterministic and named `rules_palantir_java_format-v0.1.0.tar.gz`, with source provenance and
  a stable strip prefix. The actual BCR submission is deferred; automated BCR publication applies to later releases.
- The GitHub repository belongs to the personal `realityforge` account, uses `main`, and lists Peter Donald
  `<peter@realityforge.org>` (`realityforge`, GitHub user id `11840`) as maintainer.
- The repository preserves `Copyright 2026 Peter Donald` in a root `NOTICE` alongside the unmodified Apache-2.0
  license.
- Jdbt's existing `.bazelbsp/` and `.idea/` directories are unrelated user state and remain untouched.
- Both repositories are reviewed locally before irreversible publication. Jdbt uses a local override until the rules
  release exists, then switches to an integrity-pinned GitHub archive before its final review and push.
- The release tag must identify the exact rules commit approved by the implementation reviewer. Any tracked change
  made to resolve GitHub CI after review invalidates that approval and requires another findings-free round before
  tagging or publishing.

## Requirements

- `R1`: The rules module must expose one documented target-level Java format check whose actions are immutable,
  target-granular, cacheable, and capable of reusing one formatter worker process.
- `R2`: The module must expose a one-shot command that formats Java beneath explicit safe roots in deterministic order,
  using one in-process formatter and writing only changed files.
- `R3`: The module must expose a long-running command that safely formats eligible saves beneath explicit roots without
  formatting at startup or launching a formatter JVM for each event.
- `R4`: All three workflows must produce Palantir 2.93.0 output equivalent to its established CLI behavior and share
  one repository-neutral Java implementation.
- `R5`: A consumer must be able to use the module through Bzlmod without configuring formatter Maven artifacts,
  protobuf repositories, or internal labels.
- `R6`: The repository must provide an independent consumer fixture and automated evidence for its public API, negative
  behavior, dependency isolation, and promised Bazel/Java/platform matrix.
- `R7`: The repository must be releasable as a deterministic, provenance-bearing GitHub archive and contain valid
  metadata/templates for a later BCR submission.
- `R8`: Jdbt must consume the released module without changing its check/write developer interface, and must remove its
  local formatter code and obsolete dependency-generation/configuration paths.
- `R9`: Jdbt and the rules repository must each retain one exact, comprehensive gate that passes before publication and
  handoff.
- `R10`: Public repository state, release state, archive integrity, branch governance, and both upstream branches must
  match the reviewed delivery with no hidden or temporary implementation path.

## Acceptance criteria

- `AC1` (`R1`): An anonymous smoke consumer loads `java_format_check` from root `defs.bzl`; clean sources pass, a dirty
  direct source fails with its workspace-relative path and configured remediation, no source is modified, and repair
  passes.
- `AC2` (`R1`): Action inspection proves one `PalantirJavaFormat` marker action per direct Java owner, traversal only on
  the accepted edges, source-only immutable inputs, ordinary local fallback, and worker protocol requirements; process
  evidence proves reuse without multiplexing.
- `AC3` (`R2`, `R4`): `//:java_format` rejects missing/absolute/outside/symlink roots, formats only Java below repeated
  valid roots, changes only differing content, and matches both the Palantir CLI and retained goldens for representative
  import, Javadoc, and long-string cases.
- `AC4` (`R3`): A real-process watcher test proves startup non-mutation, modify and replacement formatting, root
  inclusion/exclusion, new directories, no rewrite loop, invalid-then-valid recovery, symlink rejection, and overflow
  recovery while one process remains alive.
- `AC5` (`R5`): A consumer with its own unrelated Maven extension builds the rules module's public targets without
  importing or configuring the module's uniquely named pinned Maven repository; drift from its checked lock fails.
- `AC6` (`R6`): Root and smoke gates pass with Java 17 under Bazel 8 and 9 on Linux, macOS x86/arm64, and Windows, plus
  Java 21 and 25 under Bazel 9 on Linux; CI contains no allowed-failure lane for this bounded matrix.
- `AC7` (`R7`): `.bcr` metadata, source, and presubmit templates validate; the release workflow produces a deterministic
  archive with the expected prefix and integrity, uploads provenance, and publishes immutable public `v0.1.0` only
  after the full matrix succeeds.
- `AC8` (`R8`): Jdbt retains `tools/java_format.sh check|write` and `//:java_format_check`, passes explicit `src` and
  `tools` roots to external binaries, retains its exact remediation, and documents the external watcher command.
- `AC9` (`R8`): Jdbt removes `tools/java-format/`, its formatter Maven/protobuf dependency setup, obsolete depgen and
  Ahab references, and all local formatter labels; repository search finds no disconnected compatibility path.
- `AC10` (`R8`, `R10`): Jdbt first passes against a local override, then against `bazel_dep` plus an integrity-pinned
  `archive_override` for the public release; the released archive—not a sibling checkout—is used by its final gate.
- `AC11` (`R9`): The rules repository's `tools/check.sh` passes formatting, buildifier, unit, smoke, lockfile, and release
  layout checks; jdbt's exact `tools/check.sh` passes after migration.
- `AC12` (`R10`): Independent implementation review passes before rules publication and confirms the final release
  substitution before jdbt publication; upstream `main` branches contain the reviewed commits and jdbt preserves
  unrelated `.bazelbsp/` and `.idea/` state.
- `AC13` (`R7`, `R10`): Repository metadata is public and correct, issues are enabled, wiki/projects disabled, and
  PR-plus-CI branch protection is enabled after bootstrap without blocking the initial release.
- `AC14` (`R7`): No BCR PR is opened. The README states the pre-BCR archive override and the pre-1.0 compatibility policy,
  while later BCR automation remains configured for subsequent releases.

## Significant decisions

| ID | Decision | Rationale | Impact | User verification |
| --- | --- | --- | --- | --- |
| `D1` | Extract check, write, and watch together into one Palantir-specific module. | They share semantics and solve complementary startup/feedback costs without needing a generic framework. | A larger first release, but one formatter implementation and one compatibility matrix. | Exercise all three workflows from the smoke consumer and jdbt. |
| `D2` | Use Bzlmod only and publish the concise module name `rules_palantir_java_format`. | The module targets BCR and new consumers; legacy WORKSPACE support adds a second dependency path. | Bazel 8/9 consumers use one modern setup; older consumers are unsupported. | Inspect MODULE and consumer examples for no WORKSPACE path. |
| `D3` | Keep the root API small and hide the raw aspect/provider/worker. | Pre-1.0 can evolve internals without creating unnecessary compatibility obligations. | Advanced consumers cannot directly compose the aspect in `0.1.0`. | Load only root `defs.bzl` and use the two root aliases. |
| `D4` | Require explicit safe roots for write/watch. | A reusable module cannot assume jdbt's filesystem layout or safely mutate an implicit scope. | Consumers add root arguments; startup and mutation scope are unsurprising. | Verify missing and unsafe roots fail without writes. |
| `D5` | Pin formatter dependencies inside a uniquely named rules_jvm_external repository. | It is BCR-native, reproducible, and isolated from consumer Maven graphs. | Module releases own Palantir upgrades and a checked lockfile. | Build alongside an independent consumer Maven install. |
| `D6` | Implement the required fields of Bazel's canonical protobuf worker wire contract in a small private codec. | The original generated-protocol design leaked Bazel-internal repository mappings and forced consumer-side native protobuf tooling; the bounded codec removes both portability failures without exposing protocol internals. | No protobuf dependency is required; protocol parsing is covered directly, including IDs, cancellation, EOF, unknown fields, and malformed input boundaries. | Build from the independent module on macOS and run focused protocol tests plus real persistent-worker actions. |
| `D7` | Promise Bazel 8/9, Java 17/21/25, and all principal desktop platforms at `0.1.0`. | BCR consumers expect portable rules, while a bounded JDK set makes compatibility executable rather than aspirational. | Java 17 covers every Bazel/platform combination; focused Bazel 9 Linux lanes cover Java 21/25; later JDKs require a future CI addition. | Require every named lane to pass before release. |
| `D8` | Publish immutable `v0.1.0` before BCR submission. | The user wants a real reusable release now while retaining explicit control over registry submission. | `0.1.0` can be submitted manually later; automated attestations begin with later releases. | Inspect release immutability, archive, provenance, and absence of a BCR PR. |
| `D9` | Migrate jdbt by hard cut and temporarily use an archive override. | It proves a real consumer and avoids maintaining local/external formatter paths while BCR publication is pending. | Jdbt pins a GitHub URL and integrity until the registry entry exists. | Search for local formatter residue and build from a clean external fetch. |
| `D10` | Review the exact release SHA before publication, then reconfirm the released archive. | GitHub release immutability makes post-publication corrections expensive, and CI-driven fixes can otherwise bypass review. | Any tracked post-review fix consumes another reviewer round before tagging; delivery also has a final archive-substitution confirmation. | Compare reviewer-approved SHA, tag, archive identity, and both gates. |
| `D11` | Preserve jdbt history and create clean intentional history in the new repository. | Existing formatter commits are already upstream and document the source implementation. | No rebasing/squashing; extraction history begins at the new repository boundary. | Inspect both git histories and upstream synchronization. |
| `D12` | Keep durable API documentation in the rules repository, not jdbt domain artifacts. | This is development tooling, not a database-domain invariant or surprising hard-to-reverse jdbt architecture. | No jdbt spec or ADR; the temporary plan owns delivery evidence. | Confirm domain docs remain unchanged and README/tests own the tool contract. |

## Technical decisions

- The repository uses root public `defs.bzl` and aliases, private Starlark/Java implementation packages, `main`,
  `REPO.bazel`, Apache-2.0 license, one checked Maven lock, and a package under
  `org.realityforge.rules.palantirjavaformat`.
- `java_format_check(name, targets, remediation = ...)` applies a private aspect that owns direct workspace source
  files, traverses only the accepted Java dependency edges, and aggregates declared marker outputs.
- The formatter binaries accept repeatable `--root=PATH`; paths are resolved from `BUILD_WORKSPACE_DIRECTORY`, and
  formatter diagnostics remain repository-neutral except for consumer-provided check remediation.
- The module uses a uniquely named rules_jvm_external 7.1 extension repository for Palantir 2.93.0 and its locked
  closure. `rules_java` 9.9.0 is the only other normal Bzlmod dependency; the private worker codec has no protobuf
  build-time dependency.
- The smoke module has its own `MODULE.bazel`, `local_path_override`, optional unrelated Maven graph, clean Java target,
  and public check rule. Negative mutation/write/watch scenarios run in disposable copies so BCR steady-state tests pass.
- `tools/check.sh` is the release gate. CI runs it and the smoke suite with Java 17 under Bazel 8/9 across the accepted
  platforms and with Java 21/25 under Bazel 9 on Linux; release automation can only consume a green commit on `main`.
- Release automation produces `rules_palantir_java_format-v0.1.0.tar.gz` with prefix
  `rules_palantir_java_format-0.1.0/`, provenance, and immutable release metadata. `.bcr` templates target the smoke
  module; Publish-to-BCR configuration is present but does not open a PR in this delivery.
- Jdbt's provisional migration uses `local_path_override`; after the release it uses `archive_override` with the exact
  asset URL, strip prefix, and SRI integrity. Its wrapper passes `--root=src --root=tools`.
- GitHub bootstrap happens only after a read-only prepublication review. The approved rules commit SHA is recorded; any
  tracked CI-driven correction is re-reviewed by the same reviewer before release. Final rules CI/release state and
  archive-based jdbt changes are reconfirmed before jdbt is pushed.

## Testing decisions

- Port existing formatter, one-shot, protocol, and watcher tests while removing jdbt identity and parameterizing roots
  and remediation; retain direct Palantir CLI differential cases.
- Add Starlark/action integration evidence in the smoke consumer rather than relying only on jdbt's graph.
- Exercise dirty checks and file mutation in disposable smoke copies, leaving the checked-in smoke module permanently
  clean for BCR `bazel test //...`.
- Test dependency isolation with a separately named consumer Maven extension and enforce Maven lock consistency.
- Use process/PID or Bazel worker diagnostics to prove reuse; timing is supplementary, not proof.
- Test watcher safety with actual OS events on every promised platform and keep deterministic direct tests for overflow.
- Validate release archive structure, reproducibility, integrity, templates, provenance inputs, and a clean extracted
  consumer before publishing it.
- Run the rules full gate and jdbt's exact `tools/check.sh` before prepublication review, after release substitution, and
  before final handoff.

## Open questions

None.
