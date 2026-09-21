# Persistent Java Formatter Spec

## Source

- Shared understanding: completed `$grill-me` conversation confirmed by the user on 2026-09-21.
- Repository evidence: `.bazelversion`, `.bazelrc`, `MODULE.bazel`, `BUILD.bazel`, `tools/check.sh`,
  `tools/java_format.sh`, `tools/java-format/BUILD.bazel`, the current Java target graph, Palantir Java Format
  2.93.0 API and service metadata, and Bazel 9.2.0 worker protocol/toolchain behavior.

## Problem

Formatting currently starts a new Bazel-launched JVM for each check or write invocation. It cannot provide an
incremental target-owned Bazel check or an already-warm format-on-save loop. Developers pay formatter startup cost and
CI cannot cache formatting work at Java-target granularity.

## Required outcome

Jdbt has one in-process Palantir formatting implementation shared by a one-shot workspace formatter, an immutable
Bazel check backed by a reusable persistent worker, and a long-lived format-on-save watcher. All three workflows cover
workspace-owned Java under `src/` and `tools/`, retain current formatting semantics, and pass the repository's complete
validation gate.

## Scope

- In scope: a reusable Palantir formatter; one-shot whole-workspace writes; protobuf persistent-worker request
  handling with ordinary local fallback; a Java-target aspect and aggregate check target; a macOS/Linux filesystem
  watcher; focused unit, protocol, Starlark, and watcher tests; worker-reuse and timing evidence; developer command
  documentation; and retention of the low-cost `tools/java_format.sh write|check` wrapper.
- Out of scope: source mutation from Bazel actions, multiplex workers, remote execution, iBazel notifications, Windows
  watcher support, IDE-specific integration, arbitrary formatter frameworks, dependency-source formatting, compilation
  integration, custom diff rendering, startup formatting by the watcher, and permanent benchmark infrastructure.

## Constraints

- Formatting output must remain equivalent to the existing Palantir CLI behavior for the repository corpus and focused
  representative dirty inputs before the new implementation becomes canonical.
- Bazel formatting actions treat source files as immutable inputs and may write only declared outputs.
- The aggregate check covers every workspace-owned `.java` source below `src/` and `tools/`, including formatter
  production and test sources; it excludes generated files and external repositories.
- A formatting action owns the direct sources of one Java target. Traversal follows only the dependency edges required
  by the verified jdbt graph.
- Worker startup arguments and environment remain stable so Bazel can reuse one singleplex formatter JVM. The action
  also supports Bazel's ordinary local strategy fallback.
- Workspace-modifying `bazel run` commands resolve the source tree from `BUILD_WORKSPACE_DIRECTORY`.
- The watcher writes only normalized non-symlink `.java` paths within configured workspace roots. It serializes all
  formatting and directly overwrites a file only when formatted content differs.
- Protobuf is development-only infrastructure and must not become part of jdbt's supported downstream product API.
- Java 17, JSpecify/NullAway, Error Prone, explicit Bazel source ownership, generated-dependency boundaries,
  deterministic behavior, and the full `tools/check.sh` gate remain enforced.
- Existing untracked `.bazelbsp/` and `.idea/` content is unrelated user state and must remain untouched.

## Requirements

- `R1`: One shared formatter instance must load Palantir in-process and produce the repository's established Palantir
  output without invoking a formatter subprocess.
- `R2`: A one-shot command must discover every eligible Java source below `src/` and `tools/`, format all files in one
  JVM, and change only files whose formatted content differs.
- `R3`: `bazel build //:java_format_check` must check all eligible repository Java sources through immutable,
  target-granular actions and identify every unformatted file with an actionable remediation command.
- `R4`: Bazel must be able to reuse the formatter process across invalidated check actions and builds, while the same
  executable must complete one check outside worker mode when the worker strategy is unavailable.
- `R5`: The watcher must format created, modified, or editor-replaced eligible Java files after startup without a new
  JVM per event, recursive self-writing, or termination after temporary invalid input.
- `R6`: Watcher recovery must register new directories, coalesce repeated events, reject symlinks and out-of-scope
  paths, and recover from lost filesystem events by reporting and rescanning eligible roots.
- `R7`: The formatter tools must remain repository-development infrastructure and must not expand the supported API or
  dependency obligations of downstream jdbt consumers.
- `R8`: Developers must retain concise commands for checking, writing, and watching, while implementation detail and
  transient performance evidence remain outside domain specifications and ADRs.
- `R9`: Existing project behavior, build outputs, tests, static analysis, formatting, hermeticity checks, and coverage
  thresholds must continue to pass.

## Acceptance criteria

- `AC1` (`R1`): A unit test formats deliberately dirty Java to a checked expected result, and focused differential
  tests show identical output from the service API and current CLI behavior for formatting, imports, long strings, and
  Javadoc.
- `AC2` (`R1`, `R2`): `tools/java_format.sh write` launches the new one-shot frontend, uses one formatter JVM for the
  complete `src/` and `tools/` scope, and a second clean run changes nothing.
- `AC3` (`R3`): `bazel build //:java_format_check` passes on clean sources, including formatter sources, and a deliberate
  dirty source makes it fail with the workspace-relative filename and `tools/java_format.sh write` remediation.
- `AC4` (`R3`): Bazel action inspection demonstrates one `PalantirJavaFormat` check per source-owning Java target, no
  source outputs, and no external or generated Java inputs.
- `AC5` (`R4`): Protocol tests cover clean and dirty requests, request ID propagation, declared marker creation only on
  success, diagnostics, end-of-stream shutdown, and ordinary non-worker parameter-file execution.
- `AC6` (`R4`): Bazel worker diagnostics or process evidence demonstrates that separate invalidated formatting requests
  reuse one worker JVM under the configured mnemonic-specific worker limit.
- `AC7` (`R5`, `R6`): An end-to-end watcher test demonstrates formatting after modify and editor-style replacement,
  new-directory registration, no rewrite loop, survival and retry after invalid Java, symlink rejection, and overflow
  recovery behavior.
- `AC8` (`R5`): Starting the watcher does not rewrite existing files, and watcher writes remain restricted to normalized
  `.java` paths below `src/` and `tools/` in the source workspace.
- `AC9` (`R7`): Protobuf 33.4 is a direct development-only module dependency, Java worker classes are generated from
  Bazel's canonical schema with prebuilt `protoc`, and product targets remain consumable without the formatter package.
- `AC10` (`R8`): Developer documentation names the check, write, and watch commands; no domain specification, ADR,
  iBazel integration, or permanent benchmark artifact is introduced.
- `AC11` (`R1`, `R4`, `R5`): Closeout records the former cold formatter cost and first/subsequent worker and watcher
  timings, with evidence that repeated operations do not launch a formatter JVM per file or request.
- `AC12` (`R9`): The exact repository gate `tools/check.sh` passes after all intentional changes, and the final diff is
  limited to the formatter delivery and its temporary plan tree before closeout.

## Significant decisions

| ID | Decision | Rationale | Impact | User verification |
| --- | --- | --- | --- | --- |
| `D1` | Deliver worker check, one-shot write, and watcher together over one formatter API. | All three solve distinct parts of startup cost without needing separate formatting semantics. | More delivery surface, but one formatter implementation and focused frontends. | Verify all three commands operate on the same expected output. |
| `D2` | Preserve existing CLI output, using full-corpus fixed-point checks plus focused differential cases as the equivalence bar. | Palantir's service and CLI order internal phases differently, so equivalence must be demonstrated rather than assumed. | A discovered material mismatch blocks migration until resolved. | Inspect differential test coverage and results. |
| `D3` | Use one immutable worker action per directly owning Java target and aggregate through an aspect. | This is the smallest useful incremental/cache unit and respects Bazel ownership. | Changed targets rerun while unrelated checks remain cached. | Inspect `aquery` evidence and dirty-target behavior. |
| `D4` | Generate Java from Bazel's protobuf worker schema using development-only protobuf 33.4 and prebuilt `protoc`. | Bazel 9.2 intentionally disables its language-specific Java target, while source-built `protoc` fails the verified macOS hermeticity check. | Adds a dev-only module mapping and a stable toolchain option without exposing protobuf to product consumers. | Verify protocol generation and downstream product-target loading. |
| `D5` | Configure a singleplex worker with one mnemonic-specific instance and ordinary local fallback. | One warm formatter JVM is the intended optimization; multiplexing adds unneeded concurrency complexity. | Formatting is serialized per worker key but remains incremental. | Inspect worker diagnostics and fallback test. |
| `D6` | Include formatter implementation and tests in the aggregate check. | Self-hosting is valid in Bazel and avoids regressing the existing `src/` plus `tools/` scope. | The worker binary checks the sources used to build itself without an action cycle. | Verify aggregate roots and successful self-check. |
| `D7` | Retain the shell wrapper only as a thin dispatcher. | It preserves the current low-cost UX and the `tools/check.sh` caller without duplicating discovery or formatter logic. | The script remains stable while old parameter-file machinery is removed. | Inspect the simplified script and both modes. |
| `D8` | Watch with JDK `WatchService`, one formatting thread, direct conditional writes, strict non-symlink scope, and overflow rescans. | This minimizes dependencies and concurrency while retaining eventual correctness and source safety. | Rare overflow recovery can format other eligible dirty files after startup. | Exercise watcher safety, invalid-input recovery, and overflow behavior. |
| `D9` | Do not format existing files at watcher startup. | One-shot write already owns full-repository correction; watcher startup must not cause surprise edits. | Developers run the explicit write command when they want an initial pass. | Start against a deliberately dirty untouched file and verify no rewrite before an event. |
| `D10` | Keep this as development tooling rather than a supported downstream API. | Consumers need jdbt product targets, not its repository-local formatting process or protoc configuration. | Protobuf can remain a dev dependency and formatter targets need not load for consumers. | Verify a representative external consumer can still query/build product targets. |
| `D11` | Keep architecture rationale in this temporary plan and commands in developer documentation, not domain artifacts. | The tooling choice is reversible and does not define jdbt's database-domain contract. | No formatter ADR or domain specification is retained after closeout. | Confirm docs remain limited to developer UX. |

## Technical decisions

- A small null-marked Java library owns formatter loading, source-to-source formatting, comparison, and safe conditional
  writes. Frontends receive or construct one formatter instance and do not invoke Palantir's CLI main class.
- Formatter binaries share one Starlark-owned list of the six required `jdk.compiler` module exports.
- A repository-local `java_proto_library` generates `WorkerProtocol` classes from
  `@bazel_tools//src/main/protobuf:worker_protocol_proto`; its package is isolated beneath the development-only tool
  source tree so existing dependency packages remain loadable to consumers.
- The worker recognizes `--persistent_worker`; otherwise it expands Bazel's final `@params` argument and performs one
  identical check. Worker stdout is protocol-only, diagnostics travel in `WorkResponse.output`, and request IDs are
  preserved.
- The aspect follows the verified `deps`, `runtime_deps`, `exports`, and `tests` edges; selects direct workspace-owned
  `.java` sources by Bazel repository identity and generation status; produces one success marker per owning target;
  and propagates marker/source depsets through a provider.
- The aggregate rule exposes the aspect outputs from application, test, formatter production, and formatter test roots.
  `.bazelrc` selects `worker,local`, one worker instance per formatter worker key, and prebuilt `protoc`.
- The one-shot frontend sorts discovered files for deterministic processing. The watcher registers existing source-root
  directories without formatting them, registers new directories recursively, debounces/coalesces paths, and performs
  all formatting on one thread.
- Watcher path admission normalizes paths, requires the lexical and resolved path to stay below the workspace and an
  allowed root, requires a `.java` suffix, and rejects symbolic links. Formatting failure reports to stderr and leaves
  bytes untouched.

## Testing decisions

- Unit-test the shared formatter and filesystem write semantics without subprocesses.
- Differential-test the service against established CLI golden outputs rather than attempting an unbounded equivalence
  proof.
- Drive the worker loop with serialized protobuf requests and captured responses; separately exercise ordinary
  parameter-file mode.
- Test aspect source selection and provider aggregation through real Bazel builds/action inspection, including
  formatter self-check and deliberate dirty-source failure.
- Structure watcher registration, event admission, debouncing, and formatting so deterministic unit tests cover edge
  behavior; retain one real-process end-to-end exercise for OS/editor save behavior.
- Capture baseline, first, and warm timings as temporary task evidence. Use Bazel worker diagnostics or PID evidence to
  prove reuse rather than treating timing alone as proof.
- Run the narrowest target tests after each task and the exact `tools/check.sh` gate before implementation review.

## Open questions

None.
