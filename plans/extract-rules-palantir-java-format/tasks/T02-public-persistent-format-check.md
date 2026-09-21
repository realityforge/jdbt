# T02 — Public persistent format check

- Status: `complete`
- Blocked by: `T01`
- Spec coverage: `R1`, `R4`, `R5`; `AC1`, `AC2`, `AC5`

## Delivers

Root `defs.bzl` exposes only `java_format_check`, backed by a private target-granular aspect and canonical protobuf
singleplex worker with configurable remediation and ordinary local fallback.

## Acceptance criteria

- [x] `java_format_check(name, targets, remediation = ...)` is the only public Starlark symbol; provider, aspect, worker,
  implementation labels, and generated Maven labels are private.
- [x] The private aspect creates one immutable marker action per directly owning Java target, selects only direct
  workspace source `.java` files, excludes generated/external sources, and traverses only `deps`, `runtime_deps`,
  `exports`, and `tests`.
- [x] Worker requests preserve IDs, cancellation, EOF, diagnostics, declared-marker behavior, and protocol-only stdout;
  one formatter lives outside the request loop and multiplexing is absent.
- [x] Non-worker parameter-file execution performs the identical check once, and dirty diagnostics show each relative
  file plus the consumer-provided remediation without modifying inputs.
- [x] Documented `.bazelrc` settings enable `worker,local` with one instance while leaving fallback correct without them.
- [x] Action inspection and process evidence demonstrate ownership, immutable inputs/outputs, and worker reuse.

## Validation

- Focused protocol tests and direct parameter-file execution — prove worker/fallback semantics.
- `bazel build`/`aquery` against owned Java targets — prove action shape, traversal, and source selection.
- Deliberate dirty/restore exercise with checksums and PID diagnostics — proves failure behavior, immutability, and reuse.
- `tools/check.sh` — required task gate.

## Evidence

- Rules commit `5478d54` (`feat: add persistent Java format check`).
- `bazel test //...` passed protobuf request/response, request-ID, cancellation, EOF, stale-marker, deterministic report,
  and parameter-file fallback tests.
- `bazel aquery --include_aspects` showed one source-only scan action for each of `:subject` and `:dependency`, plus one
  reporting action consuming only their declared markers; all three actions require protobuf worker protocol and support
  worker execution.
- A deliberate dirty `Subject.java` build failed with its workspace-relative path and configured remediation. Its
  SHA-256 remained `58952e2d350f4c4d2f58d984261fa2c232d81f9d0c345294b50ca0f0521a91db` before and after the check.
- A fresh `--strategy=PalantirJavaFormat=local` build executed two local actions successfully. A separate worker build
  executed two scan actions, while `jps -lv` showed exactly one formatter worker process (PID `71176`).
- `tools/check.sh` passed Buildifier, build, protocol/integration tests, self-formatting, Maven/Bzlmod locks, and clean
  tracked-state validation for the staged T02 tree.
