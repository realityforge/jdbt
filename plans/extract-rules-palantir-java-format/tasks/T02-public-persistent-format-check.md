# T02 — Public persistent format check

- Status: `in_progress`
- Blocked by: `T01`
- Spec coverage: `R1`, `R4`, `R5`; `AC1`, `AC2`, `AC5`

## Delivers

Root `defs.bzl` exposes only `java_format_check`, backed by a private target-granular aspect and canonical protobuf
singleplex worker with configurable remediation and ordinary local fallback.

## Acceptance criteria

- [ ] `java_format_check(name, targets, remediation = ...)` is the only public Starlark symbol; provider, aspect, worker,
  implementation labels, and generated Maven labels are private.
- [ ] The private aspect creates one immutable marker action per directly owning Java target, selects only direct
  workspace source `.java` files, excludes generated/external sources, and traverses only `deps`, `runtime_deps`,
  `exports`, and `tests`.
- [ ] Worker requests preserve IDs, cancellation, EOF, diagnostics, declared-marker behavior, and protocol-only stdout;
  one formatter lives outside the request loop and multiplexing is absent.
- [ ] Non-worker parameter-file execution performs the identical check once, and dirty diagnostics show each relative
  file plus the consumer-provided remediation without modifying inputs.
- [ ] Documented `.bazelrc` settings enable `worker,local` with one instance while leaving fallback correct without them.
- [ ] Action inspection and process evidence demonstrate ownership, immutable inputs/outputs, and worker reuse.

## Validation

- Focused protocol tests and direct parameter-file execution — prove worker/fallback semantics.
- `bazel build`/`aquery` against owned Java targets — prove action shape, traversal, and source selection.
- Deliberate dirty/restore exercise with checksums and PID diagnostics — proves failure behavior, immutability, and reuse.
- `tools/check.sh` — required task gate.

## Evidence

- `pending`
