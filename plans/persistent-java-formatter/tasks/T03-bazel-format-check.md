# T03 — Target-granular Bazel format check

- Status: `pending`
- Blocked by: `T02`
- Spec coverage: `R3`, `R4`; `AC3`, `AC4`, `AC6`

## Delivers

A Starlark aspect creates immutable `PalantirJavaFormat` actions for directly owned Java sources, propagates results
through the verified jdbt target edges, and backs a root `//:java_format_check` target covering product, test, and
formatter sources with a reusable singleplex worker.

## Acceptance criteria

- [ ] Source selection uses Bazel repository/generation identity, processes only direct workspace-owned `.java` files,
  and excludes generated and external sources.
- [ ] One marker-producing action is created per source-owning target and recursive provider aggregation follows only
  `deps`, `runtime_deps`, `exports`, and `tests`.
- [ ] Aggregate roots cover all current `src/` sources plus formatter production and test sources without an analysis or
  action cycle.
- [ ] `.bazelrc` selects `worker,local`, protobuf's prebuilt compiler, and one formatter worker instance per worker key;
  action startup arguments and environment remain stable.
- [ ] `tools/java_format.sh check` is a thin dispatcher to `bazel build //:java_format_check`.
- [ ] A deliberate dirty source fails with its workspace-relative filename and remediation; restoration makes the check
  pass, and the action never modifies that source.
- [ ] Bazel diagnostics or process evidence demonstrates worker reuse across separately invalidated requests/builds.

## Validation

- `bazel build //:java_format_check` — proves the clean aggregate check and self-hosting graph.
- `bazel aquery --include_aspects ...` — proves target granularity, inputs, declared outputs, and execution requirements.
- Controlled dirty-source failure/restoration script or manual exercise — proves diagnostics and immutable inputs.
- Bazel worker diagnostics/PID exercise — proves one warm process services repeated requests.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `pending`
