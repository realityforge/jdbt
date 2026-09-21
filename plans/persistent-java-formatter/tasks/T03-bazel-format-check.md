# T03 — Target-granular Bazel format check

- Status: `complete`
- Blocked by: `T02`
- Spec coverage: `R3`, `R4`; `AC3`, `AC4`, `AC6`

## Delivers

A Starlark aspect creates immutable `PalantirJavaFormat` actions for directly owned Java sources, propagates results
through the verified jdbt target edges, and backs a root `//:java_format_check` target covering product, test, and
formatter sources with a reusable singleplex worker.

## Acceptance criteria

- [x] Source selection uses Bazel repository/generation identity, processes only direct workspace-owned `.java` files,
  and excludes generated and external sources.
- [x] One marker-producing action is created per source-owning target and recursive provider aggregation follows only
  `deps`, `runtime_deps`, `exports`, and `tests`.
- [x] Aggregate roots cover all current `src/` sources plus formatter production and test sources without an analysis or
  action cycle.
- [x] `.bazelrc` selects `worker,local`, protobuf's prebuilt compiler, and one formatter worker instance per worker key;
  action startup arguments and environment remain stable.
- [x] `tools/java_format.sh check` is a thin dispatcher to `bazel build //:java_format_check`.
- [x] A deliberate dirty source fails with its workspace-relative filename and remediation; restoration makes the check
  pass, and the action never modifies that source.
- [x] Bazel diagnostics or process evidence demonstrates worker reuse across separately invalidated requests/builds.

## Validation

- `bazel build //:java_format_check` — proves the clean aggregate check and self-hosting graph.
- `bazel aquery --include_aspects ...` — proves target granularity, inputs, declared outputs, and execution requirements.
- Controlled dirty-source failure/restoration script or manual exercise — proves diagnostics and immutable inputs.
- Bazel worker diagnostics/PID exercise — proves one warm process services repeated requests.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `bazel build //:java_format_check` and `tools/java_format.sh check` — passed with formatter self-check roots.
- Explicit-aspect `bazel aquery --include_aspects` comparison matched all 93 workspace Java files and reported 20
  target-owned `PalantirJavaFormat` marker actions with source-only inputs and worker protocol requirements.
- A deliberately unformatted `PalantirFormatter.java` failed with its workspace-relative path and remediation; its
  checksum remained unchanged during the failed action, and restoration passed.
- A formatted change to unrelated `ConfigException.java` reran one action while persistent worker PID `14512` remained
  `14512`; the source was then restored.
- Ahab's project-local reproducibility specification recognizes the deterministic formatter worker, and
  `bazel run //tools/ahab:hermeticity.check` passed with no recorded violation.
- `tools/check.sh` — passed; 10 tests passed, line coverage 89.49%, branch coverage 78.60%.
