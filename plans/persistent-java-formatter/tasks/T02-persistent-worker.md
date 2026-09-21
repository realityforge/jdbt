# T02 — Persistent worker protocol and fallback

- Status: `complete`
- Blocked by: `T01`
- Spec coverage: `R3`, `R4`, `R7`; `AC5`, `AC9`

## Delivers

A repository-development Java executable consumes Bazel's canonical protobuf worker protocol with one formatter
instance outside its request loop, returns actionable check results, and performs the same check once from a Bazel
parameter file when persistent-worker mode is not selected.

## Acceptance criteria

- [x] Protobuf 33.4 is a direct `dev_dependency`; Java protocol classes are generated locally from Bazel's canonical
  worker schema using prebuilt `protoc`, without hand-written wire handling or checked-in generated source.
- [x] The worker recognizes `--persistent_worker`, keeps protocol stdout clean, preserves request IDs, reports dirty
  workspace-relative filenames and remediation, writes the declared marker only on success, and exits cleanly at EOF.
- [x] One formatter is constructed outside the single-request loop; multiplexing and concurrent formatting are absent.
- [x] Non-worker invocation expands the final `@params` file and performs the identical immutable check once.
- [x] The protocol/tool source package is development-only and does not make protobuf necessary to load or build jdbt
  product targets from a consuming module.

## Validation

- Focused worker Java tests with serialized `WorkRequest`/`WorkResponse` messages — prove clean, dirty, request-ID,
  marker, diagnostics, and EOF behavior.
- Direct non-worker parameter-file invocation — proves the local-strategy fallback contract.
- Representative external-module query/build of a jdbt product target — proves the development dependency boundary.
- `tools/check.sh` — required step-completion gate.

## Evidence

- `bazel test //tools/java-format/src/test/java/org/realityforge/jdbt/tools/javaformat:javaformat_tests` — passed
  clean/dirty/cancelled protocol requests, request IDs, EOF, marker behavior, diagnostics, and local parameter expansion.
- `bazel run //tools/java-format:palantir_java_format_worker -- @/tmp/jdbt-worker.params` — clean direct fallback
  invocation exited zero and created an empty declared marker.
- External Bazel 9.2 consumer with jdbt's documented rules_java patch queried and built
  `@jdbt//src/main/java/org/realityforge/jdbt/config:config` without the development-only protobuf mapping.
- `tools/update_java_deps.sh --check` — passed.
- `tools/check.sh` — passed; 10 tests passed, line coverage 89.49%, branch coverage 78.60%.
