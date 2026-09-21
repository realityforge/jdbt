# T01 — Rules repository foundation and one-shot formatter

- Status: `pending`
- Blocked by: `None`
- Spec coverage: `R2`, `R4`, `R5`, `R9`; `AC3`, `AC5`, `AC11`

## Delivers

A local `rules_palantir_java_format` git repository has its public identity, license, Bzlmod dependency graph, isolated
locked Maven closure, private repository-neutral formatter implementation, explicit-root one-shot command, focused
parity/safety tests, and an initial comprehensive check script.

## Acceptance criteria

- [ ] The repository uses `main`, an unmodified Apache-2.0 `LICENSE`, root `NOTICE` containing
  `Copyright 2026 Peter Donald`, module/repository metadata without source version or compatibility level, Java package
  `org.realityforge.rules.palantirjavaformat`, and no jdbt labels or package identity.
- [ ] rules_jvm_external 7.1 owns a uniquely named strict, pinned Palantir 2.93.0 closure; rules_java 9.9.0 and protobuf
  33.4 are normal dependencies, and Maven lock drift fails.
- [ ] Root alias `//:java_format` requires repeatable safe `--root=PATH` values and rejects missing, absolute, outside,
  nonexistent, non-directory, or symlink roots without writes.
- [ ] Valid roots are normalized, sorted, and deduplicated; only regular non-symlink `.java` descendants are formatted
  deterministically, by one formatter instance, and only when output differs.
- [ ] Direct Palantir CLI comparisons and retained goldens prove import, Javadoc, and long-string parity.
- [ ] The initial `tools/check.sh` validates buildifier, formatting, Maven lock integrity, build, and unit tests.

## Validation

- `bazel test //...` — proves the repository-neutral formatter, root admission, discovery, parity, and conditional writes.
- Dependency graph and disposable consumer query/build — prove the unique Maven repository and public dependency boundary.
- License/NOTICE and repository metadata inspection — proves the accepted ownership and public identity.
- `tools/check.sh` — proves the initial repository gate from a clean checkout.

## Evidence

- `pending`
