# T01 — Rules repository foundation and one-shot formatter

- Status: `complete`
- Blocked by: `None`
- Spec coverage: `R2`, `R4`, `R5`, `R9`; `AC3`, `AC5`, `AC11`

## Delivers

A local `rules_palantir_java_format` git repository has its public identity, license, Bzlmod dependency graph, isolated
locked Maven closure, private repository-neutral formatter implementation, explicit-root one-shot command, focused
parity/safety tests, and an initial comprehensive check script.

## Acceptance criteria

- [x] The repository uses `main`, an unmodified Apache-2.0 `LICENSE`, root `NOTICE` containing
  `Copyright 2026 Peter Donald`, module/repository metadata without source version or compatibility level, Java package
  `org.realityforge.rules.palantirjavaformat`, and no jdbt labels or package identity.
- [x] rules_jvm_external 7.1 owns a uniquely named strict, pinned Palantir 2.93.0 closure; rules_java 9.9.0 is the
  only normal Bazel rule dependency, the worker protocol codec has no generated-protobuf dependency, and Maven lock
  drift fails.
- [x] Root alias `//:java_format` requires repeatable safe `--root=PATH` values and rejects missing, absolute, outside,
  nonexistent, non-directory, or symlink roots without writes.
- [x] Valid roots are normalized, sorted, and deduplicated; only regular non-symlink `.java` descendants are formatted
  deterministically, by one formatter instance, and only when output differs.
- [x] Direct Palantir CLI comparisons and retained goldens prove import, Javadoc, and long-string parity.
- [x] The initial `tools/check.sh` validates buildifier, formatting, Maven lock integrity, build, and unit tests.

## Validation

- `bazel test //...` — proves the repository-neutral formatter, root admission, discovery, parity, and conditional writes.
- Dependency graph and disposable consumer query/build — prove the unique Maven repository and public dependency boundary.
- License/NOTICE and repository metadata inspection — proves the accepted ownership and public identity.
- `tools/check.sh` — proves the initial repository gate from a clean checkout.

## Evidence

- Rules commit `b777868` (`feat: add explicit-root Java formatter`).
- `bazel test //...` passed the parity, write-minimization, explicit-root, discovery, and rejection tests.
- `tools/check.sh` passed Buildifier lint, the complete build/test set, self-formatting, lock enforcement, and a clean
  tracked diff.
- A disposable Bazel 9.2.0 consumer built its own `@consumer_maven` Guava-backed target and
  `@rules_palantir_java_format//:java_format` together, proving the dev-only tooling and unique formatter Maven
  repository do not leak across the public package boundary.
- The final dependency closure contains rules_java 9.9.0, rules_jvm_external 7.1, and the uniquely named locked Maven
  repository. The private worker uses the canonical length-delimited protobuf wire fields through a dependency-free
  codec, so neither protobuf rules nor generated runtime libraries leak into consumers.
- `LICENSE`, `NOTICE`, `MODULE.bazel`, `REPO.bazel`, and the Java package were inspected for the accepted public
  identity; the source module has no version or compatibility level.
