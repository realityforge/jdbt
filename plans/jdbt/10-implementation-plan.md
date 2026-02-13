# jdbt Implementation Plan

## Phase Sequence

1. Project foundation and quality toolchain.
2. CI gate automation with early quality feedback.
3. Persistent project planning artifacts.
4. Config and model layer (`jdbt.yml`, `repository.yml`).
5. Repository merge and ordering semantics.
6. Filesystem and artifact resolution semantics.
7. Runtime orchestration core commands.
8. Import orchestration and resume behavior.
9. Migration orchestration and migration table semantics.
10. Deterministic packaging (`package-data`) with reproducibility verification.
11. CLI command integration and argument validation.
12. Documentation and parity closure.
13. PostgreSQL support (Phase 2).
14. End-user documentation (`README.md` and usage guide).
15. Remove top-level `defaults` from `jdbt.yml`; use hardcoded runtime defaults.
16. Publish reusable Agent Skill for the delivery workflow.
17. Package skill for standalone distribution and document adoption.
18. Remove `searchDirs` config and fix search root to `jdbt.yml` directory.
19. Mark all source and test packages as `@NullMarked`.
20. Enable requested Error Prone checks and remediate violations in production and test code.
21. Flatten `jdbt.yml` to a single implicit database configuration model.
22. Enable additional requested Error Prone checks, suppress accepted `UnusedException`, and fix `UnnecessarilyFullyQualified` violations.
23. Prefer `var` for local declarations where type inference remains clear and safe.
24. Enable Picnic Error Prone `JUnitClassModifiers` and disable `StaticImport` noise.
25. Extend `var` preference to try-with-resources and foreach declarations where inference is safe.
26. Add `filterProperties` config model with strict validation and ordered declaration semantics.
27. Add strict CLI `--property` parsing and propagate filter values through command/runtime interfaces.
28. Apply declared `filterProperties` replacements in SQL execution flow and enforce required/supported values.
29. Restrict import runtime tokens to `__SOURCE__`, `__TARGET__`, `__TABLE__` with non-overridable tool-provided values.
30. Document `filterProperties`, strict CLI behavior, and reserved import token semantics.
31. Port SQL Server import assert macro expansion for ASSERT_ROW_COUNT, ASSERT_DATABASE_VERSION, and ASSERT_UNCHANGED_ROW_COUNT.
32. Document SQL Server-only import assert macros and parity notes.

## Delivery Approach

- Implement one task at a time with minimal diffs.
- Validate each task with targeted checks while iterating.
- Before task completion:
  - run full required gates,
  - update `plans/jdbt/20-task-board.yaml`,
  - commit one step.

## High-Risk Areas

- File ordering and index behavior across merged sources.
- Duplicate basename detection and conflict diagnostics.
- Import selection precedence for fixture/sql/default paths.
- Migration release-cut behavior (`*_Release-<version>.sql`).
- Deterministic ZIP output reproducibility.

## Design Constraints

- Preserve parity by default.
- Document every intentional divergence in the compatibility matrix.
- Keep default test runs database-free.
- Keep artifact outputs deterministic.

## Required Full Gates

`./gradlew clean spotlessCheck check fatJar`
