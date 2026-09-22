# Task Map

- Spec: [`SPEC.md`](../SPEC.md)
- Status: `in_progress`
- Current frontier: `T06 public release and archive migration`
- Planning reviewer: `/root/extraction_planning_reviewer` (`2/3` rounds, `Findings: none`)
- Plan checkpoint: `automatic` (confirmed `$grill-me` understanding and passing planning review)
- Implementation reviewer: `/root/implementation_reviewer` (`10` rounds; `Findings: none`; approved cleanup candidate
  `cc381c37b5fa3606f6822fe72382e0f2089c8b31` for hosted validation)

## Full-scope validation

- Gates: `/Users/peter/Code/realityforge/rules_palantir_java_format/tools/check.sh`, jdbt `tools/check.sh`, required
  GitHub Bazel 8/9 platform checks, release archive/provenance verification, and upstream state verification.
- Evidence: `pending`

## Tasks

| ID | Task | Status | Blocked by |
| --- | --- | --- | --- |
| `T01` | [`Rules repository foundation and one-shot formatter`](T01-rules-repository-foundation.md) | `complete` | None |
| `T02` | [`Public persistent format check`](T02-public-persistent-format-check.md) | `complete` | `T01` |
| `T03` | [`Safe configurable-root watcher`](T03-safe-configurable-root-watcher.md) | `complete` | `T01` |
| `T04` | [`Portable consumer, CI, and release readiness`](T04-portable-release-readiness.md) | `complete` | `T02`, `T03` |
| `T05` | [`Provisional jdbt hard-cut migration`](T05-provisional-jdbt-migration.md) | `complete` | `T04` |
| `T06` | [`Public v0.1.0 and released-archive migration`](T06-release-and-archive-migration.md) | `pending` | `T05`, prepublication review |
| `T07` | [`Final publication and governance`](T07-final-publication-and-governance.md) | `pending` | `T06`, final archive review |

## Sequencing notes

- `T01` through `T05` remain local and reversible. After their gates pass, pause implementation for the first read-only
  implementation-review round; record the approved rules commit SHA, and block `T06` until that reviewer reports
  `Findings: none`.
- `T06` creates/pushes the public rules repository, waits for its required matrix, publishes immutable `v0.1.0`, and
  replaces jdbt's local override with the released archive. The same reviewer must confirm the final substitution and
  any release-driven edits before `T07`.
- Any tracked change made after review to resolve GitHub CI invalidates the recorded release approval. Rerun both local
  gates and obtain another findings-free round from the same reviewer before creating the release tag.
- `T07` pushes jdbt only after final review, enables post-bootstrap repository governance, and verifies both upstream
  branches and release state. A final reviewer confirmation is required if rollout changes tracked content.
- Each code task ends with the narrowest meaningful checks, its repository gate when required by risk, and a coherent
  commit in the owning repository. Never stage `.bazelbsp/` or `.idea/`.
- If the full compatibility matrix, release workflow, archive consumption, or either repository gate fails, diagnose
  and fix it without reducing accepted scope; return to the user only if a public contract must change.

## Promoted knowledge

- `not-required`: the durable public tool contract belongs in the new repository's README, root API, tests, and release
  metadata. The extraction does not change jdbt's database-domain contract and does not qualify for a jdbt spec or ADR.
