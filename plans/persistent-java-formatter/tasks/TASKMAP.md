# Task Map

- Spec: [`SPEC.md`](../SPEC.md)
- Status: `ready-for-closeout`
- Current frontier: `closeout`
- Planning reviewer: `/root/planning_reviewer` (`1/3` rounds, `Findings: none`)
- Plan checkpoint: `automatic` (confirmed `$grill-me` understanding and passing planning review)
- Implementation reviewer: `/root/implementation_reviewer` (`2/5` rounds, `Findings: none`)

## Full-scope validation

- Gate: `tools/check.sh`
- Evidence: `tools/check.sh` passed after integrated clean/dirty/repair/repeat-clean and live watcher exercises; 10 tests
  passed, with 89.49% line coverage and 78.60% branch coverage.

## Tasks

| ID | Task | Status | Blocked by |
| --- | --- | --- | --- |
| `T01` | [`Shared formatter and one-shot write`](T01-shared-formatter-and-one-shot.md) | `complete` | None |
| `T02` | [`Persistent worker protocol and fallback`](T02-persistent-worker.md) | `complete` | `T01` |
| `T03` | [`Target-granular Bazel format check`](T03-bazel-format-check.md) | `complete` | `T02` |
| `T04` | [`Safe long-lived format watcher`](T04-format-watcher.md) | `complete` | `T01` |
| `T05` | [`Integrated UX, evidence, and full gate`](T05-integrated-validation.md) | `complete` | `T03`, `T04` |

## Sequencing notes

- Select the lowest-numbered pending task whose blockers are complete. After `T01`, `T02` is selected before the also
  unblocked `T04` so the worker and aggregate-check path stabilizes before watcher integration.
- Each task must leave its delivered command or target green and commit only that task's coherent slice.
- `T03` replaces check-mode internals in the shell wrapper; `T05` owns final cross-workflow documentation, performance
  evidence, deliberate dirty-source demonstrations, and the repository-wide gate.

## Promoted knowledge

- `not-required`: the delivery introduces repository-development tooling, not a durable database-domain contract or
  hard-to-reverse architectural decision.
