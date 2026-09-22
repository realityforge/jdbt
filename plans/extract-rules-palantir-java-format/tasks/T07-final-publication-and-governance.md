# T07 — Final publication and governance

- Status: `in_progress`
- Blocked by: `T06`, final archive review
- Spec coverage: `R10`; `AC12`, `AC13`

## Delivers

Reviewed jdbt migration commits are pushed, post-bootstrap repository governance is enabled, both upstream main
branches and release metadata match the reviewed delivery, and the temporary cross-repository plan is ready to close.

## Acceptance criteria

- [ ] Jdbt is pushed only after final archive review reports `Findings: none`; local and upstream `main` resolve to the
  same reviewed commit without rewriting existing history.
- [ ] Rules repository publication settings remain correct, and PR-plus-required-CI protection covers post-bootstrap
  changes to `main` without invalidating the release workflow.
- [ ] Rules local/upstream/release tag and archive source commit agree; jdbt local/upstream agree and resolve the release
  archive rather than local state.
- [ ] Repository searches and status checks show no debug/scratch/release-candidate files, stale overrides, uncommitted
  delivery changes, or modifications to jdbt `.bazelbsp/` and `.idea/`.
- [ ] Final validation evidence, reviewer rounds, release URL, check state, publication state, and domain-artifact
  classification are recorded before closeout deletes only the active plan tree.

## Validation

- Git/GitHub ref and repository-settings inspection — proves publication, governance, and identity.
- Final rules and jdbt full gates — strongest handoff verification if rollout changed any relevant state.
- `git status`, archive/module inspection, and plan audit — prove clean closeout readiness.

## Evidence

- `pending`
