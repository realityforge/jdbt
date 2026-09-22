# T07 — Final publication and governance

- Status: `complete`
- Blocked by: `T06`, final archive review
- Spec coverage: `R10`; `AC12`, `AC13`

## Delivers

Reviewed jdbt migration commits are pushed, post-bootstrap repository governance is enabled, both upstream main
branches and release metadata match the reviewed delivery, and the temporary cross-repository plan is ready to close.

## Acceptance criteria

- [x] Jdbt is pushed only after final archive review reports `Findings: none`; local and upstream `main` resolve to the
  same reviewed commit without rewriting existing history.
- [x] Rules repository publication settings remain correct, and PR-plus-required-CI protection covers post-bootstrap
  changes to `main` without invalidating the release workflow.
- [x] Rules local/upstream/release tag and archive source commit agree; jdbt local/upstream agree and resolve the release
  archive rather than local state.
- [x] Repository searches and status checks show no debug/scratch/release-candidate files, stale overrides, uncommitted
  delivery changes, or modifications to jdbt `.bazelbsp/` and `.idea/`.
- [x] Final validation evidence, reviewer rounds, release URL, check state, publication state, and domain-artifact
  classification are recorded before closeout deletes only the active plan tree.

## Validation

- Git/GitHub ref and repository-settings inspection — proves publication, governance, and identity.
- Final rules and jdbt full gates — strongest handoff verification if rollout changed any relevant state.
- `git status`, archive/module inspection, and plan audit — prove clean closeout readiness.

## Evidence

- Same-reviewer round 13 reported `Findings: none`, approved the final archive substitution, and found no residual risk
  before jdbt commit `df9b335826fe2801a332cc921ef375cbd8c5fafd` was pushed. Local `HEAD`, `origin/main`, and
  upstream `refs/heads/main` all resolve to that commit; hosted jdbt CI run `35677654369` passed its `verify` job.
- Rules local `HEAD`, local `origin/main`, upstream `main`, and peeled annotated tag `v0.1.0` all resolve to reviewed
  commit `fbd857332d0a9993fa50d66ccfcf361658e448d2`. Hosted ten-lane CI run `35676075377` and guarded
  preflight/build/attest/release run `35676536245` both completed successfully.
- The public repository remains public with issues enabled, projects/wiki disabled, and immutable releases enabled.
  Protection on `main` requires pull requests, a current branch, all ten named CI contexts, resolved conversations, and
  admin compliance; force pushes and deletion are disabled. Zero approvals keeps the single-maintainer repository
  usable while still prohibiting direct pushes.
- Immutable release `https://github.com/realityforge/rules_palantir_java_format/releases/tag/v0.1.0` contains the
  expected archive and Sigstore/SLSA provenance. The archive digest is
  `f56b55c21a5a4c80e7c3adec39b83f198ed654890a1d88c5150fa3a40f065b56`; independent prefix, provenance,
  extracted-consumer, and SRI checks passed.
- The exact rules full gate passed before publication; its ten hosted lanes passed on the released commit. Jdbt's full
  gate passed after release substitution with the sibling checkout physically unavailable, and again after the final
  reviewed evidence update. Fresh-output-base resolution exposed only the rules repository's public formatter aliases.
- Both tracked worktrees are clean. Jdbt retains only the user's untouched untracked `.bazelbsp/` and `.idea/` state;
  searches found no stale local override. No BCR PR or registry fork exists. The formatter is development tooling, so
  accepted decision `D12` keeps its durable contract in the rules README/API/tests and adds no jdbt domain spec or ADR.
