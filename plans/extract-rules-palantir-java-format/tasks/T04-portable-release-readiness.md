# T04 — Portable consumer, CI, and release readiness

- Status: `pending`
- Blocked by: `T02`, `T03`
- Spec coverage: `R5`, `R6`, `R7`, `R9`; `AC1`, `AC5`, `AC6`, `AC7`, `AC11`, `AC14`

## Delivers

An independent smoke consumer, complete cross-platform gates, public documentation, deterministic release packaging,
provenance workflow, and BCR-ready metadata make the local rules repository ready for prepublication review.

## Acceptance criteria

- [ ] A standalone `e2e/smoke` module uses `local_path_override`, loads only root public API/aliases, carries its own
  independent Maven extension, and passes its permanently clean check target.
- [ ] Disposable smoke copies prove dirty-check failure/repair, repeated-root writes, root inclusion/exclusion, watcher
  events/recovery, symlink safety, and public visibility without leaving dirty checked-in fixtures.
- [ ] CI defines Java 17 lanes for Bazel 8 and 9 across Linux, macOS x86/arm64, and Windows, plus Java 21 and 25 lanes
  for Bazel 9 on Linux, with no allowed failures; local/available lanes pass before prepublication review.
- [ ] README documents Bzlmod setup, public API, explicit roots, worker `.bazelrc`, pre-BCR archive override,
  troubleshooting, the bounded Java 17/21/25 compatibility matrix, later-JDK policy, and pre-1.0 policy without
  documenting private labels.
- [ ] `tools/check.sh` covers buildifier, source formatting, unit tests, smoke steady state and negative exercises, Maven
  lock integrity, release archive reproducibility/layout, `.bcr` templates, and a clean diff.
- [ ] `.bcr` templates contain valid maintainer/repository/test metadata and open no PR; release automation builds the
  named deterministic archive and provenance, and future Publish-to-BCR automation targets its current reusable flow.
- [ ] Repository metadata/configuration files are complete and no debug, jdbt-specific, temporary, or disconnected
  compatibility path remains.

## Validation

- `tools/check.sh` under locally available supported JDK/Bazel combinations — proves local release readiness and smoke
  consumption before the complete hosted matrix.
- CI/workflow and `.bcr` schema checks — prove matrix/release/registry configuration before publication.
- Two archive builds plus checksum/tree comparison and extracted smoke run — prove deterministic consumable packaging.

## Evidence

- `pending`
