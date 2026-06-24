#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT}"
tools/update_java_deps.sh
bazel run //:buildifier_check
tools/java_format.sh check
bazel build //...
bazel test //...
bazel coverage //src/test/java/org/realityforge/jdbt:all_tests --combined_report=lcov
tools/check_coverage.py bazel-out/_coverage/_coverage_report.dat 0.85 0.75
