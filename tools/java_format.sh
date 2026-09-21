#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-write}"

case "${MODE}" in
  write | check)
    ;;
  *)
    echo "usage: tools/java_format.sh [write|check]" >&2
    exit 2
    ;;
esac

cd "${ROOT}"

if [[ "${MODE}" == "check" ]]; then
  exec bazel build //:java_format_check
fi

exec bazel run @rules_palantir_java_format//:java_format -- --root=src --root=tools
