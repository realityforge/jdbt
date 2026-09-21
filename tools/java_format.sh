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

if [[ "${MODE}" == "write" ]]; then
  exec bazel run //tools/java-format:java_format -- --write
fi

args_file="$(mktemp)"
trap 'rm -f "${args_file}"' EXIT

while IFS= read -r source_file; do
  printf '%s/%s\n' "${ROOT}" "${source_file}" >> "${args_file}"
done < <(find src tools -type f -name '*.java' | sort)

if [[ ! -s "${args_file}" ]]; then
  exit 0
fi

bazel run //tools/java-format:palantir_java_format -- \
  --palantir \
  --dry-run \
  --set-exit-if-changed \
  "@${args_file}"
