#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="0.28"
URL="https://repo.maven.apache.org/maven2/org/realityforge/bazel/depgen/bazel-depgen/${VERSION}/bazel-depgen-${VERSION}-all.jar"
OUTPUT_BASE="$(cd "${ROOT}" && bazel info output_base)"
TOOLS_DIR="${OUTPUT_BASE}/.depgen-tools"
CACHE_DIR="${OUTPUT_BASE}/.depgen-cache"
JAR="${TOOLS_DIR}/bazel-depgen-${VERSION}-all.jar"

CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
  CHECK_ONLY=true
  shift
fi
if ((0 != $#)); then
  echo "Usage: $0 [--check]" >&2
  exit 1
fi

mkdir -p "${TOOLS_DIR}" "${CACHE_DIR}"

if [[ ! -f "${JAR}" ]]; then
  tmp="${JAR}.tmp"
  curl -fsSL -o "${tmp}" "${URL}"
  mv "${tmp}" "${JAR}"
fi

generate() {
  local directory="$1"
  java -jar "${JAR}" \
    --directory "${directory}" \
    --config-file third_party/java/dependencies.yml \
    --cache-directory "${CACHE_DIR}" \
    generate
  java -jar "${JAR}" \
    --directory "${directory}" \
    --config-file tools/java-format/dependencies.yml \
    --cache-directory "${CACHE_DIR}" \
    generate
}

cd "${ROOT}"
if [[ "${CHECK_ONLY}" == true ]]; then
  CHECK_ROOT="$(mktemp -d)"
  trap 'rm -rf "${CHECK_ROOT}"' EXIT
  mkdir -p "${CHECK_ROOT}/third_party/java" "${CHECK_ROOT}/tools/java-format"
  cp MODULE.bazel "${CHECK_ROOT}/MODULE.bazel"
  cp third_party/java/BUILD.bazel third_party/java/dependencies.yml "${CHECK_ROOT}/third_party/java/"
  cp tools/java-format/BUILD.bazel tools/java-format/dependencies.yml "${CHECK_ROOT}/tools/java-format/"
  generate "${CHECK_ROOT}"
  bazel build //:buildifier
  BUILDIFIER_DIRECTORY="$(bazel info bazel-bin)"
  BUILDIFIER="$(find "${BUILDIFIER_DIRECTORY}/buildifier.bash.runfiles" -name buildifier -print -quit)"
  if [[ ! -x "${BUILDIFIER}" ]]; then
    echo "Unable to locate the Buildifier executable" >&2
    exit 1
  fi
  "${BUILDIFIER}" \
    -mode=fix \
    -lint=fix \
    '--warnings=+unsorted-dict-items,-module-docstring,-function-docstring' \
    "${CHECK_ROOT}/MODULE.bazel" \
    "${CHECK_ROOT}/third_party/java/BUILD.bazel" \
    "${CHECK_ROOT}/tools/java-format/BUILD.bazel"
  diff -u MODULE.bazel "${CHECK_ROOT}/MODULE.bazel"
  diff -u third_party/java/BUILD.bazel "${CHECK_ROOT}/third_party/java/BUILD.bazel"
  diff -u tools/java-format/BUILD.bazel "${CHECK_ROOT}/tools/java-format/BUILD.bazel"
else
  generate "${ROOT}"
  bazel mod deps --lockfile_mode=update
  bazel run //third_party/java:update_depgen_generated_outputs
  bazel mod deps --lockfile_mode=update
  bazel run //:buildifier -- MODULE.bazel third_party/java/BUILD.bazel tools/java-format/BUILD.bazel
fi
