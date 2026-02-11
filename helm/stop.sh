#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NAMESPACE="${NAMESPACE:-test-afs}"
RELEASE_NAME="${RELEASE_NAME:-afs}"

if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found" >&2
  exit 1
fi

echo "[stop] namespace=${NAMESPACE} release=${RELEASE_NAME}"
helm uninstall "${RELEASE_NAME}" -n "${NAMESPACE}" "$@"
echo "[stop] done"
