#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

print_env_template() {
  cat <<'EOF'
[stop] missing env file: helm/.env
Create helm/.env with:

NAMESPACE=test-afs
RELEASE_NAME=afs
EOF
}

if [[ ! -f "${ENV_FILE}" ]]; then
  print_env_template
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

NAMESPACE="${NAMESPACE:-test-afs}"
RELEASE_NAME="${RELEASE_NAME:-afs}"

if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found" >&2
  exit 1
fi

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

echo "[stop] namespace=${NAMESPACE} release=${RELEASE_NAME}"
run_without_proxy helm uninstall "${RELEASE_NAME}" -n "${NAMESPACE}" "$@"
echo "[stop] done"
