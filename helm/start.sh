#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

print_env_template() {
  cat <<'EOF'
[start] missing env file: helm/.env
Create helm/.env with:

IMAGE_REPOSITORY=registry.example.com/org/afs
NAMESPACE=test-afs
RELEASE_NAME=afs
CHART_PATH=./helm/afs
DISCOVERY_REPLICAS=3
AFS_PROXY_REPLICAS=3
AFSLET_REPLICAS=3
AFSLET_LIMIT_CPU_CORES=4
AFSLET_LIMIT_MEMORY_MB=16384
AFSLET_REQUEST_CPU=4
AFSLET_REQUEST_MEMORY=16Gi
AFSLET_LIMIT_CPU=4
AFSLET_LIMIT_MEMORY=16Gi
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

IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-}"
NAMESPACE="${NAMESPACE:-test-afs}"
RELEASE_NAME="${RELEASE_NAME:-afs}"
CHART_PATH="${CHART_PATH:-${SCRIPT_DIR}/afs}"
DISCOVERY_REPLICAS="${DISCOVERY_REPLICAS:-3}"
AFS_PROXY_REPLICAS="${AFS_PROXY_REPLICAS:-3}"
AFSLET_REPLICAS="${AFSLET_REPLICAS:-3}"
AFSLET_LIMIT_CPU_CORES="${AFSLET_LIMIT_CPU_CORES:-4}"
AFSLET_LIMIT_MEMORY_MB="${AFSLET_LIMIT_MEMORY_MB:-16384}"
AFSLET_REQUEST_CPU="${AFSLET_REQUEST_CPU:-4}"
AFSLET_REQUEST_MEMORY="${AFSLET_REQUEST_MEMORY:-16Gi}"
AFSLET_LIMIT_CPU="${AFSLET_LIMIT_CPU:-4}"
AFSLET_LIMIT_MEMORY="${AFSLET_LIMIT_MEMORY:-16Gi}"

cd "${REPO_ROOT}"

if ! command -v git >/dev/null 2>&1; then
  echo "git not found" >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found" >&2
  exit 1
fi
if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found" >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi
if [[ -z "${IMAGE_REPOSITORY}" ]]; then
  echo "IMAGE_REPOSITORY is required, e.g. IMAGE_REPOSITORY=registry.example.com/org/afs" >&2
  exit 1
fi

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

GIT_SHA="$(git rev-parse --short=12 HEAD)"
BUILD_TIME="$(date '+%Y%m%d%H%M%S')"
DIRTY=0
if [[ -n "$(git status --porcelain)" ]]; then
  DIRTY=1
  IMAGE_TAG="${GIT_SHA}-${BUILD_TIME}-dirty"
else
  IMAGE_TAG="${GIT_SHA}-${BUILD_TIME}"
fi

IMAGE="${IMAGE_REPOSITORY}:${IMAGE_TAG}"

echo "[start] repository=${IMAGE_REPOSITORY}"
echo "[start] image-tag=${IMAGE_TAG}"
echo "[start] image=${IMAGE}"
echo "[start] namespace=${NAMESPACE} release=${RELEASE_NAME}"

docker build -t "${IMAGE}" "${REPO_ROOT}"
docker push "${IMAGE}"

has_set_override() {
  local key="$1"
  local prev_set=0
  local arg
  for arg in "$@"; do
    if [[ "${prev_set}" -eq 1 ]]; then
      [[ "${arg}" == "${key}="* ]] && return 0
      prev_set=0
      continue
    fi
    case "${arg}" in
      --set|--set-string)
        prev_set=1
        ;;
      --set="${key}="*|--set-string="${key}="*)
        return 0
        ;;
    esac
  done
  return 1
}

EXTRA_HELM_ARGS=()
# Deployment defaults.
if ! has_set_override "discovery.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set "discovery.replicas=${DISCOVERY_REPLICAS}")
fi
if ! has_set_override "afslet.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set "afslet.replicas=${AFSLET_REPLICAS}")
fi
if ! has_set_override "afsProxy.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set "afsProxy.replicas=${AFS_PROXY_REPLICAS}")
fi
if ! has_set_override "afslet.limitCPUCores" "$@"; then
  EXTRA_HELM_ARGS+=(--set "afslet.limitCPUCores=${AFSLET_LIMIT_CPU_CORES}")
fi
if ! has_set_override "afslet.limitMemoryMB" "$@"; then
  EXTRA_HELM_ARGS+=(--set "afslet.limitMemoryMB=${AFSLET_LIMIT_MEMORY_MB}")
fi
if ! has_set_override "afslet.resources.requests.cpu" "$@"; then
  EXTRA_HELM_ARGS+=(--set-string "afslet.resources.requests.cpu=${AFSLET_REQUEST_CPU}")
fi
if ! has_set_override "afslet.resources.requests.memory" "$@"; then
  EXTRA_HELM_ARGS+=(--set-string "afslet.resources.requests.memory=${AFSLET_REQUEST_MEMORY}")
fi
if ! has_set_override "afslet.resources.limits.cpu" "$@"; then
  EXTRA_HELM_ARGS+=(--set-string "afslet.resources.limits.cpu=${AFSLET_LIMIT_CPU}")
fi
if ! has_set_override "afslet.resources.limits.memory" "$@"; then
  EXTRA_HELM_ARGS+=(--set-string "afslet.resources.limits.memory=${AFSLET_LIMIT_MEMORY}")
fi
echo "[start] replicas: discovery=${DISCOVERY_REPLICAS} afslet=${AFSLET_REPLICAS} afsProxy=${AFS_PROXY_REPLICAS}"
echo "[start] afslet limits: cpu_cores=${AFSLET_LIMIT_CPU_CORES} memory_mb=${AFSLET_LIMIT_MEMORY_MB}"
echo "[start] afslet resources: requests(cpu=${AFSLET_REQUEST_CPU},mem=${AFSLET_REQUEST_MEMORY}) limits(cpu=${AFSLET_LIMIT_CPU},mem=${AFSLET_LIMIT_MEMORY})"

# If tag is "sha-dirty", force pulling unless caller already set image.pullPolicy.
if [[ "${DIRTY}" -eq 1 ]]; then
  if ! has_set_override "image.pullPolicy" "$@"; then
    EXTRA_HELM_ARGS+=(--set image.pullPolicy=Always)
    echo "[start] dirty workspace detected; auto set image.pullPolicy=Always"
  fi
fi

run_without_proxy helm upgrade --install "${RELEASE_NAME}" "${CHART_PATH}" \
  -n "${NAMESPACE}" \
  --create-namespace \
  --set "image.repository=${IMAGE_REPOSITORY}" \
  --set "image.tag=${IMAGE_TAG}" \
  "${EXTRA_HELM_ARGS[@]}" \
  "$@"

run_without_proxy kubectl -n "${NAMESPACE}" get deploy,ds,svc

echo "[start] done"
