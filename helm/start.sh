#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-}"
NAMESPACE="${NAMESPACE:-test-afs}"
RELEASE_NAME="${RELEASE_NAME:-afs}"
CHART_PATH="${CHART_PATH:-${SCRIPT_DIR}/afs}"

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

GIT_SHA="$(git rev-parse --short=12 HEAD)"
DIRTY=0
if [[ -n "$(git status --porcelain)" ]]; then
  DIRTY=1
  IMAGE_TAG="${GIT_SHA}-dirty"
else
  IMAGE_TAG="${GIT_SHA}"
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
# For debug convenience, default all Deployments to 1 replica unless caller overrides.
if ! has_set_override "discovery.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set discovery.replicas=1)
fi
if ! has_set_override "afslet.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set afslet.replicas=1)
fi
if ! has_set_override "afsProxy.replicas" "$@"; then
  EXTRA_HELM_ARGS+=(--set afsProxy.replicas=1)
fi
echo "[start] debug scale defaults: discovery=1 afslet=1 afsProxy=1 (override via --set ...replicas=)"

# If tag is "sha-dirty", force pulling unless caller already set image.pullPolicy.
if [[ "${DIRTY}" -eq 1 ]]; then
  if ! has_set_override "image.pullPolicy" "$@"; then
    EXTRA_HELM_ARGS+=(--set image.pullPolicy=Always)
    echo "[start] dirty workspace detected; auto set image.pullPolicy=Always"
  fi
fi

helm upgrade --install "${RELEASE_NAME}" "${CHART_PATH}" \
  -n "${NAMESPACE}" \
  --create-namespace \
  --set "image.repository=${IMAGE_REPOSITORY}" \
  --set "image.tag=${IMAGE_TAG}" \
  "${EXTRA_HELM_ARGS[@]}" \
  "$@"

kubectl -n "${NAMESPACE}" get deploy,ds,svc

echo "[start] done"
