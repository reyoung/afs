#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_ROOT="${ROOT_DIR}/.tmp/e2e/reconcile-regression/$(date '+%Y%m%d%H%M%S')"

GO_COUNT="${GO_COUNT:-10}"
BARE_COUNT="${BARE_COUNT:-3}"
HELM_COUNT="${HELM_COUNT:-3}"
REPLICA="${REPLICA:-1}"
GRPC_TIMEOUT="${GRPC_TIMEOUT:-5m}"
NAMESPACE="${NAMESPACE:-afs}"
RELEASE_NAME="${RELEASE_NAME:-afs}"
HELM_ROLLOUT_TIMEOUT="${HELM_ROLLOUT_TIMEOUT:-15m}"
HELM_SETTLE_SECONDS="${HELM_SETTLE_SECONDS:-15}"
HELM_DAEMONSET_MAX_UNAVAILABLE="${HELM_DAEMONSET_MAX_UNAVAILABLE:-1}"
IMAGE="${IMAGE:-}"
TAG="${TAG:-}"

SKIP_BUILD=0
SKIP_GO_TESTS=0
SKIP_BARE=0
SKIP_HELM=0
HELM_START_STOP=0
STOP_HELM_ON_EXIT=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/e2e/reconcile_regression.sh [options]

Runs repeated reconcile-image regression checks:
  1. Go reconcile-image tests with higher -count
  2. repeated bare smoke runs
  3. repeated helm smoke runs (optional)

Options:
  --image <name>           Image name for smoke checks. If omitted, detect from afs-afslet deployment.
  --tag <tag>              Image tag for smoke checks. If omitted, detect from afs-afslet deployment.
  --replica <n>            Requested replica count. Default: 1
  --go-count <n>           Go test -count value. Default: 10
  --bare-count <n>         Number of bare smoke runs. Default: 3
  --helm-count <n>         Number of helm smoke runs. Default: 3
  --grpc-timeout <dur>     Passed through to smoke.sh. Default: 5m
  --namespace <name>       Kubernetes namespace. Default: afs
  --release-name <name>    Helm release name. Default: afs
  --helm-rollout-timeout <dur>
                           Wait timeout for deployment/daemonset rollout. Default: 15m
  --helm-settle-seconds <n>
                           Extra settle time after rollout. Default: 15
  --helm-daemonset-max-unavailable <n>
                           Allowed unavailable daemonset pods after update. Default: 1
  --helm-start-stop        Use ./helm/start.sh before helm checks and ./helm/stop.sh on exit.
  --skip-build             Skip make build-local
  --skip-go-tests          Skip go test regression loop
  --skip-bare              Skip bare smoke loop
  --skip-helm              Skip helm smoke loop
  --help

Notes:
  - Helm lifecycle uses ./helm/start.sh and ./helm/stop.sh only.
  - Helm smoke must use mirrors.tencent.com images. This script enforces that for explicit --image.
  - Logs are written under .tmp/e2e/reconcile-regression/<timestamp>/
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

ensure_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

duration_to_seconds() {
  local raw="$1"
  case "${raw}" in
    *h) echo "$(( 3600 * ${raw%h} ))" ;;
    *m) echo "$(( 60 * ${raw%m} ))" ;;
    *s) echo "${raw%s}" ;;
    *) echo "${raw}" ;;
  esac
}

wait_for_rollout_resources() {
  local kind="$1"
  local selector="app.kubernetes.io/instance=${RELEASE_NAME}"
  local resources=()

  mapfile -t resources < <(run_without_proxy kubectl -n "${NAMESPACE}" get "${kind}" -l "${selector}" -o name)
  [[ "${#resources[@]}" -gt 0 ]] || die "no ${kind} resources found for release ${RELEASE_NAME} in namespace ${NAMESPACE}"

  local resource
  for resource in "${resources[@]}"; do
    echo "[reconcile-regression] waiting for ${resource} rollout"
    run_without_proxy kubectl -n "${NAMESPACE}" rollout status "${resource}" --timeout "${HELM_ROLLOUT_TIMEOUT}"
  done
}

wait_for_daemonsets_ready() {
  local selector="app.kubernetes.io/instance=${RELEASE_NAME}"
  local resources=()
  mapfile -t resources < <(run_without_proxy kubectl -n "${NAMESPACE}" get daemonset -l "${selector}" -o name)
  [[ "${#resources[@]}" -gt 0 ]] || die "no daemonset resources found for release ${RELEASE_NAME} in namespace ${NAMESPACE}"

  local deadline
  deadline="$(( $(date +%s) + $(duration_to_seconds "${HELM_ROLLOUT_TIMEOUT}") ))"

  local resource
  for resource in "${resources[@]}"; do
    while true; do
      local stats
      stats="$(
        run_without_proxy kubectl -n "${NAMESPACE}" get "${resource}" \
          -o jsonpath='{.status.desiredNumberScheduled} {.status.updatedNumberScheduled} {.status.numberReady} {.status.numberAvailable}'
      )"
      local desired updated ready available unavailable
      read -r desired updated ready available <<<"${stats}"
      desired="${desired:-0}"
      updated="${updated:-0}"
      ready="${ready:-0}"
      available="${available:-0}"
      unavailable="$(( desired - available ))"
      if [[ "${updated}" -eq "${desired}" && "${unavailable}" -le "${HELM_DAEMONSET_MAX_UNAVAILABLE}" ]]; then
        echo "[reconcile-regression] daemonset ready ${resource} desired=${desired} updated=${updated} ready=${ready} available=${available} unavailable=${unavailable}"
        break
      fi
      if [[ "$(date +%s)" -ge "${deadline}" ]]; then
        die "timed out waiting for ${resource}: desired=${desired} updated=${updated} ready=${ready} available=${available} unavailable=${unavailable}"
      fi
      echo "[reconcile-regression] waiting for ${resource}: desired=${desired} updated=${updated} ready=${ready} available=${available} unavailable=${unavailable}"
      sleep 5
    done
  done
}

wait_for_helm_release_ready() {
  ensure_cmd kubectl
  wait_for_rollout_resources deploy
  wait_for_daemonsets_ready
  if [[ "${HELM_SETTLE_SECONDS}" =~ ^[0-9]+$ ]] && [[ "${HELM_SETTLE_SECONDS}" -gt 0 ]]; then
    echo "[reconcile-regression] settling ${HELM_SETTLE_SECONDS}s for discovery/layerstore heartbeats"
    sleep "${HELM_SETTLE_SECONDS}"
  fi
}

cleanup() {
  if [[ "${STOP_HELM_ON_EXIT}" -eq 1 ]]; then
    (
      cd "${ROOT_DIR}"
      ./helm/stop.sh
    ) >"${LOG_ROOT}/helm-stop.log" 2>&1 || true
  fi
}

trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      [[ $# -ge 2 ]] || die "--image requires a value"
      IMAGE="$2"
      shift 2
      ;;
    --tag)
      [[ $# -ge 2 ]] || die "--tag requires a value"
      TAG="$2"
      shift 2
      ;;
    --replica)
      [[ $# -ge 2 ]] || die "--replica requires a value"
      REPLICA="$2"
      shift 2
      ;;
    --go-count)
      [[ $# -ge 2 ]] || die "--go-count requires a value"
      GO_COUNT="$2"
      shift 2
      ;;
    --bare-count)
      [[ $# -ge 2 ]] || die "--bare-count requires a value"
      BARE_COUNT="$2"
      shift 2
      ;;
    --helm-count)
      [[ $# -ge 2 ]] || die "--helm-count requires a value"
      HELM_COUNT="$2"
      shift 2
      ;;
    --grpc-timeout)
      [[ $# -ge 2 ]] || die "--grpc-timeout requires a value"
      GRPC_TIMEOUT="$2"
      shift 2
      ;;
    --namespace)
      [[ $# -ge 2 ]] || die "--namespace requires a value"
      NAMESPACE="$2"
      shift 2
      ;;
    --release-name)
      [[ $# -ge 2 ]] || die "--release-name requires a value"
      RELEASE_NAME="$2"
      shift 2
      ;;
    --helm-rollout-timeout)
      [[ $# -ge 2 ]] || die "--helm-rollout-timeout requires a value"
      HELM_ROLLOUT_TIMEOUT="$2"
      shift 2
      ;;
    --helm-settle-seconds)
      [[ $# -ge 2 ]] || die "--helm-settle-seconds requires a value"
      HELM_SETTLE_SECONDS="$2"
      shift 2
      ;;
    --helm-daemonset-max-unavailable)
      [[ $# -ge 2 ]] || die "--helm-daemonset-max-unavailable requires a value"
      HELM_DAEMONSET_MAX_UNAVAILABLE="$2"
      shift 2
      ;;
    --helm-start-stop)
      HELM_START_STOP=1
      shift
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-go-tests)
      SKIP_GO_TESTS=1
      shift
      ;;
    --skip-bare)
      SKIP_BARE=1
      shift
      ;;
    --skip-helm)
      SKIP_HELM=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

ensure_cmd go
ensure_cmd bash
mkdir -p "${LOG_ROOT}"

detect_deployed_image() {
  local full_image
  full_image="$(
    run_without_proxy kubectl -n "${NAMESPACE}" get deploy \
      -l "app.kubernetes.io/instance=${RELEASE_NAME},app.kubernetes.io/component=afslet" \
      -o jsonpath='{.items[0].spec.template.spec.containers[0].image}'
  )"
  [[ -n "${full_image}" ]] || die "failed to detect afslet image from namespace ${NAMESPACE} release ${RELEASE_NAME}"
  [[ "${full_image}" != *@* ]] || die "detected image uses digest only, pass --image and --tag explicitly"
  [[ "${full_image}" == *:* ]] || die "detected image has no tag, pass --image and --tag explicitly"
  IMAGE="${full_image%:*}"
  TAG="${full_image##*:}"
}

if [[ "${HELM_START_STOP}" -eq 1 ]]; then
  (
    cd "${ROOT_DIR}"
    ./helm/start.sh
  ) | tee "${LOG_ROOT}/helm-start.log"
  STOP_HELM_ON_EXIT=1
fi

if [[ -z "${IMAGE}" || -z "${TAG}" ]]; then
  ensure_cmd kubectl
  detect_deployed_image
fi

if [[ "${SKIP_HELM}" -eq 0 && "${IMAGE}" != mirrors.tencent.com/* ]]; then
  die "helm smoke requires mirrors.tencent.com image, got ${IMAGE}:${TAG}"
fi

echo "[reconcile-regression] log_dir=${LOG_ROOT}"
echo "[reconcile-regression] image=${IMAGE}:${TAG} replica=${REPLICA}"
echo "[reconcile-regression] go_count=${GO_COUNT} bare_count=${BARE_COUNT} helm_count=${HELM_COUNT}"
echo "[reconcile-regression] helm_release=${RELEASE_NAME} rollout_timeout=${HELM_ROLLOUT_TIMEOUT} settle_seconds=${HELM_SETTLE_SECONDS} daemonset_max_unavailable=${HELM_DAEMONSET_MAX_UNAVAILABLE}"

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  (
    cd "${ROOT_DIR}"
    make build-local
  ) | tee "${LOG_ROOT}/build-local.log"
fi

if [[ "${SKIP_GO_TESTS}" -eq 0 ]]; then
  (
    cd "${ROOT_DIR}"
    go test ./pkg/afsproxy -run 'TestReconcileImageReplica' -count "${GO_COUNT}" -v
  ) | tee "${LOG_ROOT}/go-test-reconcile.log"
fi

if [[ "${SKIP_HELM}" -eq 0 ]]; then
  wait_for_helm_release_ready | tee "${LOG_ROOT}/helm-rollout.log"
fi

if [[ "${SKIP_BARE}" -eq 0 ]]; then
  for i in $(seq 1 "${BARE_COUNT}"); do
    echo "[reconcile-regression] bare iteration ${i}/${BARE_COUNT}"
    (
      cd "${ROOT_DIR}"
      ./scripts/e2e/smoke.sh \
        --mode bare \
        --image "${IMAGE}" \
        --tag "${TAG}" \
        --replica "${REPLICA}" \
        --grpc-timeout "${GRPC_TIMEOUT}"
    ) | tee "${LOG_ROOT}/bare-${i}.log"
  done
fi

if [[ "${SKIP_HELM}" -eq 0 ]]; then
  for i in $(seq 1 "${HELM_COUNT}"); do
    echo "[reconcile-regression] helm iteration ${i}/${HELM_COUNT}"
    (
      cd "${ROOT_DIR}"
      ./scripts/e2e/smoke.sh \
        --mode helm \
        --namespace "${NAMESPACE}" \
        --image "${IMAGE}" \
        --tag "${TAG}" \
        --replica "${REPLICA}" \
        --grpc-timeout "${GRPC_TIMEOUT}"
    ) | tee "${LOG_ROOT}/helm-${i}.log"
  done
fi

echo "[reconcile-regression] success"
