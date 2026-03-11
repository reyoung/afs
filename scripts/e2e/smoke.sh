#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/linux_amd64"
AFS_CLI="${DIST_DIR}/afs_cli"

MODE=""
ADDR="${ADDR:-}"
NAMESPACE="${NAMESPACE:-afs}"
IMAGE="${IMAGE:-registry.k8s.io/pause}"
TAG="${TAG:-3.9}"
REPLICA="${REPLICA:-1}"
GRPC_TIMEOUT="${GRPC_TIMEOUT:-5m}"
SKIP_RECONCILE=0
IMAGE_EXPLICIT=0
TAG_EXPLICIT=0

DISCOVERY_PORT="${DISCOVERY_PORT:-16051}"
LAYERSTORE_PORT="${LAYERSTORE_PORT:-15051}"
AFSLET_PORT="${AFSLET_PORT:-16151}"
PROXY_GRPC_PORT="${PROXY_GRPC_PORT:-16251}"
PROXY_HTTP_PORT="${PROXY_HTTP_PORT:-16252}"
LOCAL_PROXY_PORT="${LOCAL_PROXY_PORT:-16351}"

TIMESTAMP="$(date '+%Y%m%d%H%M%S')"
LOG_DIR="${ROOT_DIR}/.tmp/e2e/${TIMESTAMP}"

PIDS=()
PORT_FORWARD_PID=""
COMPOSE_STARTED=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/e2e/smoke.sh --mode raw
  ./scripts/e2e/smoke.sh --mode compose
  ./scripts/e2e/smoke.sh --mode helm --namespace afs

Modes:
  raw|bare  Start discovery/layerstore/afslet/afs_proxy as local processes.
  compose   Start or reuse docker compose stack, then run smoke checks.
  helm      Validate an existing Kubernetes deployment through kubectl port-forward.

Checks:
  1. afs_cli -proxy-status
  2. afs_cli reconcile-image-replica

Options:
  --mode <raw|bare|compose|helm>
  --addr <host:port>        Override proxy gRPC address. For helm, skips port-forward.
  --namespace <name>        Kubernetes namespace for helm mode. Default: afs
  --image <name>            Default: registry.k8s.io/pause
  --tag <tag>               Default: 3.9
  --replica <n>             Default: 1
  --grpc-timeout <dur>      Default: 5m
  --skip-reconcile          Only run proxy-status smoke check.
  --help

Notes:
  - Build local binaries first: make build-local
  - Helm lifecycle must use ./helm/start.sh and ./helm/stop.sh
  - Helm-related kubectl calls in this script run without proxy env vars
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempts="${4:-120}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    if bash -lc "exec 3<>/dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "timed out waiting for ${label} at ${host}:${port}"
}

start_local_process() {
  local name="$1"
  shift
  run_without_proxy "$@" >"${LOG_DIR}/${name}.log" 2>&1 &
  PIDS+=("$!")
}

cleanup() {
  set +e
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
  local pid
  for ((pid = ${#PIDS[@]} - 1; pid >= 0; pid--)); do
    kill "${PIDS[pid]}" >/dev/null 2>&1 || true
  done
  for ((pid = ${#PIDS[@]} - 1; pid >= 0; pid--)); do
    wait "${PIDS[pid]}" >/dev/null 2>&1 || true
  done
  if [[ "${COMPOSE_STARTED}" -eq 1 ]]; then
    (
      cd "${ROOT_DIR}"
      run_without_proxy docker compose down >/dev/null 2>&1 || true
    )
  fi
}

trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || die "--mode requires a value"
      MODE="$2"
      shift 2
      ;;
    --addr)
      [[ $# -ge 2 ]] || die "--addr requires a value"
      ADDR="$2"
      shift 2
      ;;
    --namespace)
      [[ $# -ge 2 ]] || die "--namespace requires a value"
      NAMESPACE="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || die "--image requires a value"
      IMAGE="$2"
      IMAGE_EXPLICIT=1
      shift 2
      ;;
    --tag)
      [[ $# -ge 2 ]] || die "--tag requires a value"
      TAG="$2"
      TAG_EXPLICIT=1
      shift 2
      ;;
    --replica)
      [[ $# -ge 2 ]] || die "--replica requires a value"
      REPLICA="$2"
      shift 2
      ;;
    --grpc-timeout)
      [[ $# -ge 2 ]] || die "--grpc-timeout requires a value"
      GRPC_TIMEOUT="$2"
      shift 2
      ;;
    --skip-reconcile)
      SKIP_RECONCILE=1
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

[[ -n "${MODE}" ]] || die "--mode is required"
case "${MODE}" in
  raw|bare|compose|helm) ;;
  *) die "unsupported mode: ${MODE}" ;;
esac

mkdir -p "${LOG_DIR}"

[[ -x "${AFS_CLI}" ]] || die "missing ${AFS_CLI}, run: make build-local"

wait_for_proxy_ready() {
  local target="$1"
  local output=""
  local i
  for ((i = 1; i <= 120; i++)); do
    if output="$(run_without_proxy "${AFS_CLI}" -addr "${target}" -proxy-status -grpc-timeout 20s 2>&1)"; then
      if grep -q '^summary ' <<<"${output}" && grep -q 'reachable=true' <<<"${output}"; then
        if grep -q '^layerstore ' <<<"${output}"; then
          printf '%s\n' "${output}" >"${LOG_DIR}/proxy-status.txt"
          return 0
        fi
      fi
    fi
    sleep 2
  done
  printf '%s\n' "${output}" >"${LOG_DIR}/proxy-status-last.txt"
  die "proxy status never became ready; see ${LOG_DIR}"
}

run_status_smoke() {
  local target="$1"
  local output
  output="$(run_without_proxy "${AFS_CLI}" -addr "${target}" -proxy-status -grpc-timeout 20s)"
  grep -q '^summary ' <<<"${output}" || die "missing proxy status summary"
  grep -q 'reachable=true' <<<"${output}" || die "no reachable afslet in proxy status"
  grep -q '^layerstore ' <<<"${output}" || die "no layerstore reported in proxy status"
  printf '%s\n' "${output}" >"${LOG_DIR}/proxy-status.txt"
  printf '[e2e] proxy-status ok\n'
  printf '%s\n' "${output}"
}

run_reconcile_smoke() {
  local target="$1"
  local output
  output="$(run_without_proxy "${AFS_CLI}" reconcile-image-replica \
    -addr "${target}" \
    -image "${IMAGE}" \
    -tag "${TAG}" \
    -replica "${REPLICA}" \
    -grpc-timeout "${GRPC_TIMEOUT}")"
  grep -q "requested_replica=${REPLICA}" <<<"${output}" || die "unexpected requested replica"
  grep -q 'ensured=true' <<<"${output}" || die "image replica was not ensured"
  printf '%s\n' "${output}" >"${LOG_DIR}/reconcile-image.txt"
  printf '[e2e] reconcile-image-replica ok\n'
  printf '%s\n' "${output}"
}

detect_helm_smoke_image() {
  local full_image
  full_image="$(
    run_without_proxy kubectl -n "${NAMESPACE}" get deploy afs-afslet \
      -o jsonpath='{.spec.template.spec.containers[0].image}'
  )"
  [[ -n "${full_image}" ]] || die "failed to detect afslet image from deployment"
  [[ "${full_image}" != *@* ]] || die "detected image uses digest only, please pass --image and --tag explicitly"
  [[ "${full_image}" == *:* ]] || die "detected image has no tag, please pass --image and --tag explicitly"
  IMAGE="${full_image%:*}"
  TAG="${full_image##*:}"
  printf '[e2e] helm detected image=%s tag=%s\n' "${IMAGE}" "${TAG}"
}

start_bare_mode() {
  require_cmd bash

  mkdir -p "${LOG_DIR}/bare-cache" "${LOG_DIR}/bare-temp" "${LOG_DIR}/bare-spillcache"

  start_local_process discovery \
    "${DIST_DIR}/afs_discovery_grpcd" \
    -listen "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${DISCOVERY_PORT}" discovery

  start_local_process layerstore \
    "${DIST_DIR}/afs_layerstore_grpcd" \
    -listen "127.0.0.1:${LAYERSTORE_PORT}" \
    -cache-dir "${LOG_DIR}/bare-cache" \
    -node-id e2e-bare \
    -discovery-endpoint "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${LAYERSTORE_PORT}" layerstore

  start_local_process afslet \
    "${DIST_DIR}/afslet" \
    -listen "127.0.0.1:${AFSLET_PORT}" \
    -mount-binary "${DIST_DIR}/afs_mount" \
    -runc-binary "${DIST_DIR}/afs_runc" \
    -discovery-addr "127.0.0.1:${DISCOVERY_PORT}" \
    -temp-dir "${LOG_DIR}/bare-temp" \
    -limit-cpu 4 \
    -limit-memory-mb 4096 \
    -shared-spill-cache \
    -shared-spill-cache-dir "${LOG_DIR}/bare-spillcache" \
    -shared-spill-cache-binary "${DIST_DIR}/afs_mount_cached"
  wait_for_tcp 127.0.0.1 "${AFSLET_PORT}" afslet

  start_local_process afs_proxy \
    "${DIST_DIR}/afs_proxy" \
    -listen "127.0.0.1:${PROXY_GRPC_PORT}" \
    -http-listen "127.0.0.1:${PROXY_HTTP_PORT}" \
    -afslet-target "127.0.0.1:${AFSLET_PORT}" \
    -discovery-target "127.0.0.1:${DISCOVERY_PORT}" \
    -proxy-peers-target "127.0.0.1:${PROXY_HTTP_PORT}" \
    -node-id e2e-bare
  wait_for_tcp 127.0.0.1 "${PROXY_GRPC_PORT}" afs_proxy

  ADDR="127.0.0.1:${PROXY_GRPC_PORT}"
  wait_for_proxy_ready "${ADDR}"
}

start_compose_mode() {
  require_cmd docker
  if ! run_without_proxy docker compose version >/dev/null 2>&1; then
    die "docker compose is required"
  fi

  printf '[e2e] building local compose image afs-local:compose\n'
  run_without_proxy docker build -t afs-local:compose "${ROOT_DIR}" \
    >"${LOG_DIR}/docker-build.log" 2>&1

  if (
    cd "${ROOT_DIR}" &&
    run_without_proxy docker compose ps --status running -q afs_proxy 2>/dev/null | grep -q .
  ); then
    COMPOSE_STARTED=0
  else
    COMPOSE_STARTED=1
  fi

  (
    cd "${ROOT_DIR}" &&
    run_without_proxy docker compose up -d >/dev/null
  )

  wait_for_tcp 127.0.0.1 62051 afs_proxy 240
  ADDR="${ADDR:-127.0.0.1:62051}"
  wait_for_proxy_ready "${ADDR}"
}

start_helm_mode() {
  require_cmd kubectl

  if [[ "${IMAGE_EXPLICIT}" -eq 0 && "${TAG_EXPLICIT}" -eq 0 ]]; then
    detect_helm_smoke_image
  fi

  if [[ -n "${ADDR}" ]]; then
    wait_for_proxy_ready "${ADDR}"
    return
  fi

  local service_name
  local service_port
  service_name="$(
    run_without_proxy kubectl -n "${NAMESPACE}" get svc \
      -l app.kubernetes.io/component=afs-proxy \
      -o name | grep -v headless | head -n 1
  )"
  [[ -n "${service_name}" ]] || die "failed to find afs-proxy service in namespace ${NAMESPACE}"
  service_name="${service_name#service/}"

  service_port="$(
    run_without_proxy kubectl -n "${NAMESPACE}" get svc "${service_name}" \
      -o go-template='{{range .spec.ports}}{{if eq .name "grpc"}}{{.port}}{{end}}{{end}}'
  )"
  [[ -n "${service_port}" ]] || die "failed to resolve grpc port for service ${service_name}"

  run_without_proxy kubectl -n "${NAMESPACE}" port-forward \
    "svc/${service_name}" "${LOCAL_PROXY_PORT}:${service_port}" \
    >"${LOG_DIR}/kubectl-port-forward.log" 2>&1 &
  PORT_FORWARD_PID="$!"

  wait_for_tcp 127.0.0.1 "${LOCAL_PROXY_PORT}" kubectl-port-forward 120
  ADDR="127.0.0.1:${LOCAL_PROXY_PORT}"
  wait_for_proxy_ready "${ADDR}"
}

printf '[e2e] mode=%s image=%s:%s replica=%s log_dir=%s\n' "${MODE}" "${IMAGE}" "${TAG}" "${REPLICA}" "${LOG_DIR}"

case "${MODE}" in
  raw|bare)
    start_bare_mode
    ;;
  compose)
    start_compose_mode
    ;;
  helm)
    start_helm_mode
    ;;
esac

printf '[e2e] proxy=%s\n' "${ADDR}"
run_status_smoke "${ADDR}"
if [[ "${SKIP_RECONCILE}" -eq 0 ]]; then
  run_reconcile_smoke "${ADDR}"
else
  printf '[e2e] reconcile-image-replica skipped\n'
fi

printf '[e2e] success\n'
