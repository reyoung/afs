#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/linux_amd64"
AFS_CLI="${DIST_DIR}/afs_cli"

MODE=""
ADDR="${ADDR:-}"
NAMESPACE="${NAMESPACE:-afs}"
LOCAL_PROXY_PORT="${LOCAL_PROXY_PORT:-16351}"

DISCOVERY_PORT="${DISCOVERY_PORT:-16051}"
LAYERSTORE_PORT="${LAYERSTORE_PORT:-15051}"
AFSLET_PORT="${AFSLET_PORT:-16151}"
PROXY_GRPC_PORT="${PROXY_GRPC_PORT:-16251}"
PROXY_HTTP_PORT="${PROXY_HTTP_PORT:-16252}"

DEFAULT_CMD_IMAGE="${DEFAULT_CMD_IMAGE:-mirror.ccs.tencentyun.com/library/alpine}"
DEFAULT_CMD_TAG="${DEFAULT_CMD_TAG:-3.20}"
ENTRYPOINT_IMAGE="${ENTRYPOINT_IMAGE:-mirror.ccs.tencentyun.com/library/memcached}"
ENTRYPOINT_TAG="${ENTRYPOINT_TAG:-1.6-alpine}"
WORKDIR_IMAGE="${WORKDIR_IMAGE:-mirror.ccs.tencentyun.com/library/caddy}"
WORKDIR_TAG="${WORKDIR_TAG:-2-alpine}"

TIMESTAMP="$(date '+%Y%m%d%H%M%S')"
LOG_DIR="${ROOT_DIR}/.tmp/e2e/runtime-config/${TIMESTAMP}"
EMPTY_DIR="${LOG_DIR}/empty-dir"

PIDS=()
PORT_FORWARD_PID=""
COMPOSE_ACTIVE=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/e2e/runtime_config.sh --mode raw
  ./scripts/e2e/runtime_config.sh --mode compose
  ./scripts/e2e/runtime_config.sh --mode kubernetes --namespace afs

Modes:
  raw|bare  Start discovery/layerstore/afslet/afs_proxy as local processes.
  compose   Restart docker compose stack, then run runtime-config checks.
  helm|kubernetes|k8s
             Validate an existing Kubernetes deployment through kubectl port-forward.

Checks:
  1. image default cmd
  2. image entrypoint override
  3. image env + user
  4. image working_dir + env

Options:
  --mode <raw|bare|compose|helm|kubernetes|k8s>
  --namespace <name>        Kubernetes namespace for helm mode. Default: afs
  --addr <host:port>        Override proxy gRPC address. For helm, skips port-forward.
  --help

Notes:
  - Build local binaries first: make build-local
  - Kubernetes deployment lifecycle must use ./helm/start.sh and ./helm/stop.sh
  - Default images use mirror.ccs.tencentyun.com to avoid Docker Hub anonymous rate limits
  - Raw mode requires passwordless sudo because afs_runc executes privileged paths
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
  local idx
  for ((idx = ${#PIDS[@]} - 1; idx >= 0; idx--)); do
    kill "${PIDS[idx]}" >/dev/null 2>&1 || true
  done
  for ((idx = ${#PIDS[@]} - 1; idx >= 0; idx--)); do
    wait "${PIDS[idx]}" >/dev/null 2>&1 || true
  done
  if [[ "${COMPOSE_ACTIVE}" -eq 1 ]]; then
    (
      cd "${ROOT_DIR}"
      run_without_proxy docker compose down >/dev/null 2>&1 || true
    )
  fi
}

trap cleanup EXIT

wait_for_proxy_ready() {
  local target="$1"
  local output=""
  local i
  for ((i = 1; i <= 120; i++)); do
    if output="$(run_without_proxy "${AFS_CLI}" -addr "${target}" -proxy-status -grpc-timeout 20s 2>&1)"; then
      if grep -q '^summary ' <<<"${output}" && grep -q 'reachable=true' <<<"${output}" && grep -q '^layerstore ' <<<"${output}"; then
        printf '%s\n' "${output}" >"${LOG_DIR}/proxy-status.txt"
        return 0
      fi
    fi
    sleep 2
  done
  printf '%s\n' "${output}" >"${LOG_DIR}/proxy-status-last.txt"
  die "proxy status never became ready; see ${LOG_DIR}"
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if ! grep -Fq -- "${needle}" <<<"${haystack}"; then
    die "${label}: expected to find ${needle}"
  fi
}

run_execute_case() {
  local label="$1"
  local image="$2"
  local tag="$3"
  shift 3

  local out_path="${LOG_DIR}/${label}.tar.gz"
  local output
  if [[ $# -gt 0 ]]; then
    output="$(
      run_without_proxy "${AFS_CLI}" \
        -addr "${ADDR}" \
        -dir "${EMPTY_DIR}" \
        -image "${image}" \
        -tag "${tag}" \
        -timeout 5s \
        -out "${out_path}" \
        -- "$@" 2>&1
    )"
  else
    output="$(
      run_without_proxy "${AFS_CLI}" \
        -addr "${ADDR}" \
        -dir "${EMPTY_DIR}" \
        -image "${image}" \
        -tag "${tag}" \
        -timeout 5s \
        -out "${out_path}" 2>&1
    )"
  fi
  printf '%s\n' "${output}" >"${LOG_DIR}/${label}.txt"
  printf '[runtime-config] case=%s ok\n' "${label}"
  printf '%s\n' "${output}"
}

run_runtime_config_checks() {
  local output

  output="$(run_execute_case default-cmd "${DEFAULT_CMD_IMAGE}" "${DEFAULT_CMD_TAG}")"
  assert_contains "${output}" 'resolved_cmd=["/bin/sh"]' "default-cmd"

  output="$(run_execute_case entrypoint "${ENTRYPOINT_IMAGE}" "${ENTRYPOINT_TAG}" -h)"
  assert_contains "${output}" 'resolved_cmd=["docker-entrypoint.sh" "-h"]' "entrypoint"
  assert_contains "${output}" 'memcached 1.6.41' "entrypoint"

  output="$(run_execute_case user-env "${ENTRYPOINT_IMAGE}" "${ENTRYPOINT_TAG}" /bin/sh -lc 'id -un; id -u; pwd; printf "%s\n" "$MEMCACHED_VERSION"')"
  assert_contains "${output}" 'user="memcache"' "user-env"
  assert_contains "${output}" 'memcache' "user-env"
  assert_contains "${output}" '11211' "user-env"
  assert_contains "${output}" '1.6.41' "user-env"

  output="$(run_execute_case working-dir "${WORKDIR_IMAGE}" "${WORKDIR_TAG}" /bin/sh -lc 'pwd; printf "%s\n" "$CADDY_VERSION"')"
  assert_contains "${output}" 'cwd="/srv"' "working-dir"
  assert_contains "${output}" '/srv' "working-dir"
  assert_contains "${output}" 'v2.11.2' "working-dir"
}

start_raw_mode() {
  mkdir -p "${LOG_DIR}/raw-cache" "${LOG_DIR}/raw-temp" "${LOG_DIR}/raw-spillcache"

  start_local_process discovery \
    "${DIST_DIR}/afs_discovery_grpcd" \
    -listen "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${DISCOVERY_PORT}" discovery

  start_local_process layerstore \
    "${DIST_DIR}/afs_layerstore_grpcd" \
    -listen "127.0.0.1:${LAYERSTORE_PORT}" \
    -cache-dir "${LOG_DIR}/raw-cache" \
    -node-id e2e-runtime-raw \
    -discovery-endpoint "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${LAYERSTORE_PORT}" layerstore

  start_local_process afslet \
    "${DIST_DIR}/afslet" \
    -listen "127.0.0.1:${AFSLET_PORT}" \
    -sudo-binaries \
    -mount-binary "${DIST_DIR}/afs_mount" \
    -runc-binary "${DIST_DIR}/afs_runc" \
    -discovery-addr "127.0.0.1:${DISCOVERY_PORT}" \
    -temp-dir "${LOG_DIR}/raw-temp" \
    -limit-cpu 4 \
    -limit-memory-mb 4096 \
    -shared-spill-cache \
    -shared-spill-cache-dir "${LOG_DIR}/raw-spillcache" \
    -shared-spill-cache-binary "${DIST_DIR}/afs_mount_cached"
  wait_for_tcp 127.0.0.1 "${AFSLET_PORT}" afslet

  start_local_process afs_proxy \
    "${DIST_DIR}/afs_proxy" \
    -listen "127.0.0.1:${PROXY_GRPC_PORT}" \
    -http-listen "127.0.0.1:${PROXY_HTTP_PORT}" \
    -afslet-target "127.0.0.1:${AFSLET_PORT}" \
    -discovery-target "127.0.0.1:${DISCOVERY_PORT}" \
    -proxy-peers-target "127.0.0.1:${PROXY_HTTP_PORT}" \
    -node-id e2e-runtime-raw
  wait_for_tcp 127.0.0.1 "${PROXY_GRPC_PORT}" afs_proxy

  ADDR="127.0.0.1:${PROXY_GRPC_PORT}"
  wait_for_proxy_ready "${ADDR}"
}

start_compose_mode() {
  require_cmd docker
  run_without_proxy docker compose version >/dev/null 2>&1 || die "docker compose is required"

  printf '[runtime-config] building local compose image afs-local:compose\n'
  run_without_proxy docker build -t afs-local:compose "${ROOT_DIR}" \
    >"${LOG_DIR}/docker-build.log" 2>&1

  (
    cd "${ROOT_DIR}"
    run_without_proxy docker compose down >/dev/null 2>&1 || true
    run_without_proxy docker compose up -d >/dev/null
  )
  COMPOSE_ACTIVE=1

  wait_for_tcp 127.0.0.1 62051 afs_proxy 240
  ADDR="${ADDR:-127.0.0.1:62051}"
  wait_for_proxy_ready "${ADDR}"
}

start_helm_mode() {
  require_cmd kubectl

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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || die "--mode requires a value"
      MODE="$2"
      shift 2
      ;;
    --namespace)
      [[ $# -ge 2 ]] || die "--namespace requires a value"
      NAMESPACE="$2"
      shift 2
      ;;
    --addr)
      [[ $# -ge 2 ]] || die "--addr requires a value"
      ADDR="$2"
      shift 2
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
  raw|bare)
    MODE="raw"
    ;;
  compose) ;;
  helm|kubernetes|k8s)
    MODE="helm"
    ;;
  *)
    die "unsupported mode: ${MODE}"
    ;;
esac

require_cmd bash
mkdir -p "${LOG_DIR}" "${EMPTY_DIR}"
[[ -x "${AFS_CLI}" ]] || die "missing ${AFS_CLI}, run: make build-local"

printf '[runtime-config] mode=%s namespace=%s log_dir=%s\n' "${MODE}" "${NAMESPACE}" "${LOG_DIR}"

case "${MODE}" in
  raw)
    start_raw_mode
    ;;
  compose)
    start_compose_mode
    ;;
  helm)
    start_helm_mode
    ;;
esac

printf '[runtime-config] proxy=%s\n' "${ADDR}"
run_runtime_config_checks
printf '[runtime-config] success\n'
