#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/linux_amd64"
AFS_CLI="${DIST_DIR}/afs_cli"

MODE=""
IMAGE="${IMAGE:-registry.k8s.io/pause}"
TAG="${TAG:-3.9}"
REPLICA="${REPLICA:-1}"
ITERATIONS="${ITERATIONS:-1}"
GRPC_TIMEOUT="${GRPC_TIMEOUT:-5m}"

DISCOVERY_PORT="${DISCOVERY_PORT:-16051}"
LAYERSTORE_PORT="${LAYERSTORE_PORT:-15051}"
AFSLET_PORT="${AFSLET_PORT:-16151}"
PROXY_GRPC_PORT="${PROXY_GRPC_PORT:-16251}"
PROXY_HTTP_PORT="${PROXY_HTTP_PORT:-16252}"

TIMESTAMP="$(date '+%Y%m%d%H%M%S')"
LOG_DIR="${ROOT_DIR}/.tmp/e2e/reconcile-pull-benchmark/${TIMESTAMP}"
RAW_CSV="${LOG_DIR}/raw.csv"
SUMMARY_CSV="${LOG_DIR}/summary.csv"
COMPARISON_CSV="${LOG_DIR}/comparison.csv"

IMAGE_REF=""
PIDS=()
COMPOSE_ACTIVE=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/e2e/reconcile_pull_benchmark.sh --mode raw
  ./scripts/e2e/reconcile_pull_benchmark.sh --mode compose

Modes:
  raw|bare  Start discovery/layerstore/afslet/afs_proxy as local processes.
  compose   Reset docker compose stack, then benchmark against afs_proxy.

Benchmarks:
  1. local docker pull
  2. afs_cli reconcile-image-replica

Options:
  --mode <raw|bare|compose>
  --image <name>            Default: registry.k8s.io/pause
  --tag <tag>               Default: 3.9
  --replica <n>             Default: 1
  --iterations <n>          Default: 1
  --grpc-timeout <dur>      Default: 5m
  --help

Notes:
  - Build local binaries first: make build-local
  - compose mode runs docker compose down -v before each iteration to keep AFS cold
  - docker pull runs after removing the local image reference to keep Docker cold
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

timestamp_ms() {
  date +%s%3N
}

reset_processes() {
  local idx
  for ((idx = ${#PIDS[@]} - 1; idx >= 0; idx--)); do
    kill "${PIDS[idx]}" >/dev/null 2>&1 || true
  done
  for ((idx = ${#PIDS[@]} - 1; idx >= 0; idx--)); do
    wait "${PIDS[idx]}" >/dev/null 2>&1 || true
  done
  PIDS=()
}

compose_down_with_volumes() {
  (
    cd "${ROOT_DIR}"
    run_without_proxy docker compose down -v
  ) >/dev/null 2>&1 || true
  COMPOSE_ACTIVE=0
}

cleanup() {
  set +e
  reset_processes
  if [[ "${COMPOSE_ACTIVE}" -eq 1 ]]; then
    compose_down_with_volumes
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
    --iterations)
      [[ $# -ge 2 ]] || die "--iterations requires a value"
      ITERATIONS="$2"
      shift 2
      ;;
    --grpc-timeout)
      [[ $# -ge 2 ]] || die "--grpc-timeout requires a value"
      GRPC_TIMEOUT="$2"
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
  *)
    die "unsupported mode: ${MODE}"
    ;;
esac

[[ "${ITERATIONS}" =~ ^[0-9]+$ ]] || die "--iterations must be a positive integer"
[[ "${ITERATIONS}" -gt 0 ]] || die "--iterations must be > 0"
[[ "${REPLICA}" =~ ^[0-9]+$ ]] || die "--replica must be a non-negative integer"
[[ "${GRPC_TIMEOUT}" != "0" ]] || die "--grpc-timeout must be > 0"

IMAGE_REF="${IMAGE}:${TAG}"

require_cmd bash
require_cmd docker
require_cmd python3
[[ -x "${AFS_CLI}" ]] || die "missing ${AFS_CLI}, run: make build-local"
if [[ "${MODE}" == "compose" ]]; then
  run_without_proxy docker compose version >/dev/null 2>&1 || die "docker compose is required"
fi

mkdir -p "${LOG_DIR}"
printf 'mode,scenario,iteration,elapsed_ms\n' >"${RAW_CSV}"

append_result() {
  local mode="$1"
  local scenario="$2"
  local iteration="$3"
  local elapsed_ms="$4"
  printf '%s,%s,%s,%s\n' "${mode}" "${scenario}" "${iteration}" "${elapsed_ms}" >>"${RAW_CSV}"
}

start_local_process() {
  local log_path="$1"
  shift
  run_without_proxy "$@" >"${log_path}" 2>&1 &
  PIDS+=("$!")
}

wait_for_proxy_ready() {
  local target="$1"
  local iter_dir="$2"
  local output=""
  local i
  for ((i = 1; i <= 120; i++)); do
    if output="$(run_without_proxy "${AFS_CLI}" -addr "${target}" -proxy-status -grpc-timeout 20s 2>&1)"; then
      if grep -q '^summary ' <<<"${output}" && grep -q 'reachable=true' <<<"${output}" && grep -q '^layerstore ' <<<"${output}"; then
        printf '%s\n' "${output}" >"${iter_dir}/proxy-status.txt"
        return 0
      fi
    fi
    sleep 2
  done
  printf '%s\n' "${output}" >"${iter_dir}/proxy-status-last.txt"
  die "proxy status never became ready; see ${iter_dir}"
}

remove_local_docker_image() {
  local iter_dir="$1"
  local remove_targets=("${IMAGE_REF}")
  local repo_digests
  repo_digests="$(run_without_proxy docker image inspect "${IMAGE_REF}" --format '{{range .RepoDigests}}{{println .}}{{end}}' 2>/dev/null || true)"
  if [[ -n "${repo_digests}" ]]; then
    while IFS= read -r digest; do
      [[ -n "${digest}" ]] || continue
      remove_targets+=("${digest}")
    done <<<"${repo_digests}"
  fi
  run_without_proxy docker image rm -f "${remove_targets[@]}" \
    >"${iter_dir}/docker-image-rm.log" 2>&1 || true
}

measure_docker_pull() {
  local mode="$1"
  local iteration="$2"
  local iter_dir="$3"
  local start_ms end_ms elapsed_ms

  remove_local_docker_image "${iter_dir}"

  start_ms="$(timestamp_ms)"
  run_without_proxy docker pull "${IMAGE_REF}" \
    >"${iter_dir}/docker-pull.txt" 2>&1
  end_ms="$(timestamp_ms)"
  elapsed_ms="$((end_ms - start_ms))"

  append_result "${mode}" "docker-pull" "${iteration}" "${elapsed_ms}"
  printf '[compare] mode=%s iteration=%s docker-pull elapsed_ms=%s\n' "${mode}" "${iteration}" "${elapsed_ms}"
}

measure_reconcile() {
  local mode="$1"
  local target="$2"
  local iteration="$3"
  local iter_dir="$4"
  local start_ms end_ms elapsed_ms
  local output

  start_ms="$(timestamp_ms)"
  output="$(run_without_proxy "${AFS_CLI}" reconcile-image-replica \
    -addr "${target}" \
    -image "${IMAGE}" \
    -tag "${TAG}" \
    -replica "${REPLICA}" \
    -grpc-timeout "${GRPC_TIMEOUT}")"
  end_ms="$(timestamp_ms)"
  elapsed_ms="$((end_ms - start_ms))"

  grep -q "requested_replica=${REPLICA}" <<<"${output}" || die "unexpected requested replica in ${iter_dir}/reconcile-image.txt"
  grep -q 'ensured=true' <<<"${output}" || die "image replica was not ensured in ${iter_dir}/reconcile-image.txt"
  printf '%s\n' "${output}" >"${iter_dir}/reconcile-image.txt"

  append_result "${mode}" "reconcile-image-replica" "${iteration}" "${elapsed_ms}"
  printf '[compare] mode=%s iteration=%s reconcile-image-replica elapsed_ms=%s\n' "${mode}" "${iteration}" "${elapsed_ms}"
}

run_raw_iteration() {
  local iteration="$1"
  local iter_dir="${LOG_DIR}/raw/iter-${iteration}"
  mkdir -p "${iter_dir}/cache" "${iter_dir}/temp"

  measure_docker_pull "raw" "${iteration}" "${iter_dir}"

  start_local_process "${iter_dir}/discovery.log" \
    "${DIST_DIR}/afs_discovery_grpcd" \
    -listen "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${DISCOVERY_PORT}" discovery

  start_local_process "${iter_dir}/layerstore.log" \
    "${DIST_DIR}/afs_layerstore_grpcd" \
    -listen "127.0.0.1:${LAYERSTORE_PORT}" \
    -cache-dir "${iter_dir}/cache" \
    -node-id e2e-raw \
    -discovery-endpoint "127.0.0.1:${DISCOVERY_PORT}"
  wait_for_tcp 127.0.0.1 "${LAYERSTORE_PORT}" layerstore

  start_local_process "${iter_dir}/afslet.log" \
    "${DIST_DIR}/afslet" \
    -listen "127.0.0.1:${AFSLET_PORT}" \
    -mount-binary "${DIST_DIR}/afs_mount" \
    -runc-binary "${DIST_DIR}/afs_runc" \
    -discovery-addr "127.0.0.1:${DISCOVERY_PORT}" \
    -temp-dir "${iter_dir}/temp" \
    -limit-cpu 4 \
    -limit-memory-mb 4096
  wait_for_tcp 127.0.0.1 "${AFSLET_PORT}" afslet

  start_local_process "${iter_dir}/afs_proxy.log" \
    "${DIST_DIR}/afs_proxy" \
    -listen "127.0.0.1:${PROXY_GRPC_PORT}" \
    -http-listen "127.0.0.1:${PROXY_HTTP_PORT}" \
    -afslet-target "127.0.0.1:${AFSLET_PORT}" \
    -discovery-target "127.0.0.1:${DISCOVERY_PORT}" \
    -proxy-peers-target "127.0.0.1:${PROXY_HTTP_PORT}" \
    -node-id e2e-raw
  wait_for_tcp 127.0.0.1 "${PROXY_GRPC_PORT}" afs_proxy

  wait_for_proxy_ready "127.0.0.1:${PROXY_GRPC_PORT}" "${iter_dir}"
  measure_reconcile "raw" "127.0.0.1:${PROXY_GRPC_PORT}" "${iteration}" "${iter_dir}"

  reset_processes
}

build_compose_runtime_image() {
  printf '[compare] building local compose image afs-local:compose\n'
  run_without_proxy docker build -t afs-local:compose "${ROOT_DIR}" \
    >"${LOG_DIR}/docker-build.log" 2>&1
}

run_compose_iteration() {
  local iteration="$1"
  local iter_dir="${LOG_DIR}/compose/iter-${iteration}"
  mkdir -p "${iter_dir}"

  measure_docker_pull "compose" "${iteration}" "${iter_dir}"

  compose_down_with_volumes
  (
    cd "${ROOT_DIR}"
    run_without_proxy docker compose up -d
  ) >"${iter_dir}/docker-compose-up.log" 2>&1
  COMPOSE_ACTIVE=1

  wait_for_tcp 127.0.0.1 62051 afs_proxy 240
  wait_for_proxy_ready "127.0.0.1:62051" "${iter_dir}"
  measure_reconcile "compose" "127.0.0.1:62051" "${iteration}" "${iter_dir}"

  compose_down_with_volumes
}

write_reports() {
  python3 - "${RAW_CSV}" "${SUMMARY_CSV}" "${COMPARISON_CSV}" <<'PY'
import csv
import statistics
import sys

raw_path, summary_path, comparison_path = sys.argv[1:4]
rows = list(csv.DictReader(open(raw_path, newline="")))

groups = {}
for row in rows:
    key = (row["mode"], row["scenario"])
    groups.setdefault(key, []).append(int(row["elapsed_ms"]))

with open(summary_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["mode", "scenario", "count", "avg_ms", "median_ms", "min_ms", "max_ms"])
    for (mode, scenario), values in sorted(groups.items()):
        writer.writerow([
            mode,
            scenario,
            len(values),
            f"{sum(values)/len(values):.2f}",
            f"{statistics.median(values):.2f}",
            min(values),
            max(values),
        ])

with open(comparison_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "mode",
        "reconcile_avg_ms",
        "docker_pull_avg_ms",
        "reconcile_minus_docker_pull_ms",
        "reconcile_over_docker_pull_ratio",
    ])
    for mode in sorted({row["mode"] for row in rows}):
        reconcile = groups.get((mode, "reconcile-image-replica"), [])
        docker_pull = groups.get((mode, "docker-pull"), [])
        if not reconcile or not docker_pull:
            continue
        reconcile_avg = sum(reconcile) / len(reconcile)
        docker_pull_avg = sum(docker_pull) / len(docker_pull)
        writer.writerow([
            mode,
            f"{reconcile_avg:.2f}",
            f"{docker_pull_avg:.2f}",
            f"{reconcile_avg - docker_pull_avg:.2f}",
            f"{reconcile_avg / docker_pull_avg:.3f}" if docker_pull_avg else "",
        ])
PY
}

printf '[compare] mode=%s image=%s replica=%s iterations=%s log_dir=%s\n' "${MODE}" "${IMAGE_REF}" "${REPLICA}" "${ITERATIONS}" "${LOG_DIR}"

if [[ "${MODE}" == "compose" ]]; then
  build_compose_runtime_image
fi

for iteration in $(seq 1 "${ITERATIONS}"); do
  printf '[compare] running mode=%s iteration=%s/%s\n' "${MODE}" "${iteration}" "${ITERATIONS}"
  case "${MODE}" in
    raw)
      run_raw_iteration "${iteration}"
      ;;
    compose)
      run_compose_iteration "${iteration}"
      ;;
  esac
done

write_reports

printf 'raw=%s\n' "${RAW_CSV}"
printf 'summary=%s\n' "${SUMMARY_CSV}"
printf 'comparison=%s\n' "${COMPARISON_CSV}"
cat "${SUMMARY_CSV}"
cat "${COMPARISON_CSV}"
