#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/linux_amd64"
AFS_CLI="${DIST_DIR}/afs_cli"
ABORT_CLIENT="${ROOT_DIR}/scripts/e2e/abort_execute_client.go"
ABORT_CLIENT_BIN="${ROOT_DIR}/.tmp/e2e/abort_execute_client"

ADDR="${ADDR:-127.0.0.1:62051}"
ROUNDS="${ROUNDS:-50}"
ABORT_DELAY="${ABORT_DELAY:-750ms}"
EXPECT="${EXPECT:-zombie}"
LOG_ROOT="${ROOT_DIR}/.tmp/e2e/zombie-repro/$(date '+%Y%m%d%H%M%S')"
COMPOSE_STARTED=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/e2e/zombie_repro.sh

Environment:
  ADDR          Proxy gRPC address. Default: 127.0.0.1:62051
  ROUNDS        Number of abort rounds. Default: 50
  ABORT_DELAY   Delay between first accepted/log response and client abort. Default: 750ms
  EXPECT        Expected outcome: zombie or clean. Default: zombie
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

cleanup() {
  set +e
  if [[ "${COMPOSE_STARTED}" -eq 1 ]]; then
    (
      cd "${ROOT_DIR}"
      run_without_proxy docker compose down >/dev/null 2>&1 || true
    )
  fi
}

trap cleanup EXIT

wait_for_proxy_ready() {
  local output=""
  local i
  for ((i = 1; i <= 120; i++)); do
    if output="$(run_without_proxy "${AFS_CLI}" -addr "${ADDR}" -proxy-status -grpc-timeout 20s 2>&1)"; then
      if grep -q '^summary ' <<<"${output}" && grep -q 'reachable=true' <<<"${output}" && grep -q '^layerstore ' <<<"${output}"; then
        printf '%s\n' "${output}" >"${LOG_ROOT}/proxy-status.txt"
        return 0
      fi
    fi
    sleep 2
  done
  printf '%s\n' "${output}" >"${LOG_ROOT}/proxy-status-last.txt"
  die "proxy status never became ready; see ${LOG_ROOT}"
}

collect_container_state() {
  local container="$1"
  run_without_proxy docker exec "${container}" /bin/sh -lc 'ps -eo pid,ppid,pgid,stat,comm,args' >"${LOG_ROOT}/${container}.ps.txt" 2>&1 || true
  run_without_proxy docker logs "${container}" >"${LOG_ROOT}/${container}.log.txt" 2>&1 || true
}

check_for_zombies() {
  local found=1
  local container
  for container in afs-afslet-1 afs-afslet-2 afs-afslet-3; do
    collect_container_state "${container}"
    if grep -Eq '^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+Z' "${LOG_ROOT}/${container}.ps.txt"; then
      found=0
    fi
  done
  return "${found}"
}

main() {
  [[ -f "${ABORT_CLIENT}" ]] || die "missing ${ABORT_CLIENT}"
  [[ -x "${AFS_CLI}" ]] || die "missing ${AFS_CLI}, run: make build-local"
  run_without_proxy docker compose version >/dev/null 2>&1 || die "docker compose is required"

  mkdir -p "${LOG_ROOT}"
  mkdir -p "$(dirname "${ABORT_CLIENT_BIN}")"
  [[ "${EXPECT}" == "zombie" || "${EXPECT}" == "clean" ]] || die "EXPECT must be zombie or clean"

  printf '[zombie-repro] building abort client\n'
  run_without_proxy go build -o "${ABORT_CLIENT_BIN}" "${ABORT_CLIENT}" >"${LOG_ROOT}/abort-client-build.log" 2>&1

  printf '[zombie-repro] building local compose image afs-local:compose\n'
  run_without_proxy docker build -t afs-local:compose "${ROOT_DIR}" >"${LOG_ROOT}/docker-build.log" 2>&1

  printf '[zombie-repro] restarting docker compose stack\n'
  (
    cd "${ROOT_DIR}"
    run_without_proxy docker compose down -v >"${LOG_ROOT}/compose-down.log" 2>&1 || true
    run_without_proxy docker compose up -d >"${LOG_ROOT}/compose-up.log" 2>&1
  )
  COMPOSE_STARTED=1

  wait_for_proxy_ready

  local round
  for ((round = 1; round <= ROUNDS; round++)); do
    printf '[zombie-repro] round=%d aborting execute stream\n' "${round}"
    set +e
    run_without_proxy "${ABORT_CLIENT_BIN}" -addr "${ADDR}" -delay "${ABORT_DELAY}" >"${LOG_ROOT}/round-${round}.txt" 2>&1
    status=$?
    set -e
    if [[ "${status}" -ne 0 && "${status}" -ne 99 ]]; then
      cat "${LOG_ROOT}/round-${round}.txt" >&2 || true
      die "abort client failed unexpectedly in round ${round}"
    fi
    sleep 1
    if check_for_zombies; then
      printf '[zombie-repro] zombie detected in round=%d; logs in %s\n' "${round}" "${LOG_ROOT}"
      if [[ "${EXPECT}" == "zombie" ]]; then
        return 0
      fi
      return 1
    fi
  done

  printf '[zombie-repro] no zombies detected after %d rounds; logs in %s\n' "${ROUNDS}" "${LOG_ROOT}"
  if [[ "${EXPECT}" == "clean" ]]; then
    return 0
  fi
  return 1
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

main "$@"
