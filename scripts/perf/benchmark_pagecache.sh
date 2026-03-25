#!/usr/bin/env bash
set -euo pipefail

# Minimal compose: 1 layerstore + 1 afslet + proxy, parallel=1
# Compare page cache ON vs OFF, cold vs warm (3 warm runs each)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AFS_DIR="/data/home/josephyu/projs/afs"
IOI_DIR="/data/home/josephyu/projs/rl-infra/ad/agent-dockers/ioi"
COMPOSE_FILE="${AFS_DIR}/docker-compose.minimal.yaml"
RESULTS_BASE="${IOI_DIR}/.cache/ioi_docker"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
FILTER='^(aplusb_2023|coreputer_2023|choreography_2023)$'

run_without_proxy() {
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY "$@"
}

compose() {
  run_without_proxy docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_proxy_ready() {
  local cli="${AFS_DIR}/dist/linux_amd64/afs_cli"
  for i in $(seq 1 60); do
    if run_without_proxy "${cli}" -addr 127.0.0.1:62051 -proxy-status -grpc-timeout 10s 2>&1 | grep -q 'reachable=true'; then
      return 0
    fi
    sleep 2
  done
  echo "ERROR: proxy not ready" >&2
  return 1
}

run_benchmark() {
  local label="$1"
  local results_dir="${RESULTS_BASE}/minimal_${label}_${TIMESTAMP}"
  echo ""
  echo "=========================================="
  echo "  ${label}"
  echo "=========================================="
  cd "${IOI_DIR}" && run_without_proxy python3 -u scripts/benchmark_afs_vs_docker.py \
    --limit 3 \
    --parallel 1 \
    --proxy-addr 127.0.0.1:62051 \
    --afs-python-path ~/projs/afs/python \
    --filter "${FILTER}" \
    --results-dir "${results_dir}" \
    2>&1
  echo ""
  echo "--- ${label} summary ---"
  grep -E '(AFS ready|AFS reconcile|AFS execute|Docker execute|AFS end-to-end)' "${results_dir}/summary.md" 2>/dev/null || true
  echo ""
}

############################################################
# Phase 1: No page cache
############################################################
echo "============================================"
echo "  PHASE 1: NO PAGE CACHE"
echo "============================================"

# Cold start
compose down -v 2>/dev/null || true
AFS_PAGE_CACHE_ENABLED=false compose up -d 2>&1 | tail -3
sleep 3
wait_proxy_ready
run_benchmark "nopagecache_cold"

# Warm runs
for i in 1 2 3; do
  run_benchmark "nopagecache_warm${i}"
done

compose down -v 2>/dev/null || true

############################################################
# Phase 2: With page cache
############################################################
echo ""
echo "============================================"
echo "  PHASE 2: WITH PAGE CACHE"
echo "============================================"

# Cold start
AFS_PAGE_CACHE_ENABLED=true compose --profile page-cache up -d 2>&1 | tail -3
sleep 3
wait_proxy_ready
run_benchmark "pagecache_cold"

# Warm runs
for i in 1 2 3; do
  run_benchmark "pagecache_warm${i}"
done

compose --profile page-cache down -v 2>/dev/null || true

############################################################
# Summary
############################################################
echo ""
echo "============================================"
echo "  ALL RESULTS"
echo "============================================"
for d in "${RESULTS_BASE}"/minimal_*_${TIMESTAMP}/; do
  label="$(basename "$d" | sed "s/_${TIMESTAMP}//")"
  echo "--- ${label} ---"
  grep -E '(AFS execute|Docker execute)' "$d/summary.md" 2>/dev/null || echo "(no summary)"
done
