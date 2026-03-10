#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${IMAGE:-alpine}"
TAG="${TAG:-3.20}"
IMAGE_REF="${IMAGE}:${TAG}"
WARM_ITERS="${WARM_ITERS:-5}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/.tmp/perf}"
AFS_CLI="${ROOT_DIR}/dist/linux_amd64/afs_cli"

mkdir -p "${OUT_DIR}"
RAW_CSV="${OUT_DIR}/raw.csv"
SUMMARY_CSV="${OUT_DIR}/summary.csv"
EMPTY_DIR="${OUT_DIR}/empty-dir"
AFS_OUT="${OUT_DIR}/writable-upper.tar.gz"
mkdir -p "${EMPTY_DIR}"

if [[ ! -x "${AFS_CLI}" ]]; then
  echo "missing ${AFS_CLI}, run: make build-local" >&2
  exit 1
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
}

require_cmd docker
require_cmd go
require_cmd python3

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
    sleep 0.5
  done
  echo "timed out waiting for ${label} at ${host}:${port}" >&2
  exit 1
}

# Ensure compose stack is up for afs measurements.
( cd "${ROOT_DIR}" && docker compose up -d >/dev/null )

# Pull image in advance; pull time is excluded from benchmark.
docker pull "${IMAGE_REF}" >/dev/null

layerstore_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' afs-layerstore)"
if [[ -z "${layerstore_ip}" ]]; then
  echo "failed to resolve afs-layerstore IP" >&2
  exit 1
fi

wait_for_tcp "${layerstore_ip}" 50051 "layerstore"
wait_for_tcp 127.0.0.1 61051 "afslet"
wait_for_tcp 127.0.0.1 62051 "afs_proxy"

# Manually pre-pull image into layerstore via PullImage RPC (excluded from benchmark).
TMP_GO="$(mktemp "${OUT_DIR}/prepull_layerstore.XXXXXX.go")"
trap 'rm -f "${TMP_GO}"' EXIT
cat > "${TMP_GO}" <<'GOCODE'
package main

import (
  "context"
  "flag"
  "fmt"
  "time"

  "github.com/reyoung/afs/pkg/layerstorepb"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials/insecure"
)

func main() {
  var addr, image, tag string
  flag.StringVar(&addr, "addr", "", "layerstore addr")
  flag.StringVar(&image, "image", "", "image")
  flag.StringVar(&tag, "tag", "", "tag")
  flag.Parse()

  if addr == "" || image == "" {
    panic("-addr and -image are required")
  }

  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
  defer cancel()

  conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
  if err != nil {
    panic(err)
  }
  defer conn.Close()

  cli := layerstorepb.NewLayerStoreClient(conn)
  resp, err := cli.PullImage(ctx, &layerstorepb.PullImageRequest{
    Image:        image,
    Tag:          tag,
    PlatformOs:   "linux",
    PlatformArch: "amd64",
  })
  if err != nil {
    panic(err)
  }
  fmt.Printf("prepulled layers=%d resolved=%s/%s:%s\n", len(resp.GetLayers()), resp.GetResolvedRegistry(), resp.GetResolvedRepository(), resp.GetResolvedReference())
}
GOCODE

go run "${TMP_GO}" -addr "${layerstore_ip}:50051" -image "${IMAGE}" -tag "${TAG}" >/dev/null

printf 'scenario,phase,iteration,elapsed_ms\n' > "${RAW_CSV}"

measure_once() {
  local scenario="$1"
  local phase="$2"
  local iteration="$3"
  shift 3
  local start end elapsed
  start="$(date +%s%3N)"
  "$@" >/dev/null
  end="$(date +%s%3N)"
  elapsed=$((end - start))
  printf '%s,%s,%s,%s\n' "${scenario}" "${phase}" "${iteration}" "${elapsed}" >> "${RAW_CSV}"
}

benchmark_scenario() {
  local scenario="$1"
  shift
  measure_once "${scenario}" "warmup" 1 "$@"
  local i
  for ((i = 1; i <= WARM_ITERS; i++)); do
    measure_once "${scenario}" "warm" "${i}" "$@"
  done
}

benchmark_scenario host-bare /bin/sh -c true
benchmark_scenario docker-run docker run --rm --pull=never "${IMAGE_REF}" /bin/sh -c true
benchmark_scenario afs-direct "${AFS_CLI}" -addr 127.0.0.1:61051 -dir "${EMPTY_DIR}" -image "${IMAGE}" -tag "${TAG}" -timeout 5s -out "${AFS_OUT}" -- /bin/sh -c true
benchmark_scenario afs-proxy-compose "${AFS_CLI}" -addr 127.0.0.1:62051 -dir "${EMPTY_DIR}" -image "${IMAGE}" -tag "${TAG}" -timeout 5s -out "${AFS_OUT}" -- /bin/sh -c true

python3 - "${RAW_CSV}" "${SUMMARY_CSV}" <<'PY'
import csv
import statistics
import sys

raw_path, summary_path = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(raw_path, newline="")))

groups = {}
for r in rows:
    key = (r["scenario"], r["phase"])
    groups.setdefault(key, []).append(int(r["elapsed_ms"]))

with open(summary_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["scenario", "phase", "count", "avg_ms", "median_ms", "min_ms", "max_ms"])
    for (scenario, phase), vals in sorted(groups.items()):
        w.writerow([
            scenario,
            phase,
            len(vals),
            f"{sum(vals)/len(vals):.2f}",
            f"{statistics.median(vals):.2f}",
            min(vals),
            max(vals),
        ])
PY

echo "raw=${RAW_CSV}"
echo "summary=${SUMMARY_CSV}"
cat "${SUMMARY_CSV}"
