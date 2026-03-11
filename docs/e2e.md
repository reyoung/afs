# End-to-End Testing

This document defines the AFS smoke-level end-to-end entrypoints across three runtime shapes:

- Bare process
- Docker Compose
- Kubernetes + Helm

The unified entrypoint is `scripts/e2e/smoke.sh`. By default it validates:

- `afs_cli -proxy-status`
- `afs_cli reconcile-image-replica`

That covers the full discovery -> layerstore -> afslet -> afs_proxy control-plane and scheduling path. The privileged execute path (`afs_mount + afs_runc`) should still stay covered by the existing integration tests.

## Prerequisites

- Linux host
- Local binaries built first: `make build-local`
- `bare` mode needs local process execution on localhost ports
- `compose` mode needs `docker compose`
- `helm` mode needs access to a Kubernetes cluster, and lifecycle must go through `./helm/start.sh` / `./helm/stop.sh`

The default test image is `registry.k8s.io/pause:3.9`. In this repository environment it works reliably once host `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` inheritance is removed. You can override it:

```bash
./scripts/e2e/smoke.sh --mode bare --image alpine --tag 3.20
```

## 1. Bare Process

```bash
make build-local
./scripts/e2e/smoke.sh --mode bare
```

The script starts these binaries locally:

- `afs_discovery_grpcd`
- `afs_layerstore_grpcd`
- `afslet`
- `afs_proxy`

Then it runs the smoke checks through the local `afs_proxy` gRPC address.

## 2. Docker Compose

```bash
make build-local
./scripts/e2e/smoke.sh --mode compose
```

Behavior:

- The script first builds a local runtime image `afs-local:compose` from the repository `Dockerfile`, without inheriting host HTTP proxy env vars
- Compose services use that local image directly, so `afslet` does not run `apt-get` during container startup
- If the compose stack is not running, the script runs `docker compose up -d`
- If the script started the stack itself, it also runs `docker compose down` on exit
- If the stack was already running, the script reuses it and leaves it running

## 3. Kubernetes + Helm

Deploy first:

```bash
./helm/start.sh
```

Then run the smoke checks:

```bash
./scripts/e2e/smoke.sh --mode helm --namespace afs
```

Tear down after that:

```bash
./helm/stop.sh
```

Notes:

- `helm` mode uses `kubectl port-forward` to the in-cluster `afs-proxy` Service by default
- If `--image/--tag` are not provided, the script auto-detects the image currently used by the `afs-afslet` Deployment and uses that for the smoke test
- If you already have a reachable proxy endpoint, pass `--addr <host:port>` instead
- Do not use direct `helm upgrade/install/uninstall` for the normal deployment workflow; use `./helm/start.sh` and `./helm/stop.sh`

## Proxy Handling

`helm/start.sh` and `helm/stop.sh` already wrap `helm` and `kubectl` with proxy env removal. They unset:

- `http_proxy`
- `https_proxy`
- `all_proxy`
- `HTTP_PROXY`
- `HTTPS_PROXY`
- `ALL_PROXY`

The same no-proxy rule is also used for the local processes started by `scripts/e2e/smoke.sh --mode bare`, and for the `kubectl` calls in `helm` mode. That avoids bad registry responses caused by host-level `HTTP_PROXY` settings.

## Common Flags

```bash
./scripts/e2e/smoke.sh --help
```

Common overrides:

- `--addr <host:port>`: explicit `afs_proxy` gRPC address
- `--namespace <name>`: namespace for helm mode
- `--image <name> --tag <tag>`: override the test image
- `--replica <n>`: requested replica count for `reconcile-image-replica`
- `--skip-reconcile`: run `proxy-status` only

## Output

Logs are written to:

```text
.tmp/e2e/<timestamp>/
```

This includes:

- `proxy-status.txt`
- `reconcile-image.txt`
- `kubectl-port-forward.log` in helm mode
- local process logs in bare mode
