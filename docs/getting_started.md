# Getting Started

This guide covers the current recommended way to run AFS in Kubernetes with the project Helm scripts.

## Prerequisites

- Linux host with `git`, `docker`, `helm`, `kubectl`
- Access to a Kubernetes cluster
- Push permission to your image registry

## 1. Configure Environment

Create `helm/.env`:

```env
NAMESPACE=afs
IMAGE_REPOSITORY=mirrors.tencent.com/josephyu/afs
RELEASE_NAME=afs
CHART_PATH=./helm/afs

# Replicas
DISCOVERY_REPLICAS=3
AFS_PROXY_REPLICAS=3
AFSLET_REPLICAS=3

# afslet runtime limits passed to afslet binary
AFSLET_LIMIT_CPU_CORES=4
AFSLET_LIMIT_MEMORY_MB=16384

# afslet pod resources
AFSLET_REQUEST_CPU=4
AFSLET_REQUEST_MEMORY=16Gi
AFSLET_LIMIT_CPU=4
AFSLET_LIMIT_MEMORY=16Gi
```

Notes:
- `helm/.env` is gitignored.
- If `helm/.env` is missing, `start.sh` and `stop.sh` print a template and exit.

## 2. Start AFS

```bash
./helm/start.sh
```

What it does:
- Builds and pushes a new image tag: `<git-sha>-<datetime>` (adds `-dirty` for dirty workspace)
- Deploys/updates Helm release
- Applies defaults from `.env` unless you pass explicit `--set ...` overrides
- Automatically disables proxy env vars for `helm`/`kubectl` operations

## 3. Verify Deployment

```bash
kubectl -n afs get deploy,ds,svc
kubectl -n afs get pods -l app.kubernetes.io/component=afslet -o wide
```

Expected by default:
- `discovery` replicas: 3
- `afs-proxy` replicas: 3
- `afslet` replicas: configurable (example above is 3)

`afslet` remains a Deployment (not DaemonSet), with preferred pod anti-affinity to spread pods across nodes.

## 4. Stop AFS

```bash
./helm/stop.sh
```

## Optional: Override at Runtime

You can still override any Helm value directly:

```bash
./helm/start.sh --set afslet.replicas=6 --set-string afslet.resources.requests.memory=32Gi
```
