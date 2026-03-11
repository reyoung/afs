# AFS Performance Benchmark

本文档对当前分支的 `afs` 执行性能进行一次基线测试，并和 `docker run` 做对比。

- Commit: `2dc1b6cf2293`（workspace dirty）
- OS: `Linux 6.6.47-12.tl4.x86_64`
- CPU: `AMD EPYC 7K62 48-Core Processor`（32 vCPU）
- Docker: `28.0.1`
- Docker Compose: `2.32.1`
- 镜像: `alpine:3.20`

## 测试维度

1. **warmup 性能**：每个场景首轮执行耗时。
2. **warm 性能**：warmup 之后连续 5 轮执行耗时统计。
3. **镜像解析 / layer 预热开销不计入性能**：
   - 先 `docker pull alpine:3.20`
   - 再通过 `ResolveImage + LayerStore.EnsureLayers` 手动把 layer 预热到 `layerstore`。

## 场景定义

- `host-bare`: 裸进程基线（`/bin/sh -c true`）
- `docker-run`: 直接 `docker run --rm --pull=never alpine:3.20 /bin/sh -c true`
- `afs-direct`: `afs_cli` 直连 `afslet:61051`
- `afs-proxy-compose`: `afs_cli` 通过 `afs_proxy:62051`（docker compose 路径）

> 说明：本次 AFS 测试依赖仓库 `docker-compose.yaml` 启动 discovery/layerstore/afslet/afs_proxy；其中 `afs-proxy-compose` 即 docker compose 方式。

## 复现脚本

已提供脚本：`scripts/perf/benchmark.sh`

执行：

```bash
make build-local
WARM_ITERS=5 ./scripts/perf/benchmark.sh
```

如果需要避免 Docker Hub 匿名限流，可以在执行前注入：

```bash
LAYERSTORE_AUTH_BASIC_1='registry-1.docker.io=<username>:<password_or_token>' WARM_ITERS=5 ./scripts/perf/benchmark.sh
```

脚本输出：

- 原始数据：`.tmp/perf/raw.csv`
- 汇总数据：`.tmp/perf/summary.csv`
- 过程日志：`.tmp/perf/benchmark.log`

## 本次测试结果（ms）

| scenario | phase | count | avg_ms | median_ms | min_ms | max_ms |
|---|---:|---:|---:|---:|---:|---:|
| afs-direct | warmup | 1 | 285.00 | 285.00 | 285 | 285 |
| afs-direct | warm | 5 | 280.80 | 282.00 | 273 | 287 |
| afs-proxy-compose | warmup | 1 | 288.00 | 288.00 | 288 | 288 |
| afs-proxy-compose | warm | 5 | 287.40 | 288.00 | 282 | 292 |
| docker-run | warmup | 1 | 405.00 | 405.00 | 405 | 405 |
| docker-run | warm | 5 | 350.80 | 347.00 | 340 | 361 |
| host-bare | warmup | 1 | 4.00 | 4.00 | 4 | 4 |
| host-bare | warm | 5 | 4.60 | 5.00 | 4 | 5 |

## 结论（本机本次样本）

- `afs-direct` warm（~281ms）较 `docker-run` warm（~351ms）更快。
- `afs-proxy-compose` warm（~287ms）也快于 `docker-run` warm。
- 在当前样本里，`afs-direct` 与 `afs-proxy-compose` 的 warmup 都稳定在 ~285ms 左右，没有出现早先那种明显首轮抖动。
