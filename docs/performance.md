# AFS Performance Benchmark

本文档对当前分支的 `afs` 执行性能进行一次基线测试，并和 `docker run` 做对比。

- Commit: `e776e0d`
- OS: `Linux 6.6.47-12.tl4.x86_64`
- CPU: `AMD EPYC 7K62 48-Core Processor`（32 vCPU）
- Docker: `28.0.1`
- Docker Compose: `2.32.1`
- 镜像: `alpine:3.20`

## 测试维度

1. **warmup 性能**：每个场景首轮执行耗时。
2. **warm 性能**：warmup 之后连续 5 轮执行耗时统计。
3. **pull image 开销不计入性能**：
   - 先 `docker pull alpine:3.20`
   - 再通过 `LayerStore.PullImage` 手动把镜像预拉到 `layerstore`。

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

脚本输出：

- 原始数据：`.tmp/perf/raw.csv`
- 汇总数据：`.tmp/perf/summary.csv`

## 本次测试结果（ms）

| scenario | phase | count | avg_ms | median_ms | min_ms | max_ms |
|---|---:|---:|---:|---:|---:|---:|
| afs-direct | warmup | 1 | 520.00 | 520.00 | 520 | 520 |
| afs-direct | warm | 5 | 292.20 | 291.00 | 290 | 296 |
| afs-proxy-compose | warmup | 1 | 294.00 | 294.00 | 294 | 294 |
| afs-proxy-compose | warm | 5 | 297.80 | 297.00 | 296 | 301 |
| docker-run | warmup | 1 | 370.00 | 370.00 | 370 | 370 |
| docker-run | warm | 5 | 360.60 | 360.00 | 353 | 375 |
| host-bare | warmup | 1 | 5.00 | 5.00 | 5 | 5 |
| host-bare | warm | 5 | 4.60 | 5.00 | 4 | 5 |

## 结论（本机本次样本）

- `afs-direct` warm（~292ms）较 `docker-run` warm（~361ms）更快。
- `afs-proxy-compose` warm（~298ms）也快于 `docker-run` warm。
- `afs-direct` warmup 首轮有一次性抖动（520ms），warm 后收敛到 ~290ms。

