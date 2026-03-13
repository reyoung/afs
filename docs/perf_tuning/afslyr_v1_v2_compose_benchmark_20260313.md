# 2026-03-13 AFSLYR V1 vs V2 Compose 基准

这份记录对应本地真实 Docker Compose 环境：

- `3` 个 `afs_layerstore_grpcd`
- `3` 个 `afslet`
- `1` 个 `afs_proxy`
- `1` 个 `afs_discovery_grpcd`

测试目标是对比 `AFSLYR01` 和 `AFSLYR02` 在真实 IOI 镜像上的端到端性能差异，并区分冷态与热态。

## 测试样本

这次固定使用 9 道代表题：

- `aplusb_2023`
- `beechtree_2023`
- `biscuits_2020`
- `candies_2021`
- `cards_2022`
- `choreography_2023`
- `closing_2023`
- `coreputer_2023`
- `deliveries_2023`

统一参数：

- `--parallel 4`
- AFS execute 经 `afs_proxy`
- Docker baseline 使用同一批题目

## 测试方法

### V2 cold

1. `docker compose down -v`
2. `AFS_FORMAT_VERSION=2 docker compose up -d`
3. 跑 9 题 benchmark

结果：

- [summary.md](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v2_cold_rerun_20260313/summary.md)
- [delta.csv](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v2_cold_rerun_20260313/delta.csv)

### V2 warm

1. 不重启 compose
2. 直接复用刚跑完 `V2 cold` 的服务与缓存
3. 再跑同一轮 benchmark

结果：

- [summary.md](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v2_warm_20260313/summary.md)
- [delta.csv](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v2_warm_20260313/delta.csv)

### V1 cold

1. `docker compose down -v`
2. `AFS_FORMAT_VERSION=1 docker compose up -d`
3. 跑同一批 9 题 benchmark

结果：

- [summary.md](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v1_cold_rerun_20260313/summary.md)
- [delta.csv](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v1_cold_rerun_20260313/delta.csv)

### V1 warm

1. 不重启 compose
2. 直接复用刚跑完 `V1 cold` 的服务与缓存
3. 再跑同一轮 benchmark

结果：

- [summary.md](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v1_warm_20260313/summary.md)
- [delta.csv](/data/home/josephyu/projs/agent-dockers/ioi/.cache/ioi_docker/benchmark_afs_vs_docker_9_v1_warm_20260313/delta.csv)

## 汇总结果

| Case | AFS ready wait (s) | AFS reconcile (s) | AFS execute (s) | Docker execute (s) | AFS end-to-end (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 cold | 2.012 | 10.012 | 64.947 | 45.490 | 76.971 |
| V2 warm | 0.007 | 0.026 | 49.833 | 43.870 | 49.866 |
| V1 cold | 4.015 | 29.022 | 111.052 | 44.119 | 144.089 |
| V1 warm | 0.008 | 0.027 | 51.049 | 43.959 | 51.084 |

## 结论

### 1. `AFSLYR02` 的主要收益在冷态

`V2` 相比 `V1`：

- 冷态 `AFS end-to-end`：`76.971s vs 144.089s`，快了 `67.118s`
- 冷态 `AFS execute wall`：`64.947s vs 111.052s`，快了 `46.105s`
- 冷态 `AFS reconcile wall`：`10.012s vs 29.022s`，快了 `19.010s`

这说明 `V2` 的核心收益主要来自：

- 避免 `V1` 首次读取时的解压路径
- 避免 `V1` 首次 spill/materialize 的额外成本
- 减少冷态 mount ready 前的准备时间

### 2. 热态下 `V1` 和 `V2` 已经基本收敛

`V2` 相比 `V1`：

- 热态 `AFS end-to-end`：`49.866s vs 51.084s`，快了 `1.218s`
- 热态 `AFS execute wall`：`49.833s vs 51.049s`，快了 `1.216s`

这说明在 warm cache 条件下：

- `AFSLYR02` 仍然略优
- 但优势已经很小
- 剩余差距不再主要来自 layer format 本身，而更像 steady-state 的运行时路径成本

### 3. Docker baseline 在冷热之间基本稳定

Docker `execute wall time`：

- `V2 cold`：`45.490s`
- `V2 warm`：`43.870s`
- `V1 cold`：`44.119s`
- `V1 warm`：`43.959s`

这说明测试波动整体可接受，冷热差异主要来自 AFS 本身，而不是题目集合或宿主机环境随机波动。

## 逐题对比

| Problem | V1 cold (s) | V2 cold (s) | V1 warm (s) | V2 warm (s) | V2-V1 cold (s) | V2-V1 warm (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| aplusb_2023 | 16.086 | 25.609 | 5.511 | 5.477 | 9.523 | -0.034 |
| beechtree_2023 | 80.157 | 34.473 | 24.900 | 25.257 | -45.684 | 0.357 |
| biscuits_2020 | 53.301 | 13.385 | 5.813 | 4.834 | -39.916 | -0.979 |
| candies_2021 | 76.856 | 33.778 | 21.634 | 21.560 | -43.078 | -0.073 |
| cards_2022 | 76.479 | 46.187 | 45.536 | 44.997 | -30.292 | -0.539 |
| choreography_2023 | 9.767 | 10.538 | 9.164 | 7.626 | 0.771 | -1.538 |
| closing_2023 | 16.259 | 14.875 | 15.516 | 14.060 | -1.384 | -1.455 |
| coreputer_2023 | 9.069 | 7.848 | 6.640 | 6.626 | -1.222 | -0.014 |
| deliveries_2023 | 31.720 | 28.797 | 23.975 | 24.132 | -2.922 | 0.157 |

说明：

- `V2-V1` 为负值表示 `V2` 更快
- 冷态下收益最大的题主要是：
  - `beechtree_2023`
  - `candies_2021`
  - `biscuits_2020`
  - `cards_2022`
- 热态下大多数题的差距都缩到 `1s` 左右

## 当前判断

这组数据支持以下判断：

1. `AFSLYR02` 已经显著改善了冷态首次执行性能
2. `AFSLYR02` 的主要收益来自去掉 `V1` 的压缩层读取兼容路径
3. warm 之后 `V1/V2` 的性能已经非常接近，后续如果还要继续优化，重点应该从 layer format 切换到 steady-state 文件系统路径

## 后续建议

如果继续做性能工作，优先级建议是：

1. 在 `V2 warm` 下继续看 steady-state 热点
2. 重点关注：
   - FUSE 读路径
   - `fuse-overlayfs`
   - 容器内编译时的 sys time
3. 如果要做更大规模验证，再把这套方法扩成全量 IOI 题目
