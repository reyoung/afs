# AFS Page Cache & FUSE 优化报告

## 概述

本次优化针对 AFS（Agent File System）在 IOI 问题执行场景下的 warm 性能瓶颈，通过 5 项核心优化将 AFS 执行速度从比 Docker 慢 3.3s 提升到基本持平。

**测试用例**: aplusb_2023（12 层 builder + 16 层 verifier，53 个测试点）
**环境**: 1 layerstore + 1 afslet，parallel=1

## 最终结果

### Warm 性能对比

| 指标 | AFS (优化后) | Docker | 差距 |
|------|---:|---:|---:|
| **compile** | **1.08s** | 1.12s | **AFS 快 0.04s** |
| **verify** | 2.38s | 2.12s | +0.26s |
| **总计** | **3.46s** | 3.24s | +0.22s |

### Cold 性能对比

| 指标 | AFS | Docker | 说明 |
|------|---:|---:|------|
| **总计** | **4.57s** | 11.36s | **AFS 快 2.5×** |

## 优化历程

| # | 优化项 | 节省 | Warm 总时间 | 与 Docker 差距 |
|---|--------|---:|---:|---:|
| 0 | 基线（gRPC page cache） | — | 6.52s | +3.29s |
| 1 | **in-process page cache** — 去掉 gRPC IPC，进程内 slab+index | 0.94s | 5.58s | +2.42s |
| 2 | **并发 FUSE mount + reaper fix** — concurrency=8，修复 Wait4 竞态 | 0.69s | 4.89s | +1.73s |
| 3 | **TOC cache** — 缓存 AFSLYR02 JSON 解析结果 | 0.24s | 4.65s | +1.50s |
| 4 | **Unified overlay** — Go 内合并多层 TOC，单层只读 FUSE | 0.57s | 4.08s | +0.77s |
| 5 | **Unified-RW** — 去掉 fuse-overlayfs，Go 实现可写层 | 0.62s | 3.46s | +0.22s |
| | **总计** | **3.06s** | | |

## 技术细节

### 1. In-process Page Cache（`pkg/pagecache/store.go`）

**问题**: 原始 page cache 是独立 gRPC 容器，每次 FUSE read 需要 2 次 UDS RPC + file open/close。
且 chunk 文件在不同容器 volume 里，afslet 读不到 → cache 100% 失效。

**方案**: 去掉 gRPC service/client/proto/daemon，page cache 变成纯进程内 Go 库。
`Store.ReadThrough()` 直接操作 slab allocator + page index + fd pool。

**代码变化**: 删除 4600 行（gRPC proto/service/client/lease），新增 290 行（store.go）。

### 2. 并发 FUSE Mount + Child Reaper Fix

**问题**: layer mount concurrency=1 时，16 层串行 mount 需要 240ms。
提高并发后 fusermount3 子进程被 child reaper 的 Wait4(-1) 抢先 reap。

**方案**: RWMutex 协调——FUSE mount 持 RLock（可并发），reaper 持 WLock（mount 期间暂停）。
mount 时间从 240ms → 25-35ms。

### 3. TOC Cache（`pkg/layerformat/toc_cache.go`）

**问题**: 每次 execute 重新解析 12-16 个 layer 的 JSON TOC（profile 显示 0.35s）。

**方案**: LRU 缓存已解析的 toc struct + entriesByP map，key=layer digest。
warm 下 `layer_prepare_open_layerformat` 从 21-98ms/layer → 0ms/layer。

### 4. Unified Overlay（`pkg/layerfuse/overlay.go`）

**问题**: 原架构是 N×FUSE（per-layer）+ fuse-overlayfs = N+1 层 FUSE。
fuse-overlayfs 对每个路径在 12 层逐层 lookup → 12772 次 lookup × 80µs = ~1.0s。

**方案**: 在 Go 中合并所有 layer TOC（含 OCI whiteout 语义），输出单层只读 FUSE mount。
lookup 从 12772 → 593（-95%），FUSE 层数从 N+1 → 2。

### 5. Writable Path

当前实现固定为 `unified-koverlay`：
- 下层使用 unified read-only FUSE mount
- 上层写入统一交给 Linux kernel overlay 处理

旧的 `overlay_rw` 原型已移除，不再作为运行时策略保留。

## FUSE 操作量对比

| 模式 | Lookup 次数 | 总 FUSE 操作 | FUSE 层数 |
|------|---:|---:|---:|
| per-layer (原始) | 12,772 | ~17,000 | N+1 |
| unified (只读) | 8,073 | ~12,300 | 2 |
| **unified-rw (最终)** | **593** | **~2,000** | **1** |

## 架构对比

```
优化前:
  VFS → fuse-overlayfs(FUSE#1) → layer[0] FUSE(#2) → Go
                                → layer[1] FUSE(#3) → Go
                                → ...
                                → layer[N] FUSE(#N+1) → Go
  每次读: 2× FUSE context switch + gRPC page cache RPC

优化后:
  VFS → unified-rw FUSE(#1) → Go (merged overlay + writable upper + page cache)
  每次读: 1× FUSE context switch + in-process page cache lookup
```

## 测试覆盖

- 22 个 FUSE 集成测试（layer merge、读写、混合操作、边界情况）
- 全部 mount 真实 FUSE 文件系统验证
- IOI aplusb_2023 端到端验证（53/53 测试点通过）

## 剩余差距分析

warm 下 AFS 比 Docker 慢 ~0.2s，主要来自：
- **FUSE kernel↔userspace context switch** (~0.15s): 593 lookups + 755 reads ≈ 1500 ops × 100µs
- **runc 启动开销** (~0.05s): namespace + cgroup 创建

这是 FUSE 协议的固有限制，进一步优化需要 kernel 层面的改变（如 FUSE passthrough、virtiofs）。
