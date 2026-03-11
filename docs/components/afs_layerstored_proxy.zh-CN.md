# 组件设计（草案）：afs_layerstored_proxy

## 目标

在 `afs_mount -> layerstore` 之间增加一层本地代理 `afs_layerstored_proxy`，用于：

- 自动从 discovery 选择可用 provider（优先本地亲和）。
- 对 `ReadLayer` 的读取结果做本地 LFU 落盘缓存，提升多 mount 并发场景下的重复读性能。
- 在不改动现有 `layerstore` 数据格式（`.afslyr`）的前提下，尽量复用现有 gRPC 协议。

## 背景问题

当前路径里已有两类缓存：

- layerstore 的层文件缓存（`.afslyr`）。
- `afs_mount` 进程内/本机的文件 spill 缓存（每次 mount 会产生自己的读放大）。

但当“同一台机器上多个 mount 读取同一层相同热点文件”时，仍会重复经过网络 `ReadLayer` 和解压路径，缺少跨 mount 共享的一层本地热点缓存。

## 放置位置与职责边界

- `afs_layerstored_proxy` 部署在执行节点本机（与 `afs_mount` 同机）。
- `afs_mount` 改为优先连接本机 proxy；proxy 再去访问远端 layerstore。
- proxy 自身不做镜像拉取和持久层管理；它只做：
  - provider 发现与切换。
  - 读请求复用和缓存。

## 协议与兼容性

建议 proxy 直接实现 `layerstore.v1.LayerStore` 的子集：

- 必须：`EnsureLayers`、`StatLayer`、`ReadLayer`
- 可选透传：`HasLayer`
- 不提供：`PruneCache`、image 级存在性接口（或返回 `Unimplemented`）

这样 `afs_mount` 侧改动最小（仅 endpoint 指向 proxy）。

## 缓存对象模型（第一版）

### 缓存粒度

采用“请求级文件缓存（每个 key 对应一个本地文件）”：

- key: `digest + offset + length`
- value: 该请求返回的原始 `ReadLayerResponse.data` 字节

原因：

- proxy 只看到 `ReadLayer(digest, offset, length)`，看不到 layer 内文件路径。
- 多个 mount 对同层热点文件的读取模式通常会重复到相同 `(offset,length)`，可直接命中。
- 实现复杂度低，且符合“类似 afs_mount spill 的落盘文件缓存”思路。

### 磁盘布局

- `cache_dir/index/`：索引（可用 bolt/badger 或 JSON+WAL，首版可先 JSON+文件锁）。
- `cache_dir/data/`：缓存数据文件（一个 key 对应一个文件）。
- `cache_dir/tmp/`：写入临时文件，完成后原子 rename。

## LFU 策略

### 元数据

每个 entry 维护：

- `size`
- `freq`（命中计数）
- `last_access_unix`
- `inflight_ref`（被活跃 reader 引用次数）
- `path`

### 淘汰

- 触发条件：`total_bytes > cache_max_bytes`。
- 选择策略：`freq` 升序优先；同频按 `last_access` 更旧优先（LFU + aging）。
- 跳过项：
  - `inflight_ref > 0`（正在使用，不能删）
  - 当前请求刚写入的保护 key

### aging（避免“历史高频永不淘汰”）

- 周期性或在淘汰前对 `freq` 衰减，例如 `freq = max(1, freq/2)`。
- 衰减周期可配置（如 10~30 分钟）。

## “正在打开的文件不删除”设计

因为缓存对象是请求结果文件，语义等价于“正在使用的缓存文件不删除”：

- 读取命中时 `inflight_ref++`，请求结束 `--`（`defer` 保证）。
- 淘汰线程只处理 `inflight_ref==0`。
- 即便并发淘汰和读取竞争，也通过“索引锁 + 文件存在性校验”保证安全。

## 大文件限制（类似 ccache）

不增加 `max_cacheable_read_bytes` / `max_cacheable_entry_bytes` 等单请求大小限制。

仅使用总容量上限：

- `cache_max_bytes`：缓存目录总大小上限，超限触发 LFU 淘汰。

## 读流程（命中/回源）

1. `afs_mount` 调 proxy 的 `ReadLayer(digest, offset, length)`。
2. 以 `digest+offset+length` 查本地缓存索引。
3. 命中：直接读本地缓存文件并返回，同时增加 `freq`。
4. 未命中：向远端 layerstore 发起一次同参数回源，写入本地缓存文件，再返回。
5. 若回源失败：
   - 尝试 provider failover（重新 discovery + 重试）。
6. 返回结果给 mount。

## 一致性与正确性

- 基于 digest 内容寻址，layer 内容不可变，可长期缓存。
- key 仅使用 `digest+offset+length`，不带 provider，最大化复用率。

## 并发与单飞

- 同 key miss 时启用 singleflight，避免并发回源放大。
- 写入采用临时文件 + rename，避免脏文件暴露。

## 可观测性

建议新增指标：

- `proxy_cache_hits_total`
- `proxy_cache_misses_total`
- `proxy_cache_evictions_total`
- `proxy_cache_bytes`
- `proxy_cache_inflight_entries`
- `proxy_read_fallback_total`（failover 次数）

日志建议输出：

- 命中/未命中、请求范围、回源时延、淘汰量、跳过淘汰量（因 inflight）。

## 配置项（建议）

- `-listen`
- `-discovery-addr`（可多地址）
- `-node-id`
- `-cache-dir`
- `-cache-max-bytes`
- `-cache-evict-interval`
- `-cache-freq-aging-interval`

## 落地阶段建议

### Phase 1（最小可用）

- 实现 proxy 的 `EnsureLayers/StatLayer/ReadLayer`。
- 实现请求级落盘缓存（`digest+offset+length`）+ LFU 淘汰 + inflight 保护。
- `afs_mount` 增加 `-layerstore-proxy-addr`（可选），优先走 proxy。

### Phase 2（增强）

- 指标与管理接口（如 `Prune`、`Stats`）。
- 更稳健索引存储（bolt/badger）和崩溃恢复。
- 更细粒度热点识别（按请求分布调优）。

## 风险与权衡

- 引入新 hop，冷启动时延可能上升。
- 请求级 key 命中率依赖读取模式；若 `(offset,length)` 分布抖动，命中率会下降。
- 索引与数据一致性需要严格处理（启动重建与校验）。
- 若后续要做“层内文件语义缓存”（按路径/文件内容），需要额外解析 AFSLYR TOC，复杂度更高。

---

## 当前确认的决策

1. proxy 复用 `layerstore.v1.LayerStore` 协议，对 mount 透明接入。
2. 缓存 key 使用 `digest+offset+length`。
3. 不引入 `cache_chunk_size` 机制。
4. 不引入 `max_cacheable_read_bytes` 变量。
5. `afs_mount` 支持“优先 proxy，失败回退直连 layerstore”。

## 仍需确认的边界点

1. `digest+offset+length` 是否需要额外绑定 `resp.eof`（避免同 key 因异常短读导致脏命中）？
2. 回退策略是“proxy 连接失败才回退”，还是“proxy 任意 RPC 错误都回退”？
