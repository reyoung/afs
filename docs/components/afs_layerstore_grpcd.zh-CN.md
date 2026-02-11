# 组件：afs_layerstore_grpcd

## 作用

维护本地 layer 缓存，并通过 gRPC 提供随机读取能力。

## 对外提供

- gRPC 服务：`layerstore.v1.LayerStore`
  - `PullImage`
  - `HasImage`
  - `HasLayer`
  - `StatLayer`
  - `ReadLayer`
  - `PruneCache`

## 交互对象

- OCI registry：拉取 manifest 与 blob。
- discovery：周期上报心跳。

## 核心职责

- 拉取镜像并缓存为 `.afslyr`。
- 支持 offset 读取，供挂载侧按需读 layer。
- 执行缓存容量控制和 LRU 淘汰。
- 拉取前做磁盘预算预留（并发安全）。

## 缓存策略

- `-cache-max-bytes` 可配置。
- 默认：`min(1TB, 文件系统剩余空间的 60%)`。
- 超限触发 LRU 淘汰并更新心跳。

## 心跳上报内容

- endpoint / node id
- cached images
- layer digests
- `layer_stats(digest, afs_size)`
- `cache_max_bytes`
