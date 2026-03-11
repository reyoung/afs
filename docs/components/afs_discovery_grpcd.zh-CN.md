# 组件：afs_discovery_grpcd

## 作用

维护集群中 layerstore 节点的在线状态、layer 覆盖视图，以及 image 解析缓存（内存态）。

## 对外提供

- gRPC 服务：`discovery.v1.ServiceDiscovery`
  - `Heartbeat`
  - `FindImage`
  - `ResolveImage`

## 从哪里接收数据

- `afs_layerstore_grpcd` 周期心跳。

## 给谁使用

- `afs_mount`：做 image 解析与 provider 发现（`ResolveImage`、`FindImage`）。
- `afs_proxy`：做副本协调与 layerstore 状态聚合。

## 关键状态字段

- node id / endpoint
- layer digests 与每层大小
- image 记录：image key、resolved registry/repository/reference、有序 layer 列表
- 基于 layer 覆盖关系推导出的 image/layer provider 集合
- 节点 `cache_max_bytes`
- last seen 时间

## 运行特点

- 存储 image 元数据缓存和 provider 索引，但不存储 layer 文件内容。
- 属于控制面，不是数据面。
- image 是否“完整可用”由 layer 覆盖关系推导得出，不通过单独的 cached image 列表上报。
