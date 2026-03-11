# 组件：afs_discovery_grpcd

## 作用

维护集群中 layerstore 节点的在线状态和缓存视图（内存态）。

## 对外提供

- gRPC 服务：`discovery.v1.ServiceDiscovery`
  - `Heartbeat`
  - `FindImage`
  - `ResolveImage`
  - `FindProvider`
  - `FindImageProvider`

## 从哪里接收数据

- `afs_layerstore_grpcd` 周期心跳。

## 给谁使用

- `afs_mount`：做 image 解析与 provider 发现（`ResolveImage`、`FindImageProvider`、`FindProvider`）。
- `afs_proxy`：做副本协调与 layerstore 状态聚合。

## 关键状态字段

- node id / endpoint
- cached image keys
- layer digests 与每层大小
- 节点 `cache_max_bytes`
- last seen 时间

## 运行特点

- 不存储 layer 文件内容。
- 属于控制面，不是数据面。
