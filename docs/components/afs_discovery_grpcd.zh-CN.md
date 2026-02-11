# 组件：afs_discovery_grpcd

## 作用

维护集群中 layerstore 节点的在线状态和缓存视图（内存态）。

## 对外提供

- gRPC 服务：`discovery.v1.ServiceDiscovery`
  - `Heartbeat`
  - `FindImage`

## 从哪里接收数据

- `afs_layerstore_grpcd` 周期心跳。

## 给谁使用

- `afs_mount`：查找可用镜像 provider。
- `afs_proxy`：聚合 layerstore 集群状态。

## 关键状态字段

- node id / endpoint
- cached image keys
- layer digests 与每层大小
- 节点 `cache_max_bytes`
- last seen 时间

## 运行特点

- 不存储 layer 文件内容。
- 属于控制面，不是数据面。
