# AFS（Agent File System）

AFS 的全称是 **Agent File System**。

它是一个面向 RL 场景（例如 SWE-Agent 等）的文件系统方案，用来快速部署和复用容器镜像文件数据。

## 背景

在 RL 任务集群里，任务数量大、镜像种类多、拉取频繁。
如果每个节点都从同一个 Image Registry 重复下载相同镜像，会带来：

- Registry 压力过高
- 带宽浪费
- 启动延迟增加

AFS 的目标是把镜像数据复用范围从“单机”扩展到“整个集群”。

## 核心思想

和 Docker 的本地 layer 依赖不同，AFS 不要求所有 layer 必须在当前机器本地。

只要 layer（或对应 image）在集群中的某个节点可访问，当前节点就可以通过网络读取并挂载文件系统。

这意味着：

- 单节点磁盘不再是镜像容量上限
- 集群所有节点磁盘总和可以承载更大的镜像集合
- 避免同一镜像从 Registry 被重复下载多次

## 组件

- `afs_discovery_grpcd`
  - 服务发现与节点状态管理（心跳）
  - 返回包含某个 image 的节点 endpoint 列表

- `afs_layerstore_grpcd`
  - 管理 layer 缓存
  - 提供 `PullImage / HasImage / HasLayer / StatLayer / ReadLayer` 等能力
  - 启动后向一个或多个 discovery 服务周期性上报

- `afs_mount`
  - 只连接 discovery
  - 先找到包含 image 的节点，再挂载
  - 读失败时可在可用 provider 之间自动切换重试

## 运行方式（简述）

1. 启动 `afs_discovery_grpcd`
2. 在各节点启动 `afs_layerstore_grpcd` 并注册到 discovery
3. 任务侧运行 `afs_mount`，指定目标 image 与 mountpoint

## 当前定位

AFS 当前聚焦于 RL 大规模并发任务中的镜像文件复用与加速。

它不是 Docker 的替代品，而是为 Agent 任务场景提供的“跨节点镜像文件访问层”。
