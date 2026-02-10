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
  - 提供 `PullImage / HasImage / HasLayer / StatLayer / ReadLayer / PruneCache` 等能力
  - 启动后向一个或多个 discovery 服务周期性上报
  - 支持缓存大小限制与 LRU 淘汰
    - 默认上限：`min(1TB, 缓存所在文件系统可用空间的60%)`
    - 可通过 `-cache-max-bytes` 配置
    - `PullImage` 下载前会做磁盘空间预留（并发安全）
    - 如果镜像预估大小超过缓存上限，直接失败
    - 超限时按访问时间做 LRU 淘汰，并触发心跳上报

- `afs_mount`
  - 只连接 discovery
  - 先找到包含 image 的节点，再挂载
  - 读失败时可在可用 provider 之间自动切换重试
  - 读 layer 时会从 discovery 重新获取 provider，便于节点变化快速生效

- `afslet`
  - 基于 `afs_mount + afs_runc` 的流式执行服务
  - 支持资源准入（`cpu_cores`、`memory_mb`）并可查询运行时资源状态

- `afs_proxy`
  - 转发 `Afslet.Execute`
  - 按请求资源（`cpu_cores/memory_mb`）在 DNS 发现到的 afslet 节点中调度（每次下发都重新解析 A 记录）
  - 在可执行节点中使用 P2C（power-of-two-choices）选择负载较低节点
  - 支持下发重试（`proxy_dispatch_max_retries`、`proxy_dispatch_backoff_ms`）
  - 提供 HTTP `/dispatching` 查询本地/集群下发阶段排队数

## 运行方式（简述）

1. 启动 `afs_discovery_grpcd`
2. 在各节点启动 `afs_layerstore_grpcd` 并注册到 discovery
3. 任务侧运行 `afs_mount`，指定目标 image 与 mountpoint

本地一体化启动可以直接用 `docker-compose.yaml`（包含 discovery、layerstore、afslet、afs_proxy）。

## 常用命令

- 编译全部二进制：
  - `make build-local`

- 启动带缓存上限的 layerstore：
  - `./dist/linux_amd64/afs_layerstore_grpcd -listen 127.0.0.1:50051 -cache-dir .cache/layerstore -node-id node-1 -discovery-endpoint 127.0.0.1:60051 -cache-max-bytes 53687091200`

- 通过 gRPC 回收约 30% 缓存：
  - 调用 `LayerStore.PruneCache`，参数 `percent=30`

- 集成测试（删除某个 provider 的 layer 文件后验证可恢复读取）：
  - `sudo go test -tags integration ./cmd/afs_mount -run TestIntegrationMountReadRecoversAfterLayerFileDeleted -v -count=1`
  - 可选：设置 DockerHub 鉴权，避免匿名拉取限流：
    - `AFS_INTEGRATION_DOCKER_AUTH_B64=<base64(username:password_or_token)>`

## 当前定位

AFS 当前聚焦于 RL 大规模并发任务中的镜像文件复用与加速。

它不是 Docker 的替代品，而是为 Agent 任务场景提供的“跨节点镜像文件访问层”。
