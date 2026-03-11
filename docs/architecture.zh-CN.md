# 架构

## 总体目标

AFS 把容器镜像文件系统数据从“单机本地缓存”扩展为“集群级可复用缓存”。

## 核心服务

- `afs_discovery_grpcd`
  - 维护可用 layerstore 节点、layer 覆盖情况，以及已解析 image 元数据。
- `afs_layerstore_grpcd`
  - 补齐所需 layer，存储 `.afslyr`，提供随机读。
- `afs_mount`
  - 通过 discovery 解析 image、在选定 layerstore 上补齐缺失 layer，并挂载镜像层构建 rootfs。
- `afs_runc`
  - 在准备好的 rootfs 中执行命令，并施加 cgroup CPU/内存限制与超时控制。
- `afslet`
  - 接收流式请求，调用 `afs_mount` + `afs_runc` 执行命令，返回可写层 tar.gz。
- `afs_proxy`
  - 代理 `Afslet.Execute`，负责按资源调度和重试。

## 典型执行链路

1. 客户端（`afs_cli` / Python SDK）连接 `afs_proxy`，发起 `Execute`。
2. `afs_proxy` 解析 afslet DNS，查询状态并选择后端，转发流。
3. `afslet` 做资源准入，接收并构建 extra-dir。
4. `afslet` 启动 `afs_mount`。
5. `afs_mount` 调用 discovery 的 `ResolveImage`，拿到 resolved reference 和有序 layer 列表。
6. `afs_mount` 向 discovery 查询完整 image provider；如果没有合适 provider，就在选中的 layerstore 上执行 `EnsureLayers`。
7. `afs_mount` 逐层 FUSE 挂载，随后做 union mount。
8. `afslet` 启动 `afs_runc`，按 CPU/内存/超时执行命令。
9. `afslet` 返回日志、结果、以及 writable diff 的 tar.gz 流。

## 发现与状态上报

- layerstore 心跳上报：
  - endpoint、node id、layer digests、layer size、cache max bytes。
- discovery 内部维护：
  - `image_key -> resolved ref + ordered layers[]` 的 image 记录，
  - 基于 layer 心跳构建的 provider 映射，
  - 基于 layer 覆盖关系推导出的完整 image provider。
- proxy `Status` 流式返回：
  - 全部 layerstore 实例状态，
  - 全部 afslet 实例运行态，
  - 汇总统计。

## 部署方式

- `docker-compose.yaml`
  - 本地一体化部署。
- `helm/afs`
  - Kubernetes 部署（discovery、layerstore daemonset、afslet、afs_proxy）。
