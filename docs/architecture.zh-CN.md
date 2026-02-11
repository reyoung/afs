# 架构

## 总体目标

AFS 把容器镜像文件系统数据从“单机本地缓存”扩展为“集群级可复用缓存”。

## 核心服务

- `afs_discovery_grpcd`
  - 维护可用 layerstore 节点及其缓存状态。
- `afs_layerstore_grpcd`
  - 拉取镜像 layer，存储 `.afslyr`，提供随机读。
- `afs_mount`
  - 通过 discovery/layerstore 解析并挂载镜像层，构建 rootfs。
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
5. `afs_mount` 从 discovery 找 provider，并通过 layerstore 获取镜像与 layer 数据。
6. `afs_mount` 逐层 FUSE 挂载，随后做 union mount。
7. `afslet` 启动 `afs_runc`，按 CPU/内存/超时执行命令。
8. `afslet` 返回日志、结果、以及 writable diff 的 tar.gz 流。

## 发现与状态上报

- layerstore 心跳上报：
  - endpoint、node id、cached image keys、layer digests、layer size、cache max bytes。
- proxy `Status` 流式返回：
  - 全部 layerstore 实例状态，
  - 全部 afslet 实例运行态，
  - 汇总统计。

## 部署方式

- `docker-compose.yaml`
  - 本地一体化部署。
- `helm/afs`
  - Kubernetes 部署（discovery、layerstore daemonset、afslet、afs_proxy）。
