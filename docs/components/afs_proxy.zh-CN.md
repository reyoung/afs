# 组件：afs_proxy

## 作用

把 `Afslet.Execute` 下发到多个 afslet 后端，并做资源感知调度与重试。

## 对外提供

- gRPC：
  - `afsletpb.Afslet.Execute`（代理实现）
  - `afsproxypb.AfsProxy.Status`（流式状态）
- HTTP：
  - `/status`
  - `/dispatching`（兼容旧路径）

## 交互对象

- 每次下发都会重新解析 afslet DNS（A 记录）。
- 调用 afslet `GetRuntimeStatus` 做容量判断。
- `Status` 可查询 discovery 聚合 layerstore 信息。
- 可选查询其他 proxy 的排队数。

## 调度策略

1. 先过滤能满足 `cpu_cores`/`memory_mb` 的节点。
2. 如果全局都不可能满足，直接返回参数错误。
3. 如果暂时无空闲，进入重试。
4. 使用 P2C（power-of-two-choices）选择相对更空闲节点。
5. 线性 backoff + jitter 重试，直到成功或达到重试上限。

## 状态接口

- gRPC `Status` 流式返回：
  - layerstore 实例状态（含 layer size 与 cache 上限）
  - afslet 实例状态（可达性与资源占用）
  - 汇总信息
- HTTP `/status`：
  - 查询 dispatching 队列数
  - `include_cluster` 默认 `true`
