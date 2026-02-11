# 组件：afslet

## 作用

流式执行服务，封装 `afs_mount` 与 `afs_runc`。

## 对外提供

- gRPC 服务：`afsletpb.Afslet`
  - `Execute`（双向流）
  - `GetRuntimeStatus`（Unary）

## 交互对象

- 本地执行 `afs_mount`、`afs_runc`。
- 间接依赖 discovery 和 layerstore。

## Execute 协议

- 第一帧必须是 `StartRequest`。
- 后续按顺序上传 extra-dir 条目（目录/软链/文件块）。
- 返回流包含：
  - accepted
  - 日志
  - 执行结果
  - writable-upper 的 tar.gz 流

## 资源准入

- `cpu_cores` 与 `memory_mb` 必须设置且 > 0。
- 服务启动时有全局资源上限。
- 超上限或当前可用资源不足会拒绝。
- `GetRuntimeStatus` 返回 used/available 视图。

## 关键参数

- `-listen`
- `-mount-binary`
- `-runc-binary`
- `-discovery-addr`
- `-temp-dir`
- `-limit-cpu`
- `-limit-memory-mb`
