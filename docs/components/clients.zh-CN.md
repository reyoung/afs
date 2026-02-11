# 组件：客户端（afs_cli 与 Python SDK）

## afs_cli

### 作用

命令行客户端，覆盖执行与状态查询。

### 交互对象

- `Afslet.Execute`
- `Afslet.GetRuntimeStatus`
- `AfsProxy.Status`

### 能力

- 上传本地目录为 extra-dir 流。
- 发送执行请求（镜像/命令/资源）。
- 打印流式日志和结果。
- 保存 writable-upper tar.gz。
- 查询 proxy 状态流（`-proxy-status`）。

## Python SDK（`python/afs_sdk`）

### 作用

基于 asyncio + grpclib 的异步客户端库。

### API 分层

- 低层：`raw_execute()`（直接 protobuf 帧）
- 高层：
  - `execute(ExecuteInput)`（类型化请求与事件）
  - `status()`（类型化 proxy 状态事件）

### 流式特性

- 上传支持 `bytes` 和 `async iterator[bytes]`。
- 下载侧对 tar.gz 做增量解析，按目录/文件分片流式返回。
