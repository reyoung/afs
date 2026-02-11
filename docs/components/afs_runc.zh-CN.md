# 组件：afs_runc

## 作用

基于 `runc` 在指定 rootfs 中执行命令，并施加 CPU/内存/超时限制。

## 输入

- rootfs 路径
- command argv
- `-cpu`、`-memory-mb`、`-timeout`

## 行为

- 动态生成 OCI bundle（`config.json`）。
- 使用 cgroup 限制 CPU/内存。
- 超时后强制终止容器。
- 结束后清理容器状态。

## 网络与 DNS 默认行为

- 默认使用 host network（不创建独立 network namespace）。
- 默认只读 bind host `/etc/resolv.conf` 到容器，保证 DNS 可用。

## 默认 mount

- `/proc`
- `/dev`、`/dev/pts`、`/dev/shm`、`/dev/mqueue`
- `/sys`（只读）
- `/etc/resolv.conf`（bind ro）
