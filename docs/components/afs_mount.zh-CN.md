# 组件：afs_mount

## 作用

把镜像构造成可执行 rootfs：

- 只读 layer 挂载
- 可选 extra-dir（只读）
- writable upper（可写层）

## 交互对象

- discovery：查找 provider。
- layerstore：拉镜像与按需读 layer。

## 挂载流程

1. discovery 查 provider。
2. 从选定 provider 拉取镜像元数据。
3. 逐层 FUSE 挂载 layer。
4. 通过 `fuse-overlayfs` 做 union mount。
5. 可选挂载 `/proc`、`/dev`。

## 可用性特性

- 读失败时 provider failover。
- 读取过程中可重新从 discovery 获取 provider。
- 如果 mount 子进程提前失败，会及时退出等待。

## 关键参数

- `-mountpoint`
- `-image` / `-tag`
- `-discovery-addr`
- `-extra-dir`
- `-mount-proc-dev`
- `-work-dir`
- `-force-local-fetch`
