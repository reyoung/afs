# 快速开始

本文档说明当前推荐的 Kubernetes 部署方式：使用项目内的 Helm 脚本。

## 前置条件

- Linux 环境，已安装 `git`、`docker`、`helm`、`kubectl`
- 可访问 Kubernetes 集群
- 有镜像仓库推送权限

## 1. 配置环境变量

创建 `helm/.env`：

```env
NAMESPACE=afs
IMAGE_REPOSITORY=mirrors.tencent.com/josephyu/afs
RELEASE_NAME=afs
CHART_PATH=./helm/afs

# 副本数
DISCOVERY_REPLICAS=3
AFS_PROXY_REPLICAS=3
AFSLET_REPLICAS=3

# afslet 运行时资源限制（传给 afslet 进程）
AFSLET_LIMIT_CPU_CORES=4
AFSLET_LIMIT_MEMORY_MB=16384

# afslet Pod 资源请求/上限
AFSLET_REQUEST_CPU=4
AFSLET_REQUEST_MEMORY=16Gi
AFSLET_LIMIT_CPU=4
AFSLET_LIMIT_MEMORY=16Gi
```

说明：
- `helm/.env` 已在 `.gitignore` 中，不会提交。
- 若 `helm/.env` 不存在，`start.sh`/`stop.sh` 会打印模板并退出。

## 2. 启动 AFS

```bash
./helm/start.sh
```

脚本行为：
- 构建并推送新镜像，tag 格式为 `<git-sha>-<datetime>`（工作区脏时追加 `-dirty`）
- 安装或升级 Helm Release
- 使用 `.env` 中默认值（如有 `--set ...` 显式参数则以显式参数为准）
- 对 `helm`/`kubectl` 调用自动禁用代理环境变量

## 3. 验证部署

```bash
kubectl -n afs get deploy,ds,svc
kubectl -n afs get pods -l app.kubernetes.io/component=afslet -o wide
```

默认期望：
- `discovery`：3 副本
- `afs-proxy`：3 副本
- `afslet`：可配置（示例为 3 副本）

`afslet` 仍然是 Deployment（不是 DaemonSet），并启用了“尽量跨节点分散”的 preferred pod anti-affinity。

## 4. 停止 AFS

```bash
./helm/stop.sh
```

## 可选：命令行覆盖 Helm 参数

```bash
./helm/start.sh --set afslet.replicas=6 --set-string afslet.resources.requests.memory=32Gi
```
