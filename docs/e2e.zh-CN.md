# End-to-End 测试

本文档定义 AFS 的端到端 smoke 测试入口，覆盖三种运行形态：

- 裸进程
- Docker Compose
- Kubernetes + Helm

统一入口是 `scripts/e2e/smoke.sh`。这个脚本默认验证两件事：

- `afs_cli -proxy-status`
- `afs_cli reconcile-image-replica`

如果你要专门回归 `reconcile-image-replica` 的稳定性、分布结果和重复执行行为，使用：

```bash
./scripts/e2e/reconcile_regression.sh --image mirrors.tencent.com/josephyu/afs --tag <tag>
```

这个脚本会：

- 用更高的 `-count` 重复运行 `pkg/afsproxy` 里的 `reconcile-image` 相关测试
- 重复执行 `bare` 模式 smoke
- 可选重复执行 `helm` 模式 smoke（`--helm-start-stop` 会自动调用 `./helm/start.sh` / `./helm/stop.sh`）
- 在执行 Helm smoke 前，先等待该 release 下的 Deployment / DaemonSet rollout 完成，并额外等待一小段 heartbeat 收敛时间

它验证的是 discovery、layerstore、afslet、afs_proxy 这一整条控制面/调度链路是否连通。完整的特权执行链路（`afs_mount + afs_runc`）仍然建议继续用现有 integration 测试单独覆盖。

## 前置条件

- Linux 环境
- 已构建本地二进制：`make build-local`
- `bare` 模式需要本机可直接监听本地端口
- `compose` 模式需要 `docker compose`
- `helm` 模式需要可访问 Kubernetes 集群，并且部署生命周期必须通过 `./helm/start.sh` / `./helm/stop.sh`

默认测试镜像是 `registry.k8s.io/pause:3.9`。在当前仓库环境里，只要关闭宿主机 `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` 继承，这个镜像路径可以稳定通过 smoke。你也可以显式覆盖：

```bash
./scripts/e2e/smoke.sh --mode bare --image alpine --tag 3.20
```

## 1. 裸进程

```bash
make build-local
./scripts/e2e/smoke.sh --mode bare
```

脚本会在本机启动：

- `afs_discovery_grpcd`
- `afs_layerstore_grpcd`
- `afslet`
- `afs_proxy`

然后通过本地 `afs_proxy` 地址执行 smoke 校验。

## 2. Docker Compose

```bash
make build-local
./scripts/e2e/smoke.sh --mode compose
```

脚本行为：

- 脚本会先用仓库根目录的 `Dockerfile` 构建本地运行时镜像 `afs-local:compose`，并且该构建步骤不继承宿主机 HTTP 代理
- compose 服务直接使用这个本地镜像，`afslet` 不会在容器启动阶段执行 `apt-get`
- 如果 compose 栈未启动，会先执行 `docker compose up -d`
- 如果是脚本本次启动的 compose 栈，退出时会自动 `docker compose down`
- 如果 compose 栈原本就在运行，脚本只复用，不会替你停掉

## 3. Kubernetes + Helm

先部署：

```bash
./helm/start.sh
```

再执行 smoke：

```bash
./scripts/e2e/smoke.sh --mode helm --namespace afs
```

最后清理：

```bash
./helm/stop.sh
```

说明：

- `helm` 模式默认通过 `kubectl port-forward` 连接集群里的 `afs-proxy` Service
- 如果没有显式传 `--image/--tag`，脚本会自动读取当前 `afs-afslet` Deployment 正在使用的镜像作为 smoke 镜像
- 如果你已经有可访问的代理地址，也可以直接传 `--addr <host:port>`
- Helm 部署生命周期不要直接用 `helm upgrade/install/uninstall`，正常流程统一走 `./helm/start.sh` / `./helm/stop.sh`

## 代理注意事项

`helm/start.sh` 与 `helm/stop.sh` 已经内置了无代理调用包装，会自动清理这些环境变量后再调用 `helm` / `kubectl`：

- `http_proxy`
- `https_proxy`
- `all_proxy`
- `HTTP_PROXY`
- `HTTPS_PROXY`
- `ALL_PROXY`

另外，`scripts/e2e/smoke.sh` 在 `bare` 模式下启动的本地组件，以及 `helm` 模式里的 `kubectl` 调用，也会使用同样的无代理方式执行。这样可以避免 discovery / layerstore 走宿主机 `HTTP_PROXY` 后拿到错误的 registry 响应。

## 常用参数

```bash
./scripts/e2e/smoke.sh --help
```

常见覆盖项：

- `--addr <host:port>`：手动指定 `afs_proxy` gRPC 地址
- `--namespace <name>`：指定 Helm 模式的命名空间
- `--image <name> --tag <tag>`：覆盖测试镜像
- `--replica <n>`：指定 `reconcile-image-replica` 目标副本数
- `--skip-reconcile`：只做 `proxy-status` 校验

## 输出

脚本会把日志写到：

```text
.tmp/e2e/<timestamp>/
```

其中包括：

- `proxy-status.txt`
- `reconcile-image.txt`
- `kubectl-port-forward.log`（仅 Helm 模式）
- 本地子进程日志（仅 bare 模式）
