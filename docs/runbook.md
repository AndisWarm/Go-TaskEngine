# 运行手册

## 1. 环境准备

项目需要 Go 和 Redis。当前仓库不包含 Redis 服务，也不包含管理后台或 HTTP 管理接口。

```powershell
go version
redis-server --version
redis-cli ping
```

Windows 没有把 Redis 命令加入 `PATH` 时，可以指定本机安装路径：

```powershell
$env:GTE_REDIS_SERVER = "F:\Redis-8.10.0\redis\redis-server.exe"
$env:GTE_REDIS_CLI = "F:\Redis-8.10.0\redis\redis-cli.exe"
```

仓库提供 PowerShell 等价启动脚本。脚本使用临时的无持久化 Redis 实例，适合本地验收，不适合生产部署：

```powershell
.\scripts\start-redis.ps1 -Port 6379
# 演示完成后
.\scripts\stop-redis.ps1 -Port 6379
```

Docker 未作为本机验收前置条件；GitHub Actions 使用 `redis:7-alpine` 服务容器执行 CI。

## 2. 启动模拟示例

本项目有两个模拟业务：图像处理和 C2PA 签名。它们只用于展示任务引擎链路，不代表真实图像处理库或真实 C2PA 服务集成。

先启动 Redis，然后在两个终端运行 worker 和 producer：

```powershell
$env:TASKENGINE_REDIS_ADDR = "127.0.0.1:6379"

# 终端 1：worker 启动时先执行 Redis PING；Ctrl+C 停止并输出 metrics
go run ./examples/image-processing/cmd/worker

# 终端 2：立即成功、500ms 延时和失败重试归档
go run ./examples/image-processing/cmd/producer -duration 250ms
go run ./examples/image-processing/cmd/producer -delay 500ms -duration 100ms
go run ./examples/image-processing/cmd/producer -fail -max-retry 2 -duration 10ms

# C2PA 使用 c2pa 队列；invalid 直接进入不可重试归档
go run ./examples/c2pa-signing/cmd/worker -redis-addr 127.0.0.1:6379
go run ./examples/c2pa-signing/cmd/producer -invalid
```

所有 producer 支持：

- `-redis-addr`：Redis 地址，优先级高于 `TASKENGINE_REDIS_ADDR`。
- `-delay`：任务进入 ready 前的延时，例如 `1s`。
- `-duration`：模拟 handler 处理耗时。
- `-timeout`：handler 超时时间；超时进入失败/重试路径。
- `-max-retry`：最大重试次数。
- `-fail`：模拟可重试业务失败。
- C2PA producer 额外支持 `-invalid`，模拟不可重试输入错误。

所有 worker 支持 `-redis-addr` 和 `-run-for`。自动退出示例：

```powershell
go run ./examples/image-processing/cmd/worker -run-for 5s
```

worker 退出时输出的 `metrics` 来自接入 `server.Config.Metrics` 的实际 worker 路径：

```text
metrics processed=1 failed=0 retried=0 archived=0 total_duration=250ms
```

`failed`、`retried` 和 `archived` 只在对应 Redis 状态转换成功后计数；模拟业务输出不等同于真实业务指标。

## 3. 真实 Redis 验收

阶段 9 的真实 Redis 测试默认跳过，设置 `GTE_REAL_REDIS=1` 后执行。测试结果已经记录在 `docs/benchmark-report.md`：

```powershell
$env:GTE_REAL_REDIS = "1"
$env:GTE_REDIS_ADDR = "127.0.0.1:6386"
.\scripts\start-redis.ps1 -Port 6386
go test ./server -run '^TestRealRedis' -count=1 -v
.\scripts\stop-redis.ps1 -Port 6386
```

包含 500ms 延时调度、固定 worker 并发与 shutdown、重试/死信批量处理。真实入队 benchmark 和实测条件见 [`benchmark-report.md`](benchmark-report.md)。

## 4. 生命周期和故障语义

- `Stop` 是非阻塞停止请求：停止领取新任务并取消 active handler。
- `Shutdown` 停止领取和转发，取消并等待 handler/worker，然后停止 heartbeat、recovery 和本地 timer。
- `ShutdownTimeout` 到期时，当前 active 任务进入 requeue/recovery 路径并返回超时错误。
- `Run` 和 `RunSignals` 接受 nil context；nil context 按 `context.Background()` 处理。
- 任务失败会进入 retry ZSet；超过最大重试次数或返回 `server.ErrNonRetryable` 的任务进入 archived。
- 进程崩溃后，过期 lease 由 recovery loop 重新安排。
- 系统采用至少一次执行语义；故障切换或网络抖动可能造成重复执行，业务 handler 必须幂等。

## 5. 死信管理和自动化检查

公开 `storage.DeadLetterStore` 接口提供分页查询、按 ID 查询、重放、删除和按时间清理。

```powershell
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

GitHub Actions 配置位于 `.github/workflows/ci.yml`，执行相同的测试、race、构建和 vet 检查。CI 的 Redis 服务用于连通性环境，不代表生产部署方案。

## 6. 发布状态

阶段 9 的代码、测试、脚本、CI 配置和文档已推送到 GitHub 远程 `main`。GitHub Actions 工作流位于 `.github/workflows/ci.yml`，会执行测试、race、构建和 vet 检查。
