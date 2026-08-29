# Go-TaskEngine

Go-TaskEngine 是一个基于 Redis 的 Go 任务执行引擎。它提供客户端任务入队、服务端固定数量 worker、延时任务、失败重试、租约恢复、令牌桶限流和命令行示例。

当前仓库已经完成任务引擎核心能力、阶段 8 示例改造和阶段 9 真实 Redis 验收。阶段 9 的 benchmark、故障测试、启动脚本、GitHub Actions CI 和收口文档已完成并推送。项目不包含网页管理端或 HTTP 管理接口，也不根据小规模测试宣称生产容量。

## 当前交付状态

- 阶段 0–9：`DONE`。核心 API、Redis 状态机、服务端调度、重试死信、限流、生命周期、示例、真实 Redis 验收、benchmark、故障测试和 CI 已实现并验证。
- 生产级容量结论：未提供。未进行长时间、多客户端、多进程、跨主机压测，不宣称高并发稳定或生产可用。

阶段 9 的提交已同步到 GitHub 远程 `main`。完整过程、实测数值和边界记录在 [`TASK_ENGINE_PLAN.md`](TASK_ENGINE_PLAN.md) 与 [`docs/benchmark-report.md`](docs/benchmark-report.md)。

## 目录

- `client/`：创建任务并写入队列。
- `server/`：调度任务、运行 worker 和处理生命周期。
- `internal/`：暂不对外公开的定时器、限流和 Redis 实现。
- `examples/`：图像处理和 C2PA 签名的模拟 producer/worker；`examples/support/` 提供共享命令行配置、Redis 连通性检查和 metrics 输出。
- `docs/`：架构、运行、真实 Redis 验收和性能验证文档。
- `TASK_ENGINE_PLAN.md`：唯一的阶段计划和实际实施日志。

## 前置条件

- Go 1.24.0 或更高版本。
- 运行端到端示例和真实 Redis 测试时，需要 Redis 服务。
- 默认 Redis 地址为 `127.0.0.1:6379`；示例可通过 `-redis-addr` 或 `TASKENGINE_REDIS_ADDR` 改变地址。

## 常用验证命令

```powershell
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

当前示例是模拟业务处理，不代表已经接入真实图像处理库或真实 C2PA 签名服务。当前性能文档中的 miniredis 结果也不代表真实 Redis 网络环境的生产吞吐。

阶段 9 提供真实 Redis benchmark、功能验收测试、Windows PowerShell Redis 启停脚本和 GitHub Actions CI；所有实测条件与边界见 [`docs/benchmark-report.md`](docs/benchmark-report.md)。

## 最小演示

先启动 Redis，然后在两个终端分别运行 worker 和 producer。worker 启动时会先执行 Redis `PING`；收到 Ctrl+C 后会输出实际接入服务端 worker 路径的处理、失败、重试、死信和耗时指标。

```powershell
# 终端 1：图像 worker，Ctrl+C 停止并打印 metrics
$env:TASKENGINE_REDIS_ADDR = "127.0.0.1:6379"
go run ./examples/image-processing/cmd/worker

# 终端 2：立即成功任务
go run ./examples/image-processing/cmd/producer -duration 250ms

# 终端 2：延时任务
go run ./examples/image-processing/cmd/producer -delay 1s -duration 100ms

# 终端 2：可重试失败；max-retry=2 后进入死信归档
go run ./examples/image-processing/cmd/producer -fail -max-retry 2 -duration 10ms

# 终端 1：C2PA worker 使用不同队列
go run ./examples/c2pa-signing/cmd/worker -redis-addr 127.0.0.1:6379

# 终端 2：不可重试的非法输入，直接进入死信归档
go run ./examples/c2pa-signing/cmd/producer -invalid
```

四个命令都支持 `-redis-addr`；未指定时读取 `TASKENGINE_REDIS_ADDR`，再回退到 `127.0.0.1:6379`。producer 还支持 `-delay`、`-duration`、`-timeout`、`-max-retry`、`-fail` 和 C2PA 专用的 `-invalid`。worker 支持 `-run-for 5s` 自动停止，适合无交互演示；默认不设置时使用 Ctrl+C 停止。

## 文档

- [`docs/architecture.md`](docs/architecture.md)：架构、运行语义和配置。
- [`docs/redis-state-machine.md`](docs/redis-state-machine.md)：Redis key 和 Lua 状态转换。
- [`docs/runbook.md`](docs/runbook.md)：启动、示例、真实 Redis 验收和故障语义。
- [`docs/benchmark-report.md`](docs/benchmark-report.md)：benchmark、真实 Redis 结果和未测量项。

`asynq-master` 和 `asynq-master.zip` 是本地参考资料，不属于 Go-TaskEngine，不会被修改或提交到 GitHub。

## 阶段开发

请先阅读 [`TASK_ENGINE_PLAN.md`](TASK_ENGINE_PLAN.md)。每个阶段都必须完成代码、测试、构建、状态记录和独立 Git 提交后，才能进入下一阶段。
