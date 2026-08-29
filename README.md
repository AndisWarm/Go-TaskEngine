# Go-TaskEngine

Go-TaskEngine 是一个基于 Redis 的 Go 任务执行引擎。它提供客户端任务入队、服务端固定数量 worker、延时任务、失败重试、租约恢复、令牌桶限流和命令行示例。

当前仓库的目标是先完成可测试的 Go 核心库，再按阶段补齐公开 API、故障验证和真实 Redis 压测。项目不包含网页管理端或 HTTP 管理接口。

## 目录

- `client/`：创建任务并写入队列。
- `server/`：调度任务、运行 worker 和处理生命周期。
- `internal/`：暂不对外公开的定时器、限流和 Redis 实现。
- `examples/`：图像处理和 C2PA 签名的模拟 producer/worker。
- `docs/`：架构、运行和性能验证文档。
- `TASK_ENGINE_PLAN.md`：唯一的阶段计划和实际实施日志。

## 前置条件

- Go 1.24.0 或更高版本。
- 运行端到端示例和真实 Redis 测试时，需要 Redis 服务。
- 默认 Redis 地址为 `127.0.0.1:6379`；示例的配置方式会在后续阶段完善。

## 常用验证命令

```powershell
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

当前示例是模拟业务处理，不代表已经接入真实图像处理库或真实 C2PA 签名服务。当前性能文档中的 miniredis 结果也不代表真实 Redis 网络环境的生产吞吐。

## 参考项目

`asynq-master` 和 `asynq-master.zip` 是本地参考资料，不属于 Go-TaskEngine，不会被修改或提交到 GitHub。

## 阶段开发

请先阅读 [`TASK_ENGINE_PLAN.md`](TASK_ENGINE_PLAN.md)。每个阶段都必须完成代码、测试、构建、状态记录和独立 Git 提交后，才能进入下一阶段。
