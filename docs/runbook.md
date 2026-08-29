# 运行手册

## 启动 Redis

本项目默认连接 `127.0.0.1:6379`。启动 Redis 后，可以分别运行示例目录下的 producer 和 worker：

```powershell
go run ./examples/image-processing/cmd/worker
go run ./examples/image-processing/cmd/producer

go run ./examples/c2pa-signing/cmd/worker
go run ./examples/c2pa-signing/cmd/producer
```

worker 收到 Ctrl+C 或 `SIGTERM` 后停止领取新任务，等待现有任务；超过 `ShutdownTimeout` 的任务会进入 requeue/recovery 路径。

## 测试

不需要外部 Redis 的单元测试使用 miniredis。完整验证命令为：

```powershell
go test ./...
go test -race ./...
go build ./...
go test ./server -bench BenchmarkClientEnqueue -benchmem -run '^$'
```

## 故障语义

- 任务失败不代表立即丢弃：可重试错误进入 retry ZSet。
- 超过最大重试次数或返回 `server.ErrNonRetryable` 的任务进入 archived。
- 进程崩溃后，过期 lease 由 recovery loop 重新安排。
- 任务可能重复执行，业务代码必须自行保证幂等性。
- 令牌桶限制的是实际领取速度；令牌不足时 worker 不领取 pending 任务。
