# 运行手册

## 启动 Redis 和模拟示例

本项目示例默认连接 `127.0.0.1:6379`。可以通过环境变量统一配置，也可以给每个命令传入 `-redis-addr`；命令启动时会先执行 Redis `PING`，连接失败会立即退出并显示地址和错误。

```powershell
$env:TASKENGINE_REDIS_ADDR = "127.0.0.1:6379"

# 终端 1：启动 worker；Ctrl+C 停止并输出实际 metrics
go run ./examples/image-processing/cmd/worker

# 终端 2：入队立即任务、延时任务和可重试失败任务
go run ./examples/image-processing/cmd/producer -duration 250ms
go run ./examples/image-processing/cmd/producer -delay 1s -duration 100ms
go run ./examples/image-processing/cmd/producer -fail -max-retry 2 -duration 10ms

# C2PA 使用独立队列；invalid 是不可重试示范
go run ./examples/c2pa-signing/cmd/worker -redis-addr 127.0.0.1:6379
go run ./examples/c2pa-signing/cmd/producer -invalid
```

所有 producer 都支持：

- `-redis-addr`：Redis 地址，优先级高于 `TASKENGINE_REDIS_ADDR`。
- `-delay`：入队后等待指定时间再变为 ready，例如 `1s`。
- `-duration`：模拟业务处理耗时。
- `-timeout`：任务 handler 超时时间；超时会走失败/重试路径。
- `-max-retry`：最大重试次数。
- `-fail`：模拟可重试业务失败。
- C2PA producer 额外支持 `-invalid`，模拟不可重试输入错误并直接归档。

所有 worker 都支持 `-redis-addr` 和 `-run-for`。例如 `go run ./examples/image-processing/cmd/worker -run-for 5s` 会在 5 秒后自动执行 shutdown 并输出 metrics，适合自动化演示；没有 `-run-for` 时使用 Ctrl+C。

worker 输出的 `metrics` 来自传给 `server.Config.Metrics` 的 `server.Metrics` 快照，格式为：

```text
metrics processed=1 failed=0 retried=0 archived=0 total_duration=250ms
```

这里的 `failed`、`retried` 和 `archived` 是服务端实际完成相应状态转换后记录的计数；模拟 handler 的处理结果不代表真实图像或 C2PA 业务集成。

- `Stop` 是非阻塞停止请求：停止领取新任务并取消 active handler；调用方需要使用 `Shutdown` 等待 worker 和维护循环完成。
- `Shutdown` 的顺序是停止领取和转发、取消并等待 handler/worker，再停止 heartbeat、recovery 和本地 timer。超过 `ShutdownTimeout` 时，将当前 active 任务执行 requeue 并返回超时错误。
- `Run` 和 `RunSignals` 接受 nil context；nil context 按 `context.Background()` 处理，服务仍可通过 `Stop` 结束。

## 重试与死信管理

- 普通 handler 错误按 `RetryBaseDelay * 2^RetryCount` 计算下一次执行时间；`RetryJitter` 是对基础退避的对称随机比例，取值范围为 `0` 到 `1`，`RetryMaxDelay` 限制最终延时上限。
- 返回 `server.ErrNonRetryable` 的错误直接进入 archived；达到 `MaxRetry` 后也进入 archived。归档记录保留任务内容、重试次数、最后错误和失败时间。
- `storage.DeadLetterStore` 提供分页查询、按 ID 查询、重放、删除和按时间清理。重放会清零重试次数、清除当前错误，并以 pending 状态重新入队。
- recovery loop 定期检查过期 lease。恢复和业务处理之间采用至少一次执行语义，故障切换或网络抖动可能造成重复执行，业务 handler 必须幂等。

## 死信操作示例

公开 `storage.DeadLetterStore` 接口提供以下操作：

```go
page, err := deadLetters.ListDeadLetters(ctx, "default", 0, 20)
msg, err := deadLetters.GetDeadLetter(ctx, "default", taskID)
err = deadLetters.ReplayDeadLetter(ctx, "default", taskID)
err = deadLetters.DeleteDeadLetter(ctx, "default", taskID)
removed, err := deadLetters.CleanupDeadLetters(ctx, "default", time.Now().Add(-24*time.Hour), 100)
```

重放会清零重试次数并将任务放回 pending；死信操作仍遵循至少一次执行语义。

## 分布式限流

- `limiter.NewScopedTokenBucket` 使用 `gte:limiter:<scope>` 作为 Redis key；相同 scope 的不同 server 共享容量和补充速率，不同 scope 相互隔离。
- Lua 脚本使用 Redis 服务端时间完成原子补充和扣减；server 在 `Claim` 前获取令牌，令牌不足时依据 `RetryAfter` 等待，不会领取 pending 任务。
- miniredis 和真实 Redis 测试均覆盖 burst、补充、并发竞争和双 server 共享 bucket。真实 Redis 双 server 测试中，容量为 1、速率为 10 token/s，4 个任务的 handler 时间跨度实测为 304.7ms；该结果不代表生产吞吐保证。

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
