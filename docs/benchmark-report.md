# 性能压测记录

## 测试时间

2026-08-29，Windows amd64，Intel 12th Gen Core i5-1240P。

## 已实际执行

命令：

```powershell
go test ./server -bench BenchmarkClientEnqueue -benchmem -run '^$' -benchtime=1s
```

结果：

```text
BenchmarkClientEnqueue-16     4702    338497 ns/op    259004 B/op    843 allocs/op
PASS
```

该 benchmark 使用 miniredis 模拟 Redis，测量的是 Client 组装任务并通过 Redis 脚本写入的本地测试路径，不等同于真实 Redis 网络环境下的生产吞吐。当前没有据此宣称高并发稳定或生产容量。

## 尚未测量的项目

以下指标需要后续在明确 Redis 版本、网络、payload 大小和并发参数后单独测量：

- 多 server 实例下的任务消费吞吐。
- 500 毫秒延时任务的实际误差分布。
- 不同 `Concurrency` 下的 CPU、内存和最大并发。
- retry/dead-letter 吞吐和恢复时间。
- 跨多个 server 共享 token bucket 时的实际消费速率。
- 真实独立进程收到 Ctrl+C/SIGTERM 后的退出时间。
