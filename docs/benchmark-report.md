# 性能与真实 Redis 验收记录

## 测试环境

- 测试日期：2026-08-29。
- 操作系统：Windows amd64。
- CPU：12th Gen Intel(R) Core(TM) i5-1240P。
- Go：`go1.26.5 windows/amd64`。
- Redis：Redis 8.10.0，本机 `redis-server.exe`，回环地址，不代表跨主机网络环境。
- Docker：当前验收机器未安装 Docker；CI 使用 GitHub Actions Ubuntu runner 和 `redis:7-alpine` 服务容器。

## 可重复命令

### miniredis 基准

```powershell
go test ./server -bench '^BenchmarkClientEnqueue$' -benchmem -run '^$' -benchtime=1s
```

该 benchmark 使用 miniredis，适合比较本地代码变化，不作为真实 Redis 吞吐结论。

### 真实 Redis 入队基准

先确认 `redis-server` 和 `redis-cli` 已在 `PATH`，或指定 Windows 路径：

```powershell
$env:GTE_REDIS_SERVER = "F:\Redis-8.10.0\redis\redis-server.exe"
$env:GTE_REDIS_CLI = "F:\Redis-8.10.0\redis\redis-cli.exe"
$env:GTE_REAL_REDIS = "1"
$env:GTE_REDIS_ADDR = "127.0.0.1:6385"

.\scripts\start-redis.ps1 -Port 6385
go test ./server -run '^$' -bench '^BenchmarkClientEnqueueRealRedis$' -benchmem -benchtime=1s -count=1
.\scripts\stop-redis.ps1 -Port 6385
```

真实 benchmark 使用独立运行队列和运行 ID，不会消费默认队列中的业务任务；benchmark 结束时清理该队列的 Redis 键。

实测结果：

```text
BenchmarkClientEnqueueRealRedis-16     7285    202047 ns/op    2175 B/op    36 allocs/op
PASS
```

采样条件：`-benchtime=1s`，默认 Go benchmark 并行度 `GOMAXPROCS=16`，payload 为 7 字节 `payload`，单次运行约 7,285 次入队；未进行多客户端并发压测，因此不推导高并发稳定性或生产吞吐。

Windows 真实 Redis 实例可使用 `scripts/start-redis.ps1` 和 `scripts/stop-redis.ps1` 启停。脚本为每个端口创建独立临时数据目录，避免默认工作目录中的 `dump.rdb` 污染验收。

## 真实 Redis 功能验收

执行完整的真实 Redis 验收测试：

```powershell
$env:GTE_REDIS_SERVER = "F:\Redis-8.10.0\redis\redis-server.exe"
$env:GTE_REDIS_CLI = "F:\Redis-8.10.0\redis\redis-cli.exe"
$env:GTE_REAL_REDIS = "1"
$env:GTE_REDIS_ADDR = "127.0.0.1:6386"

.\scripts\start-redis.ps1 -Port 6386
go test ./server -run '^TestRealRedis' -count=1 -v
.\scripts\stop-redis.ps1 -Port 6386
```

实测结果：

- 真实 Redis 500ms 延时调度：目标误差 `2.4021ms`，任务未提前执行。
- 固定 worker 并发：20 个任务，`Concurrency=4`，观测最大并发为 `4`；shutdown 回收耗时为测试日志精度下的 `0s`。
- 重试/死信路径：10 个不可重试任务，归档耗时 `10.314ms`，观测吞吐约 `969.6 tasks/s`。该结果是本机小批量单次实验，不是容量承诺。
- 阶段 6 已有真实 Redis 双 server 共享 Token Bucket 验收：4 个任务时间跨度 `303.5821ms`，容量为 1、补充速率为 10 token/s。
- 阶段 5 已有真实 Redis 独立进程 lease recovery 验收。

这些测试使用运行 ID 隔离任务键，但不会清理用户共享 Redis 中其他数据。生产环境应使用独立数据库、命名空间或清理策略。

## 发布状态

阶段 9 的代码、测试、脚本、CI 配置和文档已推送到 GitHub 远程 `main`。GitHub Actions 工作流位于 `.github/workflows/ci.yml`，会执行普通测试、竞态测试、构建和 `go vet` 检查。CI 的 Redis 服务用于自动化测试环境，不代表生产部署方案。

## 已知边界与未测量项

- 未进行长时间、多进程、多客户端并发压测。
- 未测量 CPU、内存、P95/P99 延迟或跨主机网络延迟。
- 未将 miniredis 结果当作真实 Redis 结果。
- 当前没有生产容量、可用性或高并发稳定性结论。
- Redis 故障恢复、至少一次执行和重复执行语义已有功能测试，但仍需业务 handler 自行保证幂等性。
