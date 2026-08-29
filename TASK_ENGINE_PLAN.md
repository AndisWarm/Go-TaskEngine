# Go-TaskEngine 完整落地与 GitHub 分批发布计划

> **执行约束：** 后续每个阶段只处理本计划列出的范围。阶段必须先写失败测试，再写最小实现，再运行指定测试/构建；验证失败不得进入下一阶段。每次实际修改都要在本文档底部追加实施日志。

**目标：** 将当前项目整理为可由外部 Go 项目使用、可测试、可维护的 Redis 任务执行引擎，并按阶段提交推送到 `https://github.com/AndisWarm/Go-TaskEngine.git`。

**架构：** `client/` 负责创建任务和入队，`server/` 负责调度和固定 worker，Redis 存放任务持久状态及原子状态转换，`examples/` 只提供可运行的模拟业务示例。需要公开给外部 Go 项目的模型、存储接口和 Redis 实现放在可导入的公开包中；定时器等内部机制保留在 `internal/`，各模块通过接口隔离。

**技术栈：** Go 1.24.0、Redis、`github.com/redis/go-redis/v9`、`github.com/alicebob/miniredis/v2`、Go 标准测试和 race 检测。

## 全局约束

- 项目范围只包含 Go 任务执行引擎、客户端 SDK、Redis 服务端、命令行示例和测试，不创建网页或 HTTP 管理端。
- `asynq-master` 和 `asynq-master.zip` 只作为参考资料，加入 `.gitignore`，不修改、不提交、不把它们的代码或测试当作本项目完成证据。
- 本文件是唯一进度记录；所有阶段初始为 `PLANNED`，只有实际改代码且通过至少一次 `go test` 或 `go build` 后才能改成 `IN_PROGRESS`、`PARTIAL` 或 `DONE`。
- 未完成整个链路时写 `PARTIAL`；没有真实并发、故障切换或压测证据，不写“高并发稳定”“优雅退出成功”或“生产可用”。
- 安全方案不在本计划中扩展；只保留运行所需的错误、配置和数据一致性处理。
- 每个阶段完成后单独提交，提交信息包含阶段编号；提交前运行该阶段验证命令，提交后立即推送。
- GitHub 已登录；推送前先验证远程地址和认证。代理先使用本机现有 Git 配置；若需显式代理，使用 `127.0.0.1:17897`，连接失败就停止推送并报告实际错误。

## 当前已核实状态

以下结论来自当前工作区已查看的代码和文档，不把参考项目当作本项目证据：

- 根目录已有独立 `go.mod`，模块名为 `go-taskengine`，代码位于 `client/`、`server/`、`internal/`、`examples/` 和 `docs/`。
- `client/` 已有 `NewTask`、`NewClient`、`Enqueue`、`EnqueueAt`、`EnqueueIn`，并支持队列、优先级、最大重试、超时、截止时间和延时选项。
- `server/` 已有固定 worker、Redis claim、成功确认、失败重试、归档、lease、heartbeat、recovery、shutdown 和基础 metrics。
- `internal/redisstore/` 已有任务 Hash、pending List、priority ZSet、scheduled/retry ZSet、active List、lease ZSet、archived ZSet 和对应 Lua 状态转换。
- `internal/limiter/` 已有 Redis Lua Token Bucket，并接入 server 的领取路径；`internal/timer/` 已有本地时间优先队列原语，但尚未接入 server 主调度循环。
- 当前没有前端、HTTP、REST、gRPC 或独立网络管理服务；示例是命令行程序。
- 当前缺口包括：公开 API 依赖 `internal` 包；没有按任务类型路由；死信缺少列表、分页和手动重放；成功任务删除而不是保留完成记录；server 多处忽略 Redis 操作错误；示例 Redis 地址硬编码；真实 Redis 多实例压测和独立进程故障测试不足。

## 阶段顺序与交付物

### 阶段 0：Git 基线和可重复构建

新增根目录 `.gitignore`、`README.md`，更新本文档；初始化 Git，排除参考项目、构建产物、测试输出、IDE 文件和本地环境文件；说明目录职责、Redis 前置条件、测试命令和模拟示例边界；确认远程为指定 GitHub 地址和默认分支。

验证：`go test ./...`、`go build ./...`、`go vet ./...`、`git check-ignore asynq-master asynq-master.zip`。验证后提交 `chore(phase-0): establish repository baseline` 并推送。

### 阶段 1：公开包边界和客户端 SDK

新增公开 `model/`、`storage/`、`redisstore/`；修改 `client/`、`server/` 和测试。让外部 Go 项目能够导入任务模型、存储接口和 Redis Store；让 server 依赖公开任务存储接口而不是 `internal` 类型；补齐 `TaskMessage.Validate`、上下文取消、重复 ID、无效选项、`EnqueueAt` 和 `Deadline` 测试；确认 `StateCompleted` 的实际持久化策略。

验证：`go test ./client ./model ./storage ./redisstore ./... -race`、`go build ./...`，并使用项目目录外的临时 Go 模块完成公开包编译检查。通过后提交 `feat(phase-1): expose stable client and storage APIs` 并推送。

### 阶段 2：Redis 状态机和数据一致性

统一任务 Hash、pending、priority、scheduled、retry、active、lease、archived/dead-letter 的 key 约定；核对 enqueue、claim、move-ready、ack、retry、archive、requeue、lease recovery 的 Lua 脚本返回值；把状态不匹配和脚本未实际操作转换成明确错误；补充重复 ID、并发 claim、Redis 错误、完整字段、同毫秒排序和 list/zset/hash 一致性测试。

验证：miniredis 单元测试、单独标记的真实 Redis 集成测试、`go test ./redisstore -race`、`go test ./...`、`go build ./...`。通过后提交 `feat(phase-2): harden atomic Redis task state transitions` 并推送。

### 阶段 3：服务端路由、错误传播和固定 worker

整理 `server/server.go` 的调度、worker、处理结果和生命周期职责；增加按 `TaskMessage.Type` 注册和路由 Handler 的公开接口；未知类型进入明确错误路径；验证 `Config.Concurrency` 是唯一 worker 上限；验证完整多队列优先级顺序；让 MoveReady、PendingCount、Claim、Ack、Retry、Archive、Requeue 和 lease 错误可观察；metrics 只在状态转换成功后计数。

验证：并发上限、完整优先级、未知类型、Redis 故障和 ack/retry/archive 失败测试；`go test ./server -race`、`go test ./...`、`go build ./...`。通过后提交 `feat(phase-3): add routed handlers and observable worker errors` 并推送。

### 阶段 4：延时调度和 Timer/Time Wheel 接入

将 scheduled/retry 到期扫描接入 dispatcher；用 Lua 批量搬运并保证重复扫描不重复入队；为 Timer/Time Wheel 增加可注入时钟、并发 Schedule/Cancel 和慢回调语义；它只负责本地唤醒，不替代 Redis 持久化；校验 `HeartbeatInterval < LeaseDuration` 等危险配置。

验证：500ms、1s、跨秒边界、重启后发现延时任务和重复 dispatcher 场景；`go test ./internal/timer ./redisstore ./server -race`、`go test ./...`、`go build ./...`。把实际误差范围写入文档；通过后提交 `feat(phase-4): integrate durable delayed dispatch scheduling` 并推送。

### 阶段 5：重试、死信管理和 Lease 恢复

实现 `baseDelay * 2^n`、jitter 和 max delay；明确不可重试错误、最大重试次数、重试计数和错误保留；提供死信查询、分页、按 ID 查询、手动重放和删除/清理策略；接入 heartbeat 和真实 recovery loop；文档明确至少一次执行语义和重复执行可能性。

验证：精确退避、重试后成功、不可重试、达到上限、死信字段、查询/重放、lease 过期和重复执行测试；至少一次独立进程故障模拟；`go test ./server ./redisstore -run 'Test.*Retry|Test.*Dead|Test.*Lease' -race`、`go test ./...`、`go build ./...`。通过后提交 `feat(phase-5): complete retry dead-letter and lease recovery` 并推送。

### 阶段 6：Redis Token Bucket 限流和多实例验证

保留 Lua 原子令牌桶；明确容量、补充速率、burst、scope 和等待时间；在真正 claim 之前扣令牌；按实际需要支持全局、队列或业务 scope；用两个独立 server 共享同一 Redis bucket 验证消费速率和竞争行为。

验证：burst、稳定速率、并发、跨秒时间边界和双实例端到端测试；分开标记 miniredis 和真实 Redis 测试；`go test ./internal/limiter ./server -race`、`go test ./...`、`go build ./...`。通过后提交 `feat(phase-6): verify distributed token bucket scheduling limits` 并推送。

### 阶段 7：Shutdown 和信号生命周期

统一 `Start`、`Stop`、`Shutdown`、`Run`、`RunSignals` 的状态机和 nil context 行为；固定关闭顺序为停止领取、停止转发、等待 handler、停止 heartbeat/recovery、关闭资源；确认正常 shutdown 的等待/取消语义并让代码、测试和文档一致；超时任务走已验证的 requeue/recovery 路径。

验证：重复 Start/Shutdown、Shutdown 后 Start、真实退出信号、handler 忽略 context 和超时回收测试；`go test ./server -run 'Test.*Shutdown|Test.*Signal' -race`、`go test ./...`、`go build ./...`。通过后提交 `feat(phase-7): finalize server lifecycle and shutdown semantics` 并推送。

### 阶段 8：可配置的模拟示例和可观测性

保留图像处理和 C2PA 签名模拟边界；把硬编码 Redis 地址改为环境变量或命令行参数；增加连通性检查、启动参数、失败/超时/不可重试示范和 Ctrl+C 说明；记录实际接入 server 的任务状态、耗时、成功、失败、重试和死信指标。

验证：Redis 可用时分别运行 producer/worker 端到端流程，检查重试、死信和退出；`go test ./...`、`go build ./...`。未跑通的示例标为 `PARTIAL`；通过后提交 `feat(phase-8): make examples configurable and observable` 并推送。

### 阶段 9：真实 Redis 压测、故障测试和文档收口

提供可重复的真实 Redis 测试入口；记录入队吞吐、延时误差分布、worker 并发上限、重试/死信吞吐、限流速率、shutdown 回收时间和故障恢复结果；写明 Redis 版本、CPU/内存、payload、并发数、持续时间和采样方法；区分 miniredis、本地真实 Redis 和多进程测试。

验证：`go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`、全部示例验证和真实 Redis 验收清单。未完成项保持 `PARTIAL`；通过后提交 `docs(phase-9): close validation and release documentation` 并推送。

## 当前阶段状态

- 阶段 0：`DONE` —— 已创建 `.gitignore`、`README.md` 和本计划；`go test ./...`、`go build ./...`、`go vet ./...` 均通过，Git 初始化、远程配置和参考项目忽略规则检查已完成；基线提交已推送到 GitHub `main` 分支。
- 阶段 1：`DONE` —— 已新增公开 `model/`、`storage/`、`redisstore/` 包边界，server/client/examples 已切换到公开类型；新增模型超时校验、客户端精确延时、负延时和取消上下文测试；目录外临时 Go 模块导入公开 API 编译通过。
- 阶段 2：`DONE` —— 成功确认保留 `completed` 任务记录和完成时间；无效状态转换返回明确错误；scheduled/retry 到期搬运使用 Lua 批量脚本，并通过并发搬运只成功一次测试。
- 阶段 3：`PLANNED`
- 阶段 4：`PLANNED`
- 阶段 5：`PLANNED`
- 阶段 6：`PLANNED`
- 阶段 7：`PLANNED`
- 阶段 8：`PLANNED`
- 阶段 9：`PLANNED`

## 每阶段固定执行模板

1. 阅读本阶段文件，确认前一阶段提交存在。
2. 写本阶段失败测试并运行，确认失败原因正确。
3. 写最小实现，只处理本阶段范围。
4. 运行阶段测试、`go test ./...` 和 `go build ./...`；需要时增加 `-race`、`go vet` 和真实 Redis 测试。
5. 只根据实际代码和命令输出更新本文档状态与实施日志。
6. 用 `git diff`、`git status` 和 `git check-ignore` 检查没有包含参考项目。
7. 只添加本阶段文件并创建一条阶段提交。
8. 用已认证 GitHub 远程和本机代理推送，记录提交哈希和结果。
9. 下一阶段开始前确认工作区干净、远程提交可见、上一阶段验证记录完整。

## 预计提交批次

- `phase-0`：Git 忽略规则、README、基线文档。
- `phase-1`：公开模型、存储边界和客户端 SDK。
- `phase-2`：Redis 原子状态机和一致性。
- `phase-3`：处理器路由、worker 和错误传播。
- `phase-4`：延时调度和 Timer/Time Wheel 接入。
- `phase-5`：重试、死信和 Lease 恢复。
- `phase-6`：Token Bucket 和双实例验证。
- `phase-7`：Shutdown 与信号生命周期。
- `phase-8`：模拟示例配置和 metrics。
- `phase-9`：真实 Redis 压测、故障验收和文档收口。

## 实施日志

- 2026-08-29 16:01（UTC+8）：用户确认执行分阶段落地方案；本次执行开始。当前只读核对确认项目已有 Go 核心代码，但公开包边界、死信管理、Time Wheel 接入、真实 Redis 多实例测试和示例配置仍有缺口。
- 2026-08-29 16:02（UTC+8）：阶段 0 创建根目录 `.gitignore`、`README.md` 并重写本文件；首次组合命令中的 `go test ./...`、`go build ./...`、`go vet ./...` 均通过，但因当时尚未初始化 Git，最后的 `git check-ignore` 返回“不是 Git 仓库”。
- 2026-08-29 16:05（UTC+8）：初始化 Git 的 `main` 分支并配置远程 `https://github.com/AndisWarm/Go-TaskEngine.git`；`git check-ignore -v asynq-master asynq-master.zip` 均命中根目录 `.gitignore`。阶段 0 当前代码验证通过，状态为 `DONE`；提交和推送尚未完成。
- 2026-08-29 16:08（UTC+8）：创建阶段提交 `2055b97 chore(phase-0): establish repository baseline`，暂存区未包含 `asynq-master` 或 `asynq-master.zip`；通过 `git -c http.proxy=http://127.0.0.1:17897 push -u origin main` 成功推送到 GitHub `main` 分支。
- 2026-08-29 16:27（UTC+8）：阶段 1 完成公开包边界改造：新增 `model/model.go`、`storage/storage.go`、`redisstore/redisstore.go`；`internal/model` 改为公开模型兼容别名；client、server 和示例使用公开包，server.New 接收 `storage.TaskStore`。新增模型负超时校验、客户端 `EnqueueAt`、负 `EnqueueIn` 和取消上下文测试；目录外临时模块导入公开 API 编译通过。验证命令 `go test -race ./...`、`go build ./...`、`go vet ./...` 均通过。阶段 1 状态为 `DONE`，提交和推送尚未完成。
- 2026-08-29 16:29（UTC+8）：阶段 1 提交已修正并推送：`40024c7 feat(phase-1): expose stable client and storage APIs`；工作区未包含未提交文件。
- 2026-08-29 16:41（UTC+8）：阶段 2 完成 Redis 状态机增强：成功确认保留任务 Hash、写入 `completed` 状态和 `completed_at`；Ack/Retry/Archive/Requeue 的 Lua 返回 0 时返回 `ErrInvalidTransition`；到期 scheduled/retry 任务改用 Lua 批量搬运；新增完成记录、无效转换和并发搬运测试，并更新状态机文档。验证命令 `go test -race ./...`、`go build ./...`、`go vet ./...` 均通过。阶段 2 状态为 `DONE`，提交和推送尚未完成。
