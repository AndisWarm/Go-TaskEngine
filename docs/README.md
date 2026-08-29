# Go-TaskEngine 文档

本目录记录 Go-TaskEngine 的架构、Redis 状态机、运行方式和验证结果。文档中的性能结论只来自已执行的测试，并明确区分 miniredis、本机真实 Redis 和尚未进行的生产级压测。

## 文档导航

- [`architecture.md`](architecture.md)：组件职责、任务数据流、运行语义、关键配置和验证分层。
- [`redis-state-machine.md`](redis-state-machine.md)：Redis key 约定和 Lua 状态转换。
- [`runbook.md`](runbook.md)：Redis 启动、模拟示例、真实 Redis 验收、生命周期和故障语义。
- [`benchmark-report.md`](benchmark-report.md)：真实 Redis benchmark、功能验收结果、测试条件和未测量项。
- [`../README.md`](../README.md)：项目概览、当前交付状态和最小示例命令。
- [`../TASK_ENGINE_PLAN.md`](../TASK_ENGINE_PLAN.md)：唯一阶段进度记录和实施日志。

## 当前状态

阶段 0–9 的核心功能、模拟示例、真实 Redis 测试、benchmark、故障验收、PowerShell 启停脚本、CI 配置和文档已完成并推送到 GitHub 远程 `main`。

项目不包含网页、HTTP 管理后台、云部署或生产容量承诺。任务执行采用至少一次语义，业务 handler 需要自行保证幂等性。

## 发布状态

阶段 9 的代码、测试、脚本、CI 配置和文档已推送到 GitHub 远程 `main`。GitHub Actions 工作流位于 `.github/workflows/ci.yml`，会执行测试、race、构建和 vet 检查。