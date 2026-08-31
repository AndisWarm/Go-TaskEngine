# 存储错误契约归位实施计划

> **执行要求：** 按任务顺序逐项执行；每个生产代码修改前必须先运行对应失败测试，确认失败原因与缺失契约或依赖泄漏一致。

**目标：** 把三个通用存储错误归入 `storage`，保持 `redisstore` 公共名称兼容，并让 `server` 的生产代码不再导入 Redis 存储实现。

**架构：** `storage` 同时拥有方法签名和跨实现错误语义；`internal/redisstore` 依赖并实现该契约；公开 `redisstore` 继续转发原名称；`server` 只依赖 `storage`。通过错误身份测试、非 Redis 假实现测试和生产包导入检查固定该边界。

**技术栈：** Go 1.26.5、标准库 `errors`／`go/parser`／`go/token`、Go test、race detector、go vet。

## 全局约束

- 保留 `redisstore.ErrNoTask`、`redisstore.ErrTaskExists`、`redisstore.ErrInvalidTransition`，不制造破坏性公共 API 变更。
- 不改变 `storage.TaskStore` 方法签名。
- 不重构限流器、关闭上下文或 Redis Lua 状态机。
- 不修改 `My_learn/`、`docs/study_notes/` 和用户已有的 `.gitignore` 改动。
- 目录依赖保持为 `server → storage → model`，Redis 实现反向依赖 `storage` 契约。

---

### 任务一：建立通用存储错误契约

**文件：**
- 修改：`storage/storage_test.go`
- 修改：`redisstore/public_api_test.go`
- 修改：`storage/storage.go`
- 修改：`internal/redisstore/rdb.go`

**接口：**
- 产生：`storage.ErrNoTask error`
- 产生：`storage.ErrTaskExists error`
- 产生：`storage.ErrInvalidTransition error`
- 保持：`redisstore` 中三个同名变量与 `storage` 对应变量具有相同错误身份

- [ ] **步骤一：为 `storage` 错误集合编写失败测试**

在 `storage/storage_test.go` 末尾增加：

```go
func TestStorageErrorsAreDefinedAndDistinct(t *testing.T) {
	contractErrors := []error{ErrNoTask, ErrTaskExists, ErrInvalidTransition}
	for i, err := range contractErrors {
		if err == nil {
			t.Fatalf("contract error %d is nil", i)
		}
		for j := i + 1; j < len(contractErrors); j++ {
			if err == contractErrors[j] {
				t.Fatalf("contract errors %d and %d have the same identity", i, j)
			}
		}
	}
}
```

- [ ] **步骤二：为 Redis 公共兼容别名编写失败测试**

在 `redisstore/public_api_test.go` 末尾增加：

```go
func TestPublicRedisErrorsAliasStorageContract(t *testing.T) {
	tests := []struct {
		name string
		got  error
		want error
	}{
		{name: "no task", got: redisstore.ErrNoTask, want: storage.ErrNoTask},
		{name: "task exists", got: redisstore.ErrTaskExists, want: storage.ErrTaskExists},
		{name: "invalid transition", got: redisstore.ErrInvalidTransition, want: storage.ErrInvalidTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("error identity differs: got %v, want %v", tt.got, tt.want)
			}
		})
	}
}
```

- [ ] **步骤三：运行测试并确认正确失败**

运行：

```powershell
go test ./storage ./redisstore
```

预期：编译失败，错误明确指出 `ErrNoTask`、`ErrTaskExists`、`ErrInvalidTransition` 尚未在 `storage` 中定义。不得因为测试语法或导入错误失败。

- [ ] **步骤四：在 `storage` 定义错误并补全方法语义注释**

把 `storage/storage.go` 的导入和接口整理为：

```go
// Package storage defines the durable task-store contract used by the engine.
package storage

import (
	"context"
	"errors"
	"time"

	"go-taskengine/model"
)

var (
	// ErrNoTask means an operation has no task to return.
	ErrNoTask = errors.New("no processable task")
	// ErrTaskExists means enqueue or schedule found an existing task ID.
	ErrTaskExists = errors.New("task already exists")
	// ErrInvalidTransition means the requested durable state transition is not valid.
	ErrInvalidTransition = errors.New("invalid task state transition")
)

// TaskStore is the storage contract required by the server worker engine.
// Implementations must make task state transitions durable and safe for concurrent callers.
type TaskStore interface {
	// Enqueue persists an immediately processable task and returns ErrTaskExists for a duplicate ID.
	Enqueue(context.Context, *model.TaskMessage) error
	// Schedule persists a delayed task and returns ErrTaskExists for a duplicate ID.
	Schedule(context.Context, *model.TaskMessage) error
	// Claim atomically activates one task or returns ErrNoTask when the queue is empty.
	Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error)
	MoveReady(context.Context, time.Time, int, ...string) (int, error)
	// AckSuccess completes an active task and returns ErrInvalidTransition for a state mismatch.
	AckSuccess(context.Context, *model.TaskMessage) error
	// ScheduleRetry moves an active task to retry and returns ErrInvalidTransition for a state mismatch.
	ScheduleRetry(context.Context, *model.TaskMessage, time.Time, string) error
	// Archive moves an active task to the archive and returns ErrInvalidTransition for a state mismatch.
	Archive(context.Context, *model.TaskMessage, string) error
	// Requeue returns an active task to pending and returns ErrInvalidTransition for a state mismatch.
	Requeue(context.Context, *model.TaskMessage) error
	// Get returns ErrNoTask when the requested task does not exist.
	Get(context.Context, string, string) (*model.TaskMessage, error)
	ExpiredIDs(context.Context, time.Time, string, int) ([]string, error)
	PendingCount(context.Context, string) (int64, error)
	RetryCount(context.Context, string) (int64, error)
	ArchivedCount(context.Context, string) (int64, error)
	ExtendLease(context.Context, string, []string, time.Time, time.Duration) error
}
```

保留现有 `DeadLetterStore` 定义，不改变其方法。

- [ ] **步骤五：让 Redis 实现引用契约错误**

在 `internal/redisstore/rdb.go` 导入 `go-taskengine/storage`，把当前三个 `errors.New` 改为：

```go
var (
	ErrNoTask            = storage.ErrNoTask
	ErrTaskExists        = storage.ErrTaskExists
	ErrInvalidTransition = storage.ErrInvalidTransition
)
```

保留标准库 `errors` 导入，因为死信分页和状态判断仍使用它。

- [ ] **步骤六：格式化并运行聚焦测试**

运行：

```powershell
gofmt -w storage/storage.go storage/storage_test.go internal/redisstore/rdb.go redisstore/public_api_test.go
go test ./storage ./internal/redisstore ./redisstore
```

预期：三个包全部通过，兼容别名具有相同错误身份。

- [ ] **步骤七：提交任务一**

```powershell
git add storage/storage.go storage/storage_test.go internal/redisstore/rdb.go redisstore/public_api_test.go
git commit -m "refactor: move storage errors into contract"
```

---

### 任务二：移除服务端对 Redis 实现的依赖

**文件：**
- 新建：`server/dependency_test.go`
- 新建：`server/storage_contract_test.go`
- 修改：`server/server.go`
- 修改：`model/model.go`

**接口：**
- 消费：`storage.ErrNoTask`
- 保持：`server.New(storage.TaskStore, Handler, Config) *Server`
- 产生：生产包 `go-taskengine/server` 的直接导入集合不包含 `go-taskengine/redisstore`

- [ ] **步骤一：编写生产包依赖边界失败测试**

创建 `server/dependency_test.go`：

```go
package server_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestServerProductionCodeDoesNotImportRedisStore(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", name, err)
			}
			if path == "go-taskengine/redisstore" {
				t.Fatalf("production file %s imports Redis implementation", name)
			}
		}
	}
}
```

- [ ] **步骤二：编写非 Redis 存储空队列行为测试**

创建 `server/storage_contract_test.go`：

```go
package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-taskengine/model"
	"go-taskengine/storage"
)

type emptyContractStore struct {
	storage.TaskStore
	claimed chan struct{}
	once    sync.Once
}

var _ storage.TaskStore = (*emptyContractStore)(nil)

func (s *emptyContractStore) MoveReady(context.Context, time.Time, int, ...string) (int, error) {
	return 0, nil
}

func (s *emptyContractStore) PendingCount(context.Context, string) (int64, error) {
	return 1, nil
}

func (s *emptyContractStore) Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error) {
	s.once.Do(func() { close(s.claimed) })
	return nil, storage.ErrNoTask
}

func TestServerTreatsStorageErrNoTaskAsEmptyQueue(t *testing.T) {
	store := &emptyContractStore{claimed: make(chan struct{})}
	errorsSeen := make(chan error, 1)
	var handled atomic.Int32
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error {
		handled.Add(1)
		return nil
	}), Config{
		PollInterval:      time.Millisecond,
		LeaseDuration:     2 * time.Hour,
		HeartbeatInterval: time.Hour,
		RecoveryInterval:  time.Hour,
		ErrorHandler: func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		},
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.claimed:
	case <-time.After(time.Second):
		t.Fatal("server did not call Claim")
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errorsSeen:
		t.Fatalf("empty queue was reported as an error: %v", err)
	default:
	}
	if got := handled.Load(); got != 0 {
		t.Fatalf("handled %d tasks from an empty store", got)
	}
}
```

- [ ] **步骤三：运行服务端测试并确认依赖测试失败**

运行：

```powershell
go test ./server -run 'TestServerProductionCodeDoesNotImportRedisStore|TestServerTreatsStorageErrNoTaskAsEmptyQueue' -count=1
```

预期：非 Redis 存储行为测试通过；依赖边界测试失败并指出 `server.go` 导入了 Redis 实现。这个组合结果证明行为兼容已建立，但生产包边界仍未修复。

- [ ] **步骤四：把服务端判断切换到 `storage`**

在 `server/server.go`：

1. 删除导入 `go-taskengine/redisstore`。
2. 把空队列判断改为：

```go
if errors.Is(err, storage.ErrNoTask) {
	continue
}
```

3. 把 `Server` 注释改为：

```go
// Server runs a fixed-size worker pool over a durable task store.
```

- [ ] **步骤五：移除公共模型注释中的 Redis 实现限定**

把 `model/model.go` 的三处注释改为：

```go
// Package model contains task data shared by clients, servers, and storage implementations.
```

```go
// TaskState is the durable state of a task.
```

```go
// TaskMessage is the serialized task envelope persisted by a task store.
```

- [ ] **步骤六：格式化并验证服务端边界**

运行：

```powershell
gofmt -w server/server.go server/dependency_test.go server/storage_contract_test.go model/model.go
go test ./server ./model -count=1
go list -f '{{range .Imports}}{{println .}}{{end}}' go-taskengine/server | Select-String -SimpleMatch 'go-taskengine/redisstore'
```

预期：测试全部通过；最后一个命令没有匹配输出，表示服务端生产包不再直接导入 Redis 存储实现。

- [ ] **步骤七：提交任务二**

```powershell
git add server/server.go server/dependency_test.go server/storage_contract_test.go model/model.go
git commit -m "refactor: decouple server from Redis errors"
```

---

### 任务三：执行完整回归与架构审计收口

**文件：**
- 验证：`storage/`
- 验证：`internal/redisstore/`
- 验证：`redisstore/`
- 验证：`server/`
- 验证：`model/`
- 检查：`docs/design/2026-08-31-storage-contract-errors.md`

**接口：**
- 验证三个 `storage` 错误是唯一契约来源。
- 验证 Redis 公共错误名称保持兼容。
- 验证 `server` 直接依赖中没有 Redis 存储实现。

- [ ] **步骤一：检查错误定义和依赖是否唯一**

运行：

```powershell
rg -n 'ErrNoTask|ErrTaskExists|ErrInvalidTransition' storage internal/redisstore redisstore server
rg -n 'go-taskengine/redisstore' server --glob '*.go' --glob '!**/*_test.go'
```

预期：三个错误值在 `storage` 创建；Redis 两层只做引用或兼容转发；第二条命令没有匹配。

- [ ] **步骤二：运行完整测试**

```powershell
go test ./... -count=1
```

预期：全部包通过。

- [ ] **步骤三：运行竞态测试**

```powershell
go test -race ./... -count=1
```

预期：全部包通过且没有竞态报告。

- [ ] **步骤四：运行构建和静态检查**

```powershell
go build ./...
go vet ./...
git diff --check
```

预期：三个命令均成功，没有构建错误、vet 诊断或空白错误。

- [ ] **步骤五：核对工作区，保护用户原有改动**

```powershell
git status --short
git diff -- .gitignore My_learn docs/study_notes
```

预期：`.gitignore`、`My_learn/` 和 `docs/study_notes/` 的既有用户内容未被本轮实现覆盖或加入实现提交。

- [ ] **步骤六：输出审计结论**

最终报告必须区分：

- 已修复：三个存储错误的契约归位、Redis 公共兼容、服务端 Redis 错误依赖、抽象包 Redis 限定注释。
- 已验证：普通测试、竞态测试、构建、vet、生产包导入边界。
- 后续问题：`ErrNoTask` 语义过载、具体令牌桶耦合、后台存储调用可能削弱关闭超时。

未经单独设计和用户批准，不修改三个后续问题。
