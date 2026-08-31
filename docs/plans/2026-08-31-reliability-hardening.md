# Go-TaskEngine 契约与可靠性硬化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复存储错误语义、限流具体类型耦合、无界存储上下文、非原子任务读取和非原子 lease 续期五项问题。

**Architecture:** `storage` 拥有可区分且向后兼容的错误契约，`limiter` 暴露服务端需要的窄接口，`server` 的所有持久化调用使用 lifecycle context。Redis 实现通过 `HMGET` 原子读取任务，并通过单槽 Lua 脚本按当前 active 状态续租或清理 lease。

**Tech Stack:** Go 1.26.5、Redis Lua、`github.com/redis/go-redis/v9`、`github.com/alicebob/miniredis/v2`、Go test、race detector、go vet。

## Global Constraints

- 保持 `TaskStore.Claim` 方法签名。
- 保持 `Config.TokenBucket` 字段名。
- 保持 `limiter.NewTokenBucket`、`NewScopedTokenBucket` 和 `TokenBucket`。
- 保持 `redisstore.ErrNoTask` 和 `storage.ErrNoTask` 的 `errors.Is` 兼容行为。
- 不新增 `storage/v2`、`limiter/v2` 或适配器目录。
- `ShutdownTimeout` 是整个关闭过程的严格上限；超时任务依赖 lease recovery，不同步重排。
- 不修改 `My_learn/`、`docs/study_notes/`、`.gitignore` 和上一轮仅有行尾差异的文件。
- 每项生产代码修改前先运行对应失败测试。

---

### Task 1: Split empty-queue and task-not-found errors

**Files:**
- Modify: `storage/storage.go`
- Modify: `storage/storage_test.go`
- Modify: `internal/redisstore/rdb.go`
- Modify: `redisstore/redisstore.go`
- Modify: `redisstore/public_api_test.go`
- Modify: `server/server.go`
- Modify: `server/storage_contract_test.go`

**Interfaces:**
- Produces: `storage.ErrQueueEmpty error`
- Produces: `storage.ErrTaskNotFound error`
- Produces: `storage.IsQueueEmpty(error) bool`
- Preserves: `storage.ErrNoTask error`
- Preserves: `TaskStore.Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error)`

- [ ] **Step 1: Write failing storage error tests**

Add imports `errors` and `fmt` to `storage/storage_test.go`, then add:

```go
func TestTaskAbsenceErrorsAreDistinctAndBackwardCompatible(t *testing.T) {
	if ErrQueueEmpty == ErrTaskNotFound {
		t.Fatal("queue-empty and task-not-found errors share one identity")
	}
	if !errors.Is(ErrQueueEmpty, ErrNoTask) {
		t.Fatal("ErrQueueEmpty is not compatible with ErrNoTask")
	}
	if !errors.Is(ErrTaskNotFound, ErrNoTask) {
		t.Fatal("ErrTaskNotFound is not compatible with ErrNoTask")
	}
	if !IsQueueEmpty(ErrQueueEmpty) {
		t.Fatal("ErrQueueEmpty was not classified as an empty queue")
	}
	if !IsQueueEmpty(fmt.Errorf("legacy claim: %w", ErrNoTask)) {
		t.Fatal("wrapped legacy ErrNoTask was not classified as an empty queue")
	}
	if IsQueueEmpty(ErrTaskNotFound) {
		t.Fatal("ErrTaskNotFound was classified as an empty queue")
	}
}
```

- [ ] **Step 2: Run the storage test and verify RED**

Run:

```powershell
go test ./storage -run '^TestTaskAbsenceErrorsAreDistinctAndBackwardCompatible$' -count=1
```

Expected: compilation fails because `ErrQueueEmpty`, `ErrTaskNotFound`, and `IsQueueEmpty` do not exist.

- [ ] **Step 3: Implement the compatible storage errors**

In `storage/storage.go`, extend the error block and add the classifier:

```go
var (
	// ErrNoTask is the compatibility root for operations that cannot return a task.
	ErrNoTask = errors.New("no processable task")
	// ErrQueueEmpty means Claim has no task available in the requested queue.
	ErrQueueEmpty = fmt.Errorf("task queue is empty: %w", ErrNoTask)
	// ErrTaskNotFound means a task lookup found no task for the requested queue and ID.
	ErrTaskNotFound = fmt.Errorf("task not found: %w", ErrNoTask)
	// ErrTaskExists means enqueue or schedule found an existing task ID.
	ErrTaskExists = errors.New("task already exists")
	// ErrInvalidTransition means the requested durable state transition is not valid.
	ErrInvalidTransition = errors.New("invalid task state transition")
)

// IsQueueEmpty reports whether err means Claim found no available task.
// It accepts the legacy ErrNoTask unless the error is the new ErrTaskNotFound.
func IsQueueEmpty(err error) bool {
	if err == nil || errors.Is(err, ErrTaskNotFound) {
		return false
	}
	return errors.Is(err, ErrQueueEmpty) || errors.Is(err, ErrNoTask)
}
```

Add `fmt` to the imports. Update the `Claim` comment to mention `ErrQueueEmpty` and the `Get` comment to mention `ErrTaskNotFound`.

- [ ] **Step 4: Run storage tests and verify GREEN**

Run:

```powershell
gofmt -w storage/storage.go storage/storage_test.go
go test ./storage -count=1
```

Expected: all `storage` tests pass.

- [ ] **Step 5: Write failing Redis and server classification tests**

Add to `redisstore/public_api_test.go`:

```go
func TestPublicRedisAbsenceErrorsAliasStorageContract(t *testing.T) {
	if redisstore.ErrQueueEmpty != storage.ErrQueueEmpty {
		t.Fatal("redisstore.ErrQueueEmpty does not alias storage.ErrQueueEmpty")
	}
	if redisstore.ErrTaskNotFound != storage.ErrTaskNotFound {
		t.Fatal("redisstore.ErrTaskNotFound does not alias storage.ErrTaskNotFound")
	}
}
```

Refactor `emptyContractStore` in `server/storage_contract_test.go` to include `claimErr error`, and return `claimErr` when non-nil, otherwise `storage.ErrNoTask`. Add:

```go
func TestServerReportsTaskNotFoundFromClaim(t *testing.T) {
	store := &emptyContractStore{
		claimed:  make(chan struct{}),
		claimErr: storage.ErrTaskNotFound,
	}
	errorsSeen := make(chan error, 1)
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error { return nil }), Config{
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
	defer s.Shutdown()
	select {
	case err := <-errorsSeen:
		if !errors.Is(err, storage.ErrTaskNotFound) {
			t.Fatalf("reported error = %v, want ErrTaskNotFound", err)
		}
	case <-time.After(time.Second):
		t.Fatal("task-not-found claim error was treated as an empty queue")
	}
}
```

Add `errors` to the server test imports.

- [ ] **Step 6: Run focused tests and verify RED**

Run:

```powershell
go test ./redisstore ./server -run 'TestPublicRedisAbsenceErrorsAliasStorageContract|TestServerReportsTaskNotFoundFromClaim' -count=1
```

Expected: `redisstore` fails to compile because aliases are missing; after aliases exist but before server logic changes, the server test times out because broad `ErrNoTask` matching suppresses `ErrTaskNotFound`.

- [ ] **Step 7: Wire new errors through Redis and server**

In `internal/redisstore/rdb.go`, add aliases:

```go
ErrQueueEmpty         = storage.ErrQueueEmpty
ErrTaskNotFound       = storage.ErrTaskNotFound
```

Change `Claim` empty result to:

```go
return nil, ErrQueueEmpty
```

Change `Get` missing result to:

```go
return nil, ErrTaskNotFound
```

In `redisstore/redisstore.go`, export both aliases. In `server/server.go`, change the claim condition to:

```go
if storage.IsQueueEmpty(err) {
	continue
}
```

- [ ] **Step 8: Run focused and regression tests**

Run:

```powershell
gofmt -w internal/redisstore/rdb.go redisstore/redisstore.go redisstore/public_api_test.go server/server.go server/storage_contract_test.go
go test ./storage ./internal/redisstore ./redisstore ./server -count=1
```

Expected: all four packages pass, including legacy `ErrNoTask` handling and new task-not-found reporting.

- [ ] **Step 9: Commit Task 1 only**

```powershell
git add storage/storage.go storage/storage_test.go internal/redisstore/rdb.go redisstore/redisstore.go redisstore/public_api_test.go server/server.go server/storage_contract_test.go
git commit -m "refactor: distinguish absent task outcomes"
```

---

### Task 2: Depend on a limiter interface

**Files:**
- Modify: `limiter/limiter.go`
- Modify: `server/server.go`
- Modify: `server/public_api_test.go`

**Interfaces:**
- Produces: `limiter.Limiter`
- Preserves: `limiter.TokenBucket`
- Changes: `server.Config.TokenBucket` static type from `*limiter.TokenBucket` to `limiter.Limiter`

- [ ] **Step 1: Write the failing custom limiter compile test**

Replace `server/public_api_test.go` with:

```go
package server_test

import (
	"context"
	"testing"

	"go-taskengine/limiter"
	"go-taskengine/server"
)

type customLimiter struct{}

func (customLimiter) Acquire(context.Context, float64) (limiter.Result, error) {
	return limiter.Result{Allowed: true}, nil
}
func (customLimiter) Validate() error    { return nil }
func (customLimiter) Capacity() float64 { return 1 }

func TestPublicLimiterCanConfigureServer(t *testing.T) {
	bucket := limiter.NewScopedTokenBucket(nil, "public", 1, 1)
	_ = server.Config{TokenBucket: bucket}
}

func TestCustomLimiterCanConfigureServer(t *testing.T) {
	_ = server.Config{TokenBucket: customLimiter{}}
}
```

- [ ] **Step 2: Run the public API test and verify RED**

Run:

```powershell
go test ./server -run '^TestCustomLimiterCanConfigureServer$' -count=1
```

Expected: compilation fails because `customLimiter` cannot be assigned to `*limiter.TokenBucket`.

- [ ] **Step 3: Define and consume the limiter interface**

In `limiter/limiter.go`, import `context` and add:

```go
// Limiter is the rate-limit contract required by the task server.
type Limiter interface {
	Acquire(context.Context, float64) (Result, error)
	Validate() error
	Capacity() float64
}

var _ Limiter = (*TokenBucket)(nil)
```

In `server/server.go`, change:

```go
TokenBucket limiter.Limiter
```

Keep validation and dispatch logic unchanged because they already use only those methods.

- [ ] **Step 4: Verify custom and Redis limiters**

Run:

```powershell
gofmt -w limiter/limiter.go server/server.go server/public_api_test.go
go test ./internal/limiter ./limiter ./server -count=1
```

Expected: custom limiter compiles, Redis token-bucket tests and server integration tests pass.

- [ ] **Step 5: Commit Task 2 only**

```powershell
git add limiter/limiter.go server/server.go server/public_api_test.go
git commit -m "refactor: depend on limiter contract"
```

---

### Task 3: Bound server storage calls by lifecycle context

**Files:**
- Modify: `server/dependency_test.go`
- Create: `server/lifecycle_context_test.go`
- Modify: `server/shutdown_test.go`
- Modify: `server/server.go`

**Interfaces:**
- Consumes: existing `Server.ctx` lifecycle context
- Preserves: `Shutdown() error`, `Stop()`
- Changes: timeout recovery from immediate requeue to lease expiration

- [ ] **Step 1: Add a failing source-boundary test**

Append to `server/dependency_test.go`:

```go
func TestServerStorageCallsDoNotUseBackgroundContext(t *testing.T) {
	source, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	methods := []string{
		"MoveReady", "PendingCount", "Claim", "Requeue", "AckSuccess",
		"ScheduleRetry", "Archive", "ExtendLease", "ExpiredIDs", "Get",
	}
	for _, method := range methods {
		forbidden := "s.store." + method + "(context.Background()"
		if strings.Contains(text, forbidden) {
			t.Errorf("server storage call %s uses context.Background", method)
		}
	}
}
```

The file already imports `os` and `strings`.

- [ ] **Step 2: Add a failing lifecycle cancellation behavior test**

Create `server/lifecycle_context_test.go`:

```go
package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-taskengine/model"
	"go-taskengine/storage"
)

type cancelAwareAckStore struct {
	storage.TaskStore
	started  chan struct{}
	canceled chan struct{}
}

func (s *cancelAwareAckStore) AckSuccess(ctx context.Context, _ *model.TaskMessage) error {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return ctx.Err()
}

func TestProcessStorageTransitionUsesLifecycleContext(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	store := &cancelAwareAckStore{started: make(chan struct{}), canceled: make(chan struct{})}
	s := &Server{
		store:      store,
		handler:    HandlerFunc(func(context.Context, *TaskMessage) error { return nil }),
		cfg:        Config{},
		ctx:        lifecycle,
		handlerCtx: context.Background(),
	}
	done := make(chan struct{})
	go func() {
		s.process(&model.TaskMessage{ID: "context-1", Queue: "default"})
		close(done)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("AckSuccess did not start")
	}
	cancel()
	select {
	case <-store.canceled:
	case <-time.After(time.Second):
		t.Fatal("AckSuccess did not receive lifecycle cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not return after lifecycle cancellation")
	}
	if !errors.Is(lifecycle.Err(), context.Canceled) {
		t.Fatalf("lifecycle error = %v", lifecycle.Err())
	}
}
```

- [ ] **Step 3: Change the shutdown timeout test to the safe lease behavior**

Rename `TestShutdownTimeoutRequeuesRunningTask` in `server/shutdown_test.go` to `TestShutdownTimeoutLeavesRunningTaskActiveForLeaseRecovery`. Replace the pending assertion with:

```go
if got := producerStorePending(t, s, "default"); got != 0 {
	t.Fatalf("pending after timeout = %d, want 0", got)
}
msg, err := s.store.Get(context.Background(), "default", "slow-1")
if err != nil {
	t.Fatal(err)
}
if msg.State != model.StateActive {
	t.Fatalf("state after timeout = %s, want active", msg.State)
}
```

Add `go-taskengine/model` to imports. Keep `close(release)` after assertions.

- [ ] **Step 4: Run the lifecycle tests and verify RED**

Run:

```powershell
go test ./server -run 'TestServerStorageCallsDoNotUseBackgroundContext|TestProcessStorageTransitionUsesLifecycleContext|TestShutdownTimeoutLeavesRunningTaskActiveForLeaseRecovery' -count=1
```

Expected failures:

- source-boundary test lists current background-context storage calls;
- process test times out because `AckSuccess` receives a background context;
- shutdown test sees pending state because `requeueActive` runs after timeout.

- [ ] **Step 5: Route all storage calls through lifecycle context**

In `server/server.go`, replace the first context argument of these calls with `s.ctx`:

```go
s.store.Requeue(s.ctx, msg)
s.store.AckSuccess(s.ctx, msg)
s.store.Archive(s.ctx, msg, reason)
s.store.ScheduleRetry(s.ctx, msg, at, reason)
s.store.ExtendLease(s.ctx, queue, ids, now, s.cfg.LeaseDuration)
s.store.ExpiredIDs(s.ctx, now, queue, 100)
s.store.Get(s.ctx, queue, id)
s.store.Archive(s.ctx, msg, "task lease expired")
s.store.ScheduleRetry(s.ctx, msg, retryAt, "task lease expired")
```

`MoveReady`, `PendingCount`, and `Claim` already use `s.ctx`.

- [ ] **Step 6: Remove unsafe timeout requeue**

In both `Shutdown` timeout branches, cancel lifecycle context and return the timeout error without calling `requeueActive`:

```go
if !waitUntil(workDone, deadline) {
	s.cancel()
	return fmt.Errorf("shutdown timeout after %s", s.cfg.ShutdownTimeout)
}
```

```go
if !waitUntil(maintenanceDone, deadline) {
	return fmt.Errorf("shutdown timeout after %s", s.cfg.ShutdownTimeout)
}
```

Delete the now-unused `requeueActive` method. Keep dispatch's post-claim `Requeue(s.ctx, msg)` because graceful `Shutdown` leaves lifecycle context valid until worker intake stops; immediate `Stop` may cancel it and then lease recovery is the fallback.

- [ ] **Step 7: Run lifecycle tests and server regression**

Run:

```powershell
gofmt -w server/server.go server/dependency_test.go server/lifecycle_context_test.go server/shutdown_test.go
go test ./server -run 'TestServerStorageCallsDoNotUseBackgroundContext|TestProcessStorageTransitionUsesLifecycleContext|TestShutdownTimeoutLeavesRunningTaskActiveForLeaseRecovery|Test.*Shutdown|Test.*Signal' -count=1
go test ./server -count=1
```

Expected: all tests pass; the timed-out task remains active and no storage method receives an unbounded background context.

- [ ] **Step 8: Commit Task 3 only**

```powershell
git add server/server.go server/dependency_test.go server/lifecycle_context_test.go server/shutdown_test.go
git commit -m "fix: bound storage calls to server lifecycle"
```

---

### Task 4: Read task message and state atomically

**Files:**
- Modify: `internal/redisstore/rdb_test.go`
- Modify: `internal/redisstore/rdb.go`
- Modify: `internal/redisstore/deadletter_test.go`

**Interfaces:**
- Consumes: `storage.ErrTaskNotFound`
- Preserves: `Get(context.Context, string, string) (*model.TaskMessage, error)`
- Produces: one-command `HMGET msg state` snapshot

- [ ] **Step 1: Write failing missing-state and not-found tests**

Add to `internal/redisstore/rdb_test.go`:

```go
func TestGetReturnsTaskNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	_, err := store.Get(context.Background(), "default", "missing")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Get error = %v, want ErrTaskNotFound", err)
	}
}

func TestGetRejectsTaskRecordWithoutState(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	msg := message("missing-state", 1, time.UnixMilli(1000))
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HDel(ctx, TaskKey("default", msg.ID), "state").Err(); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "default", msg.ID)
	if err == nil {
		t.Fatalf("Get returned task with missing state: %+v", got)
	}
}
```

Add standard-library `errors` if not already imported.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
go test ./internal/redisstore -run 'TestGetReturnsTaskNotFound|TestGetRejectsTaskRecordWithoutState' -count=1
```

Expected: task-not-found test passes after Task 1; missing-state test fails because current `Get` returns a message while ignoring `redis.Nil` for state.

- [ ] **Step 3: Replace two HGET calls with one HMGET**

Replace `Store.Get` in `internal/redisstore/rdb.go` with:

```go
func (s *Store) Get(ctx context.Context, queue, id string) (*model.TaskMessage, error) {
	values, err := s.client.HMGet(ctx, TaskKey(queue, id), "msg", "state").Result()
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}
	if len(values) != 2 {
		return nil, fmt.Errorf("get task: unexpected field count %d", len(values))
	}
	if values[0] == nil {
		return nil, ErrTaskNotFound
	}
	if values[1] == nil {
		return nil, errors.New("get task: state is missing")
	}
	var msg model.TaskMessage
	if err := json.Unmarshal([]byte(fmt.Sprint(values[0])), &msg); err != nil {
		return nil, fmt.Errorf("decode task: %w", err)
	}
	msg.State = model.TaskState(fmt.Sprint(values[1]))
	return &msg, nil
}
```

- [ ] **Step 4: Update dead-letter not-found expectation**

In `internal/redisstore/deadletter_test.go`, change the deleted lookup assertion to require `ErrTaskNotFound`; also retain an assertion that `errors.Is(err, ErrNoTask)` is true for compatibility.

- [ ] **Step 5: Run Redis store tests**

Run:

```powershell
gofmt -w internal/redisstore/rdb.go internal/redisstore/rdb_test.go internal/redisstore/deadletter_test.go
go test ./internal/redisstore ./redisstore -count=1
```

Expected: all tests pass; missing state returns an error and missing tasks remain compatible with `ErrNoTask` through `errors.Is`.

- [ ] **Step 6: Commit Task 4 only**

```powershell
git add internal/redisstore/rdb.go internal/redisstore/rdb_test.go internal/redisstore/deadletter_test.go
git commit -m "fix: read task state atomically"
```

---

### Task 5: Extend leases only for active tasks

**Files:**
- Modify: `internal/redisstore/state_transition_test.go`
- Modify: `internal/redisstore/rdb.go`

**Interfaces:**
- Preserves: `ExtendLease(context.Context, string, []string, time.Time, time.Duration) error`
- Produces: active-state-checked Lua lease update

- [ ] **Step 1: Write failing stale-heartbeat test**

Add to `internal/redisstore/state_transition_test.go`:

```go
func TestExtendLeaseDoesNotRecreateLeaseAfterCompletion(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := message("stale-heartbeat", 1, now)
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckSuccess(ctx, active); err != nil {
		t.Fatal(err)
	}
	if err := store.ExtendLease(ctx, "default", []string{active.ID}, now.Add(time.Second), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.ZScore(ctx, LeaseKey("default"), active.ID).Result(); err != redis.Nil {
		t.Fatalf("completed task lease error = %v, want redis.Nil", err)
	}
}
```

Add `github.com/redis/go-redis/v9` to imports.

- [ ] **Step 2: Run the lease test and verify RED**

Run:

```powershell
go test ./internal/redisstore -run '^TestExtendLeaseDoesNotRecreateLeaseAfterCompletion$' -count=1
```

Expected: test fails because the current pipeline unconditionally recreates the completed task's lease.

- [ ] **Step 3: Add the state-aware Lua script**

In `internal/redisstore/rdb.go`, add:

```go
var extendLeaseScript = redis.NewScript(`
local expiresAt = ARGV[1]
local extended = 0
for i = 2, #KEYS do
  local id = ARGV[i]
  if redis.call("HGET", KEYS[i], "state") == "active" then
    redis.call("ZADD", KEYS[1], expiresAt, id)
    extended = extended + 1
  else
    redis.call("ZREM", KEYS[1], id)
  end
end
return extended
`)
```

Replace `ExtendLease` with:

```go
func (s *Store) ExtendLease(ctx context.Context, queue string, ids []string, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		lease = s.leaseDuration
	}
	if len(ids) == 0 {
		return nil
	}
	keys := make([]string, 1, len(ids)+1)
	keys[0] = LeaseKey(queue)
	args := make([]interface{}, 1, len(ids)+1)
	args[0] = now.Add(lease).UnixMilli()
	for _, id := range ids {
		keys = append(keys, TaskKey(queue, id))
		args = append(args, id)
	}
	if _, err := extendLeaseScript.Run(ctx, s.client, keys, args...).Int(); err != nil {
		return fmt.Errorf("extend task leases: %w", err)
	}
	return nil
}
```

All keys use the same queue hash tag and therefore one Redis Cluster slot.

- [ ] **Step 4: Verify active extension and stale cleanup**

Run:

```powershell
gofmt -w internal/redisstore/rdb.go internal/redisstore/state_transition_test.go
go test ./internal/redisstore -run 'TestExtendLeaseDoesNotRecreateLeaseAfterCompletion|Test.*Lease|TestAckSuccessRetainsCompletedTaskRecord' -count=1
go test ./internal/redisstore -count=1
```

Expected: completed task lease remains absent, existing lease and state-machine tests pass.

- [ ] **Step 5: Commit Task 5 only**

```powershell
git add internal/redisstore/rdb.go internal/redisstore/state_transition_test.go
git commit -m "fix: extend leases only for active tasks"
```

---

### Task 6: Align architecture documentation and run full verification

**Files:**
- Modify: `docs/architecture.md`
- Verify: `storage/`
- Verify: `limiter/`
- Verify: `server/`
- Verify: `internal/redisstore/`
- Verify: `redisstore/`

**Interfaces:**
- Documents: strict shutdown deadline, lifecycle context, absence errors, atomic read, and active-only lease extension
- Verifies: `server → storage、limiter` contract dependencies

- [ ] **Step 1: Update architecture documentation**

In `docs/architecture.md`:

- change the shutdown description so a timed-out active task remains active, heartbeat stops, and lease recovery handles it;
- document `ErrQueueEmpty` versus `ErrTaskNotFound` and `ErrNoTask` compatibility;
- document that server accepts `limiter.Limiter`, with Redis `TokenBucket` as one implementation;
- document `HMGET` task snapshots and Lua active-state lease checks;
- ensure the existing and any new Mermaid blocks start with the required Microsoft YaHei initialization, use vertical `flowchart TB`, and quote all Chinese labels with full-width punctuation;

- [ ] **Step 2: Check contract ownership and forbidden dependencies**

Run:

```powershell
rg -n 'ErrQueueEmpty|ErrTaskNotFound|IsQueueEmpty' storage internal/redisstore redisstore server
rg -n 'TokenBucket\s+\*limiter\.TokenBucket' server --glob '*.go'
rg -n 's\.store\.[A-Za-z]+\(context\.Background\(\)' server/server.go
```

Expected:

- errors originate in `storage` and are referenced by implementations and consumers;
- no concrete token-bucket field remains in server;
- no server storage call uses `context.Background()`.

- [ ] **Step 3: Run focused tests**

```powershell
go test ./storage ./limiter ./internal/redisstore ./redisstore ./server -count=1
```

Expected: all focused packages pass.

- [ ] **Step 4: Run full tests and race detector**

```powershell
go test ./... -count=1
go test -race ./... -count=1
```

Expected: all packages pass with no race report.

- [ ] **Step 5: Run build, vet, formatting, and diff checks**

```powershell
gofmt -w storage/*.go limiter/*.go server/*.go internal/redisstore/*.go redisstore/*.go
go build ./...
go vet ./...
git diff --check
```

Expected: all commands exit successfully with no diagnostics.

- [ ] **Step 6: Protect pre-existing user changes**

Run:

```powershell
git status --short
git diff --ignore-space-at-eol -- .gitignore docs/design/2026-08-31-storage-contract-errors.md docs/plans/2026-08-31-storage-contract-errors.md server/dependency_test.go server/storage_contract_test.go
```

Expected: `.gitignore` content remains untouched. Any planned edits to the two server tests are visible as intentional content changes; the old design and plan files retain no content changes beyond their pre-existing line-ending status.

- [ ] **Step 7: Commit documentation only**

```powershell
git add docs/architecture.md
git commit -m "docs: document hardened task lifecycle"
```

- [ ] **Step 8: Report exact outcome**

The final response must state:

- which of the five findings were fixed;
- the intentional shutdown behavior change;
- compatibility limits for `ErrNoTask` and `Config.TokenBucket`;
- exact verification commands and outcomes;
- any pre-existing modified files left untouched.
