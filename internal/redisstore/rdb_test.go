package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/internal/model"
)

func newTestStore(t *testing.T) (*Store, *redis.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), client
}

func message(id string, priority int, runAt time.Time) *model.TaskMessage {
	return &model.TaskMessage{ID: id, Type: "test", Payload: []byte(id), Queue: "default", Priority: priority, MaxRetry: 2, RunAt: runAt, CreatedAt: runAt, State: model.StatePending}
}

func TestEnqueueClaimAndAckUsesAtomicState(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	if err := store.Enqueue(ctx, message("low", 1, now)); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, message("high", 10, now.Add(time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	got, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != "high" || got.State != model.StateActive {
		t.Fatalf("claimed %+v", got)
	}
	if n := rdb.LLen(ctx, PendingKey("default")).Val(); n != 1 {
		t.Fatalf("pending length = %d", n)
	}
	if err := store.AckSuccess(ctx, got); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Get(ctx, "default", "high")
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != model.StateCompleted {
		t.Fatalf("completed state = %s", completed.State)
	}
	if exists := rdb.Exists(ctx, TaskKey("default", "high")).Val(); exists != 1 {
		t.Fatal("ack removed completed task record")
	}
}

func TestMoveReadyUsesUnixMilliCutoff(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	runAt := time.UnixMilli(1500)
	msg := message("delayed", 1, runAt)
	msg.State = model.StateScheduled
	if err := store.Schedule(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if moved, err := store.MoveReady(ctx, time.UnixMilli(1499), 10, "default"); err != nil || moved != 0 {
		t.Fatalf("early move = %d, err=%v", moved, err)
	}
	if moved, err := store.MoveReady(ctx, time.UnixMilli(1500), 10, "default"); err != nil || moved != 1 {
		t.Fatalf("ready move = %d, err=%v", moved, err)
	}
	got, err := store.Claim(ctx, "default", time.UnixMilli(1500), time.Second)
	if err != nil || got == nil || got.ID != "delayed" {
		t.Fatalf("claim = %+v, err=%v", got, err)
	}
}

func TestRetryAndArchiveTransitions(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := message("retry", 1, now)
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	active.RetryCount = 1
	active.State = model.StateRetry
	if err := store.ScheduleRetry(ctx, active, now.Add(2*time.Second), "temporary"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.RetryCount(ctx, "default"); err != nil || got != 1 {
		t.Fatalf("retry count = %d, err=%v", got, err)
	}
	retry, err := store.Claim(ctx, "default", now.Add(2*time.Second), time.Second)
	if err == nil || retry != nil {
		t.Fatalf("retry was claimable before forwarding: %+v, err=%v", retry, err)
	}
	if _, err := store.MoveReady(ctx, now.Add(2*time.Second), 10, "default"); err != nil {
		t.Fatal(err)
	}
	retry, err = store.Claim(ctx, "default", now.Add(2*time.Second), time.Second)
	if err != nil || retry == nil {
		t.Fatalf("retry claim = %+v, err=%v", retry, err)
	}
	if err := store.Archive(ctx, retry, "permanent"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ArchivedCount(ctx, "default"); err != nil || got != 1 {
		t.Fatalf("archived count = %d, err=%v", got, err)
	}
}

func TestExtendLeaseMovesExpiryForward(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := message("heartbeat", 1, now)
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ExtendLease(ctx, "default", []string{active.ID}, now, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := rdb.ZScore(ctx, LeaseKey("default"), active.ID).Val(); got != float64(now.Add(10*time.Second).UnixMilli()) {
		t.Fatalf("lease expiry = %v", got)
	}
}
