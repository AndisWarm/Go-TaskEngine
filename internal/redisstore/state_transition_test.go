package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/internal/model"
)

func TestAckSuccessRetainsCompletedTaskRecord(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := message("completed", 1, now)
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
	got, err := store.Get(ctx, "default", "completed")
	if err != nil {
		t.Fatalf("completed task should remain queryable: %v", err)
	}
	if got.State != model.StateCompleted {
		t.Fatalf("completed state = %s", got.State)
	}
	if rdb.Exists(ctx, TaskKey("default", "completed")).Val() != 1 {
		t.Fatal("completed task record was deleted")
	}
}

func TestStateTransitionsRejectInactiveTask(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	msg := message("transition", 1, time.UnixMilli(1000))
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err := store.AckSuccess(ctx, msg); err == nil {
		t.Fatal("ack of pending task was accepted")
	}
	if err := store.Requeue(ctx, msg); err == nil {
		t.Fatal("requeue of pending task was accepted")
	}
}

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
