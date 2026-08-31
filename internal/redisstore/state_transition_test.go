package redisstore

import (
	"context"
	"errors"
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
	if err := store.ExtendLease(ctx, "default", []*model.TaskMessage{active}, now.Add(time.Second), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.ZScore(ctx, LeaseKey("default"), active.ID).Result(); err != redis.Nil {
		t.Fatalf("completed task lease error = %v, want redis.Nil", err)
	}
}

func reclaimForAttemptTest(t *testing.T, store *Store, id string, now time.Time) (*model.TaskMessage, *model.TaskMessage) {
	t.Helper()
	ctx := context.Background()
	if err := store.Enqueue(ctx, message(id, 1, now)); err != nil {
		t.Fatal(err)
	}
	stale, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	recovered := *stale
	recovered.RetryCount++
	if err := store.ScheduleRetry(ctx, &recovered, now, "expired lease"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MoveReady(ctx, now, 1, "default"); err != nil {
		t.Fatal(err)
	}
	current, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return stale, current
}

func TestStaleAttemptCannotMutateReclaimedTask(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	stale, current := reclaimForAttemptTest(t, store, "attempt-transition", now)

	transitions := []struct {
		name string
		run  func() error
	}{
		{name: "ack", run: func() error { return store.AckSuccess(ctx, stale) }},
		{name: "retry", run: func() error { return store.ScheduleRetry(ctx, stale, now.Add(time.Second), "stale") }},
		{name: "archive", run: func() error { return store.Archive(ctx, stale, "stale") }},
		{name: "requeue", run: func() error { return store.Requeue(ctx, stale) }},
	}
	for _, transition := range transitions {
		if err := transition.run(); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("stale %s error = %v, want ErrInvalidTransition", transition.name, err)
		}
	}
	if err := store.AckSuccess(ctx, current); err != nil {
		t.Fatalf("current attempt ack: %v", err)
	}
}

func TestStaleAttemptCannotExtendCurrentLease(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	stale, current := reclaimForAttemptTest(t, store, "attempt-heartbeat", now)
	leaseBefore := rdb.ZScore(ctx, LeaseKey("default"), current.ID).Val()

	if err := store.ExtendLease(ctx, "default", []*model.TaskMessage{stale}, now.Add(time.Hour), time.Hour); err != nil {
		t.Fatal(err)
	}
	if leaseAfter := rdb.ZScore(ctx, LeaseKey("default"), current.ID).Val(); leaseAfter != leaseBefore {
		t.Fatalf("stale heartbeat changed current lease from %v to %v", leaseBefore, leaseAfter)
	}
}
