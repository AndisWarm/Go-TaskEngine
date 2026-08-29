package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/internal/model"
	"go-taskengine/internal/redisstore"
	"go-taskengine/storage"
)

func TestExponentialBackoffIsCapped(t *testing.T) {
	base := 2 * time.Second
	if got := ExponentialBackoff(0, base, time.Minute); got != 2*time.Second {
		t.Fatal(got)
	}
	if got := ExponentialBackoff(1, base, time.Minute); got != 4*time.Second {
		t.Fatal(got)
	}
	if got := ExponentialBackoff(2, base, time.Minute); got != 8*time.Second {
		t.Fatal(got)
	}
	if got := ExponentialBackoff(10, base, 10*time.Second); got != 10*time.Second {
		t.Fatal(got)
	}
}

func TestExponentialBackoffWithJitterStaysWithinBounds(t *testing.T) {
	for i := 0; i < 100; i++ {
		got := ExponentialBackoffWithJitter(0, 10*time.Millisecond, 100*time.Millisecond, 0.5)
		if got < 5*time.Millisecond || got > 15*time.Millisecond {
			t.Fatalf("jittered delay = %s, want range [5ms, 15ms]", got)
		}
	}
	for i := 0; i < 100; i++ {
		got := ExponentialBackoffWithJitter(10, 10*time.Millisecond, 15*time.Millisecond, 0.5)
		if got <= 0 || got > 15*time.Millisecond {
			t.Fatalf("capped jittered delay = %s, want (0, 15ms]", got)
		}
	}
}

func TestServerRejectsInvalidRetryJitter(t *testing.T) {
	s, _ := testServer(t, HandlerFunc(func(context.Context, *TaskMessage) error { return nil }), Config{RetryJitter: 1.1})
	if err := s.Start(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start error = %v, want ErrInvalidConfig", err)
	}
}

func TestServerRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error {
		if attempts.Add(1) < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	s, producer := testServer(t, handler, Config{Concurrency: 1, PollInterval: time.Millisecond, RetryBaseDelay: 5 * time.Millisecond})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("retry", nil), client.WithTaskID("retry-1"), client.WithMaxRetry(3)); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return attempts.Load() == 3 })
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestServerRecoveryLoopSchedulesExpiredLeaseRetry(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	store := redisstore.New(rdb)
	producer := client.NewClient(store)
	started := make(chan struct{})
	release := make(chan struct{})
	var attempts atomic.Int32
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error {
		if attempts.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	s := New(store, handler, Config{
		Concurrency:       1,
		LeaseDuration:     time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		RecoveryInterval:  time.Millisecond,
		RetryBaseDelay:    time.Millisecond,
		PollInterval:      time.Millisecond,
	})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("lease-retry", nil), client.WithTaskID("lease-retry-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	ctx := context.Background()
	if _, err := rdb.ZAdd(ctx, redisstore.LeaseKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: "lease-retry-1"}).Result(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		msg, err := store.Get(ctx, "default", "lease-retry-1")
		return err == nil && msg.RetryCount >= 1 && msg.LastError == "task lease expired"
	})
	close(release)
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestServerArchivesNonRetryableFailureAsDeadLetter(t *testing.T) {
	var attempts atomic.Int32
	s, producer := testServer(t, HandlerFunc(func(context.Context, *TaskMessage) error {
		attempts.Add(1)
		return ErrNonRetryable
	}), Config{PollInterval: time.Millisecond})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("non-retryable", nil), client.WithTaskID("non-retryable-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		count, err := s.store.ArchivedCount(context.Background(), "default")
		return err == nil && count == 1
	})
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("non-retryable attempts = %d, want 1", attempts.Load())
	}
	deadLetters, ok := s.store.(storage.DeadLetterStore)
	if !ok {
		t.Fatal("server store does not expose dead-letter management")
	}
	msg, err := deadLetters.GetDeadLetter(context.Background(), "default", "non-retryable-1")
	if err != nil {
		t.Fatal(err)
	}
	if msg.LastError != ErrNonRetryable.Error() || msg.State != model.StateArchived {
		t.Fatalf("dead letter = %+v", msg)
	}
}

func TestServerArchivesAfterMaximumRetries(t *testing.T) {
	var attempts atomic.Int32
	s, producer := testServer(t, HandlerFunc(func(context.Context, *TaskMessage) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), Config{PollInterval: time.Millisecond, RetryBaseDelay: time.Millisecond})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("max-retry", nil), client.WithTaskID("max-retry-1"), client.WithMaxRetry(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		count, err := s.store.ArchivedCount(context.Background(), "default")
		return err == nil && count == 1
	})
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("maximum retry attempts = %d, want 2", attempts.Load())
	}
}

func TestPermanentFailureIsArchived(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	store := redisstore.New(rdb)
	msg := &model.TaskMessage{ID: "dead-1", Type: "dead", Queue: "default", MaxRetry: 3, RunAt: time.Now(), CreatedAt: time.Now()}
	if err := store.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(context.Background(), "default", time.Now(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Archive(context.Background(), active, "bad input"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ArchivedCount(context.Background(), "default"); err != nil || got != 1 {
		t.Fatalf("archived=%d err=%v", got, err)
	}
}

func TestExpiredLeaseCanBeRecovered(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	store := redisstore.New(rdb)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := &model.TaskMessage{ID: "lease-1", Type: "lease", Queue: "default", MaxRetry: 2, RunAt: now, CreatedAt: now}
	if err := store.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	active, err := store.Claim(ctx, "default", now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := store.ExpiredIDs(ctx, now.Add(2*time.Millisecond), "default", 10)
	if err != nil || len(ids) != 1 || ids[0] != active.ID {
		t.Fatalf("expired=%v err=%v", ids, err)
	}
	if err := store.Requeue(ctx, active); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "default", now.Add(3*time.Millisecond), time.Second); err != nil {
		t.Fatal(err)
	}
}
