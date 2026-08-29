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
