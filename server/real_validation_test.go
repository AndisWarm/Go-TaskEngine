package server

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/redisstore"
)

func realRedisForValidation(t *testing.T) *redisstore.Store {
	t.Helper()
	if os.Getenv("GTE_REAL_REDIS") != "1" {
		t.Skip("set GTE_REAL_REDIS=1 to run real Redis validation")
	}
	addr := os.Getenv("GTE_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping Redis at %s: %v", addr, err)
	}
	return redisstore.New(rdb)
}

func realValidationRunID() string {
	return fmt.Sprintf("real-validation-%d", time.Now().UnixNano())
}

func TestRealRedisDelayedDispatchTiming(t *testing.T) {
	store := realRedisForValidation(t)
	producer := client.NewClient(store)
	started := make(chan time.Time, 1)
	server := New(store, HandlerFunc(func(context.Context, *TaskMessage) error {
		started <- time.Now()
		return nil
	}), Config{Concurrency: 1, PollInterval: 2 * time.Millisecond})

	runID := realValidationRunID()
	target := time.Now().Add(500 * time.Millisecond)
	if _, err := producer.EnqueueAt(context.Background(), client.NewTask("real-delayed", nil), target, client.WithTaskID(runID)); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case actual := <-started:
		delayError := actual.Sub(target)
		t.Logf("real Redis delayed dispatch target=500ms error=%s", delayError)
		if actual.Before(target) {
			t.Fatalf("task ran before target by %s", target.Sub(actual))
		}
		if delayError > 250*time.Millisecond {
			t.Fatalf("delayed task was late by %s", delayError)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("real Redis delayed task did not run")
	}
	if err := server.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestRealRedisFixedConcurrencyAndShutdown(t *testing.T) {
	store := realRedisForValidation(t)
	producer := client.NewClient(store)
	var running, maximum, processed atomic.Int32
	handler := HandlerFunc(func(ctx context.Context, _ *TaskMessage) error {
		current := running.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		select {
		case <-time.After(15 * time.Millisecond):
		case <-ctx.Done():
		}
		running.Add(-1)
		processed.Add(1)
		return nil
	})
	server := New(store, handler, Config{Concurrency: 4, PollInterval: time.Millisecond})
	runID := realValidationRunID()
	for i := 0; i < 20; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("real-concurrency", nil), client.WithTaskID(fmt.Sprintf("%s-%d", runID, i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return processed.Load() == 20 })
	shutdownStarted := time.Now()
	if err := server.Shutdown(); err != nil {
		t.Fatal(err)
	}
	t.Logf("real Redis fixed concurrency maximum=%d shutdown=%s", maximum.Load(), time.Since(shutdownStarted))
	if maximum.Load() > 4 {
		t.Fatalf("maximum concurrency = %d, want at most 4", maximum.Load())
	}
}

func TestRealRedisRetryDeadLetterThroughput(t *testing.T) {
	store := realRedisForValidation(t)
	producer := client.NewClient(store)
	metrics := NewMetrics()
	const taskCount = 10
	server := New(store, HandlerFunc(func(context.Context, *TaskMessage) error {
		return fmt.Errorf("real validation failure")
	}), Config{Concurrency: 4, PollInterval: time.Millisecond, Metrics: metrics})
	runID := realValidationRunID()
	for i := 0; i < taskCount; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("real-dead-letter", nil), client.WithTaskID(fmt.Sprintf("%s-%d", runID, i)), client.WithMaxRetry(0)); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return metrics.Snapshot().Archived == taskCount })
	elapsed := time.Since(started)
	if err := server.Shutdown(); err != nil {
		t.Fatal(err)
	}
	t.Logf("real Redis retry/dead-letter tasks=%d elapsed=%s throughput=%.1f tasks/s", taskCount, elapsed, float64(taskCount)/elapsed.Seconds())
}
