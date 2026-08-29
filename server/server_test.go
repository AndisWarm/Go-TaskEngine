package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/internal/limiter"
	"go-taskengine/internal/redisstore"
)

func testServer(t *testing.T, handler Handler, cfg Config) (*Server, *client.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := redisstore.New(rdb)
	return New(store, handler, cfg), client.NewClient(store)
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestServerHonorsFixedConcurrency(t *testing.T) {
	var running, maximum, processed atomic.Int32
	handler := HandlerFunc(func(ctx context.Context, msg *TaskMessage) error {
		current := running.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
		}
		running.Add(-1)
		processed.Add(1)
		return nil
	})
	s, producer := testServer(t, handler, Config{Concurrency: 2, PollInterval: time.Millisecond})
	for i := 0; i < 8; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("compute", []byte("x")), client.WithTaskID(string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return processed.Load() == 8 })
	if maximum.Load() > 2 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestServerProcessesHigherPriorityQueueFirst(t *testing.T) {
	var mu sync.Mutex
	var order []string
	handler := HandlerFunc(func(_ context.Context, msg *TaskMessage) error {
		mu.Lock()
		order = append(order, msg.Queue)
		mu.Unlock()
		return nil
	})
	s, producer := testServer(t, handler, Config{Concurrency: 1, PollInterval: time.Millisecond, Queues: map[string]int{"low": 1, "high": 10}})
	for i := 0; i < 3; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("work", nil), client.WithTaskID("low-"+string(rune('a'+i))), client.WithQueue("low")); err != nil {
			t.Fatal(err)
		}
		if _, err := producer.Enqueue(context.Background(), client.NewTask("work", nil), client.WithTaskID("high-"+string(rune('a'+i))), client.WithQueue("high")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 6 })
	if order[0] != "high" {
		t.Fatalf("first queue = %q, order=%v", order[0], order)
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestServerForwardsMillisecondDelayedTask(t *testing.T) {
	started := make(chan time.Time, 1)
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error { started <- time.Now(); return nil })
	s, producer := testServer(t, handler, Config{Concurrency: 1, PollInterval: time.Millisecond})
	before := time.Now()
	if _, err := producer.EnqueueIn(context.Background(), client.NewTask("delayed", nil), 50*time.Millisecond, client.WithTaskID("delayed-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-started:
		if at.Before(before.Add(35 * time.Millisecond)) {
			t.Fatalf("task ran too early: %s", at.Sub(before))
		}
	case <-time.After(time.Second):
		t.Fatal("delayed task did not run")
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestServerConsumesAtConfiguredTokenRate(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	store := redisstore.New(rdb)
	bucket := limiter.NewTokenBucket(rdb, "gte:limit:integration", 2, 10)
	var count atomic.Int32
	var mu sync.Mutex
	var times []time.Time
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		count.Add(1)
		return nil
	})
	s := New(store, handler, Config{Concurrency: 1, PollInterval: time.Millisecond, TokenBucket: bucket})
	producer := client.NewClient(store)
	for i := 0; i < 4; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("limited", nil), client.WithTaskID("limited-"+string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return count.Load() == 4 })
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	elapsed := times[len(times)-1].Sub(times[0])
	mu.Unlock()
	if elapsed < 150*time.Millisecond {
		t.Fatalf("four tasks consumed in %s, token rate was not enforced", elapsed)
	}
}
