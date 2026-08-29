package server

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/internal/redisstore"
)

func benchmarkTaskID(runID string, index int) string {
	return fmt.Sprintf("real-bench-%s-%d", runID, index)
}

func benchmarkQueue(runID string) string {
	return "real-benchmark-" + runID
}

func TestBenchmarkQueueIsolated(t *testing.T) {
	if got, want := benchmarkQueue("run-a"), "real-benchmark-run-a"; got != want {
		t.Fatalf("benchmark queue = %q, want %q", got, want)
	}
}

func TestBenchmarkTaskIDIncludesRunID(t *testing.T) {
	first := benchmarkTaskID("run-a", 0)
	second := benchmarkTaskID("run-b", 0)
	if first == second {
		t.Fatalf("benchmark task IDs collide across runs: %q", first)
	}
	if got, want := benchmarkTaskID("run-a", 3), "real-bench-run-a-3"; got != want {
		t.Fatalf("benchmark task ID = %q, want %q", got, want)
	}
}

func BenchmarkClientEnqueue(b *testing.B) {
	mini := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	producer := client.NewClient(redisstore.New(rdb))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := producer.Enqueue(context.Background(), client.NewTask("benchmark", []byte("payload")), client.WithTaskID(fmt.Sprintf("bench-%d", i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientEnqueueRealRedis(b *testing.B) {
	if os.Getenv("GTE_REAL_REDIS") != "1" {
		b.Skip("set GTE_REAL_REDIS=1 to run the real Redis benchmark")
	}
	addr := os.Getenv("GTE_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	b.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("ping Redis at %s: %v", addr, err)
	}
	producer := client.NewClient(redisstore.New(rdb))
	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	queue := benchmarkQueue(runID)
	b.Cleanup(func() {
		ctx := context.Background()
		keys, _ := rdb.Keys(ctx, redisstore.TaskKey(queue, "")+"*").Result()
		keys = append(keys,
			redisstore.PendingKey(queue),
			redisstore.PendingRankKey(queue),
			redisstore.ScheduledKey(queue),
			redisstore.RetryKey(queue),
			redisstore.ActiveKey(queue),
			redisstore.LeaseKey(queue),
			redisstore.ArchivedKey(queue),
		)
		_, _ = rdb.Del(ctx, keys...).Result()
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := producer.Enqueue(context.Background(), client.NewTask("real-benchmark", []byte("payload")), client.WithTaskID(benchmarkTaskID(runID, i)), client.WithQueue(queue))
		if err != nil {
			b.Fatal(err)
		}
	}
}
