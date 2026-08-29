package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/limiter"
	"go-taskengine/redisstore"
)

func TestTwoServersShareOneTokenBucketRealRedis(t *testing.T) {
	if os.Getenv("GTE_REAL_REDIS") != "1" {
		t.Skip("set GTE_REAL_REDIS=1 to run the real Redis limiter test")
	}
	redisPath, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skipf("redis-server is unavailable: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	redisProcess := exec.Command(redisPath, "--save", "", "--appendonly", "no", "--port", strconv.Itoa(port))
	redisProcess.Stdout = io.Discard
	redisProcess.Stderr = io.Discard
	if err := redisProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = redisProcess.Process.Kill()
		_, _ = redisProcess.Process.Wait()
	}()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	waitForRedis(t, rdb)
	store := redisstore.New(rdb)
	producer := client.NewClient(store)
	var processed atomic.Int32
	var mu sync.Mutex
	var times []time.Time
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		processed.Add(1)
		return nil
	})
	first := New(store, handler, Config{
		Concurrency: 1, PollInterval: time.Millisecond,
		TokenBucket: limiter.NewScopedTokenBucket(rdb, "real-two-servers", 1, 10),
	})
	second := New(store, handler, Config{
		Concurrency: 1, PollInterval: time.Millisecond,
		TokenBucket: limiter.NewScopedTokenBucket(rdb, "real-two-servers", 1, 10),
	})
	for i := 0; i < 4; i++ {
		if _, err := producer.Enqueue(context.Background(), client.NewTask("real-shared-limit", nil), client.WithTaskID("real-shared-limit-"+string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return processed.Load() == 4 })
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := second.Shutdown(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	elapsed := times[len(times)-1].Sub(times[0])
	mu.Unlock()
	t.Logf("real two-server shared bucket elapsed = %s", elapsed)
	if elapsed < 250*time.Millisecond {
		t.Fatalf("real shared token bucket elapsed = %s, want at least 250ms", elapsed)
	}
}
