package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/model"
	"go-taskengine/redisstore"
)

func TestLeaseRecoveryChild(t *testing.T) {
	if os.Getenv("GTE_LEASE_CHILD") != "1" {
		return
	}
	addr := os.Getenv("GTE_REDIS_ADDR")
	if addr == "" {
		t.Fatal("GTE_REDIS_ADDR is required")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	store := redisstore.New(rdb)
	if _, err := store.Claim(context.Background(), "default", time.Now(), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestIndependentProcessLeaseRecovery(t *testing.T) {
	if os.Getenv("GTE_REAL_REDIS") != "1" {
		t.Skip("set GTE_REAL_REDIS=1 to run the real Redis process test")
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
	ctx := context.Background()
	waitForRedis(t, rdb)
	store := redisstore.New(rdb)
	producer := client.NewClient(store)
	if _, err := producer.Enqueue(ctx, client.NewTask("process-recovery", nil), client.WithTaskID("process-recovery-1")); err != nil {
		t.Fatal(err)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestLeaseRecoveryChild$", "-test.v")
	child.Env = append(os.Environ(), "GTE_LEASE_CHILD=1", "GTE_REDIS_ADDR="+addr)
	child.Stdout = io.Discard
	child.Stderr = io.Discard
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		msg, err := store.Get(ctx, "default", "process-recovery-1")
		return err == nil && msg.State == model.StateActive
	})
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = child.Process.Wait()

	processed := make(chan struct{}, 1)
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error {
		processed <- struct{}{}
		return nil
	}), Config{
		Concurrency:       1,
		LeaseDuration:     50 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
		RecoveryInterval:  5 * time.Millisecond,
		RetryBaseDelay:    time.Millisecond,
		PollInterval:      time.Millisecond,
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("new server did not recover task from terminated process")
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func waitForRedis(t *testing.T, rdb *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("redis did not start before timeout")
}
