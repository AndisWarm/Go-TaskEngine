package server

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go-taskengine/client"
)

func TestShutdownTimeoutRequeuesRunningTask(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	handler := HandlerFunc(func(_ context.Context, _ *TaskMessage) error {
		close(started)
		<-release
		return nil
	})
	s, producer := testServer(t, handler, Config{Concurrency: 1, ShutdownTimeout: 20 * time.Millisecond, PollInterval: time.Millisecond})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("slow", nil), client.WithTaskID("slow-1")); err != nil {
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
	if err := s.Shutdown(); err == nil {
		t.Fatal("shutdown should report timeout")
	}
	if got := producerStorePending(t, s, "default"); got != 1 {
		t.Fatalf("pending after timeout = %d", got)
	}
	close(release)
}

func TestRunSignalsStopsOnInterrupt(t *testing.T) {
	s, _ := testServer(t, HandlerFunc(func(context.Context, *TaskMessage) error { return nil }), Config{PollInterval: time.Millisecond})
	signals := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- s.RunSignals(context.Background(), signals) }()
	time.Sleep(20 * time.Millisecond)
	signals <- os.Interrupt
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("signal did not stop server")
	}
}

func TestHandlerReceivesCancellationWhenServerStops(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var once atomic.Bool
	handler := HandlerFunc(func(ctx context.Context, _ *TaskMessage) error {
		close(started)
		<-ctx.Done()
		if once.CompareAndSwap(false, true) {
			close(canceled)
		}
		return nil
	})
	s, producer := testServer(t, handler, Config{Concurrency: 1, ShutdownTimeout: time.Second, PollInterval: time.Millisecond})
	if _, err := producer.Enqueue(context.Background(), client.NewTask("cancel", nil), client.WithTaskID("cancel-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler did not receive cancellation")
	}
}

func producerStorePending(t *testing.T, s *Server, queue string) int64 {
	t.Helper()
	return s.store.PendingCount(context.Background(), queue)
}
