package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-taskengine/model"
	"go-taskengine/storage"
)

type cancelAwareAckStore struct {
	storage.TaskStore
	started  chan struct{}
	canceled chan struct{}
}

func (s *cancelAwareAckStore) AckSuccess(ctx context.Context, _ *model.TaskMessage) error {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return ctx.Err()
}

func TestProcessStorageTransitionUsesLifecycleContext(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	store := &cancelAwareAckStore{started: make(chan struct{}), canceled: make(chan struct{})}
	s := &Server{
		store:      store,
		handler:    HandlerFunc(func(context.Context, *TaskMessage) error { return nil }),
		cfg:        Config{},
		ctx:        lifecycle,
		handlerCtx: context.Background(),
	}
	done := make(chan struct{})
	go func() {
		s.process(&model.TaskMessage{ID: "context-1", Queue: "default"})
		close(done)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("AckSuccess did not start")
	}
	cancel()
	select {
	case <-store.canceled:
	case <-time.After(time.Second):
		t.Fatal("AckSuccess did not receive lifecycle cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not return after lifecycle cancellation")
	}
	if !errors.Is(lifecycle.Err(), context.Canceled) {
		t.Fatalf("lifecycle error = %v", lifecycle.Err())
	}
}
