package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-taskengine/model"
	"go-taskengine/storage"
)

type emptyContractStore struct {
	storage.TaskStore
	claimed  chan struct{}
	once     sync.Once
	claimErr error
}

var _ storage.TaskStore = (*emptyContractStore)(nil)

func (s *emptyContractStore) MoveReady(context.Context, time.Time, int, ...string) (int, error) {
	return 0, nil
}

func (s *emptyContractStore) PendingCount(context.Context, string) (int64, error) {
	return 1, nil
}

func (s *emptyContractStore) Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error) {
	s.once.Do(func() { close(s.claimed) })
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	return nil, storage.ErrNoTask
}

func TestServerTreatsStorageErrNoTaskAsEmptyQueue(t *testing.T) {
	store := &emptyContractStore{claimed: make(chan struct{})}
	errorsSeen := make(chan error, 1)
	var handled atomic.Int32
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error {
		handled.Add(1)
		return nil
	}), Config{
		PollInterval:      time.Millisecond,
		LeaseDuration:     2 * time.Hour,
		HeartbeatInterval: time.Hour,
		RecoveryInterval:  time.Hour,
		ErrorHandler: func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		},
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.claimed:
	case <-time.After(time.Second):
		t.Fatal("server did not call Claim")
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errorsSeen:
		t.Fatalf("empty queue was reported as an error: %v", err)
	default:
	}
	if got := handled.Load(); got != 0 {
		t.Fatalf("handled %d tasks from an empty store", got)
	}
}

func TestServerReportsTaskNotFoundFromClaim(t *testing.T) {
	store := &emptyContractStore{
		claimed:  make(chan struct{}),
		claimErr: storage.ErrTaskNotFound,
	}
	errorsSeen := make(chan error, 1)
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error { return nil }), Config{
		PollInterval:      time.Millisecond,
		LeaseDuration:     2 * time.Hour,
		HeartbeatInterval: time.Hour,
		RecoveryInterval:  time.Hour,
		ErrorHandler: func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		},
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()
	select {
	case err := <-errorsSeen:
		if !errors.Is(err, storage.ErrTaskNotFound) {
			t.Fatalf("reported error = %v, want ErrTaskNotFound", err)
		}
	case <-time.After(time.Second):
		t.Fatal("task-not-found claim error was treated as an empty queue")
	}
}
