package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go-taskengine/model"
)

type publicStore struct{}

func (publicStore) Enqueue(context.Context, *model.TaskMessage) error  { return nil }
func (publicStore) Schedule(context.Context, *model.TaskMessage) error { return nil }
func (publicStore) Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error) {
	return nil, nil
}
func (publicStore) MoveReady(context.Context, time.Time, int, ...string) (int, error) {
	return 0, nil
}
func (publicStore) AckSuccess(context.Context, *model.TaskMessage) error { return nil }
func (publicStore) ScheduleRetry(context.Context, *model.TaskMessage, time.Time, string) error {
	return nil
}
func (publicStore) Archive(context.Context, *model.TaskMessage, string) error { return nil }
func (publicStore) Requeue(context.Context, *model.TaskMessage) error         { return nil }
func (publicStore) Get(context.Context, string, string) (*model.TaskMessage, error) {
	return nil, nil
}
func (publicStore) ExpiredIDs(context.Context, time.Time, string, int) ([]string, error) {
	return nil, nil
}
func (publicStore) PendingCount(context.Context, string) (int64, error)  { return 0, nil }
func (publicStore) RetryCount(context.Context, string) (int64, error)    { return 0, nil }
func (publicStore) ArchivedCount(context.Context, string) (int64, error) { return 0, nil }
func (publicStore) ExtendLease(context.Context, string, []string, time.Time, time.Duration) error {
	return nil
}

func TestPublicTaskStoreInterfaceIsImplementable(t *testing.T) {
	var _ TaskStore = publicStore{}
}

func TestStorageErrorsAreDefinedAndDistinct(t *testing.T) {
	contractErrors := []error{ErrNoTask, ErrTaskExists, ErrInvalidTransition}
	for i, err := range contractErrors {
		if err == nil {
			t.Fatalf("contract error %d is nil", i)
		}
		for j := i + 1; j < len(contractErrors); j++ {
			if err == contractErrors[j] {
				t.Fatalf("contract errors %d and %d have the same identity", i, j)
			}
		}
	}
}

func TestTaskAbsenceErrorsAreDistinctAndBackwardCompatible(t *testing.T) {
	if ErrQueueEmpty == ErrTaskNotFound {
		t.Fatal("queue-empty and task-not-found errors share one identity")
	}
	if !errors.Is(ErrQueueEmpty, ErrNoTask) {
		t.Fatal("ErrQueueEmpty is not compatible with ErrNoTask")
	}
	if !errors.Is(ErrTaskNotFound, ErrNoTask) {
		t.Fatal("ErrTaskNotFound is not compatible with ErrNoTask")
	}
	if !IsQueueEmpty(ErrQueueEmpty) {
		t.Fatal("ErrQueueEmpty was not classified as an empty queue")
	}
	if !IsQueueEmpty(fmt.Errorf("legacy claim: %w", ErrNoTask)) {
		t.Fatal("wrapped legacy ErrNoTask was not classified as an empty queue")
	}
	if IsQueueEmpty(ErrTaskNotFound) {
		t.Fatal("ErrTaskNotFound was classified as an empty queue")
	}
}
