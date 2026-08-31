// Package storage defines the durable task-store contract used by the engine.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-taskengine/model"
)

var (
	// ErrNoTask is the compatibility root for operations that cannot return a task.
	ErrNoTask = errors.New("no processable task")
	// ErrQueueEmpty means Claim has no task available in the requested queue.
	ErrQueueEmpty = fmt.Errorf("task queue is empty: %w", ErrNoTask)
	// ErrTaskNotFound means a task lookup found no task for the requested queue and ID.
	ErrTaskNotFound = fmt.Errorf("task not found: %w", ErrNoTask)
	// ErrTaskExists means enqueue or schedule found an existing task ID.
	ErrTaskExists = errors.New("task already exists")
	// ErrInvalidTransition means the requested durable state transition is not valid.
	ErrInvalidTransition = errors.New("invalid task state transition")
)

// IsQueueEmpty reports whether err means Claim found no available task.
// It accepts the legacy ErrNoTask unless the error is the new ErrTaskNotFound.
func IsQueueEmpty(err error) bool {
	if err == nil || errors.Is(err, ErrTaskNotFound) {
		return false
	}
	return errors.Is(err, ErrQueueEmpty) || errors.Is(err, ErrNoTask)
}

// TaskStore is the storage contract required by the server worker engine.
// Implementations must make task state transitions durable and safe for concurrent callers.
type TaskStore interface {
	// Enqueue persists an immediately processable task and returns ErrTaskExists for a duplicate ID.
	Enqueue(context.Context, *model.TaskMessage) error
	// Schedule persists a delayed task and returns ErrTaskExists for a duplicate ID.
	Schedule(context.Context, *model.TaskMessage) error
	// Claim atomically activates one task or returns ErrQueueEmpty when the queue is empty.
	Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error)
	MoveReady(context.Context, time.Time, int, ...string) (int, error)
	// AckSuccess completes an active task and returns ErrInvalidTransition for a state mismatch.
	AckSuccess(context.Context, *model.TaskMessage) error
	// ScheduleRetry moves an active task to retry and returns ErrInvalidTransition for a state mismatch.
	ScheduleRetry(context.Context, *model.TaskMessage, time.Time, string) error
	// Archive moves an active task to the archive and returns ErrInvalidTransition for a state mismatch.
	Archive(context.Context, *model.TaskMessage, string) error
	// Requeue returns an active task to pending and returns ErrInvalidTransition for a state mismatch.
	Requeue(context.Context, *model.TaskMessage) error
	// Get returns ErrTaskNotFound when the requested task does not exist.
	Get(context.Context, string, string) (*model.TaskMessage, error)
	ExpiredIDs(context.Context, time.Time, string, int) ([]string, error)
	PendingCount(context.Context, string) (int64, error)
	RetryCount(context.Context, string) (int64, error)
	ArchivedCount(context.Context, string) (int64, error)
	ExtendLease(context.Context, string, []string, time.Time, time.Duration) error
}

// DeadLetterStore manages tasks that have been moved to the archived state.
// Replay resets the retry counter and returns the task to the pending state.
type DeadLetterStore interface {
	ListDeadLetters(context.Context, string, int, int) ([]*model.TaskMessage, error)
	GetDeadLetter(context.Context, string, string) (*model.TaskMessage, error)
	ReplayDeadLetter(context.Context, string, string) error
	DeleteDeadLetter(context.Context, string, string) error
	CleanupDeadLetters(context.Context, string, time.Time, int) (int, error)
}
