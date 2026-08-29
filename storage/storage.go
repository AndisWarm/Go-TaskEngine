// Package storage defines the durable task-store contract used by the engine.
package storage

import (
	"context"
	"time"

	"go-taskengine/model"
)

// TaskStore is the storage contract required by the server worker engine.
// Implementations must make task state transitions durable and safe for concurrent callers.
type TaskStore interface {
	Enqueue(context.Context, *model.TaskMessage) error
	Schedule(context.Context, *model.TaskMessage) error
	Claim(context.Context, string, time.Time, time.Duration) (*model.TaskMessage, error)
	MoveReady(context.Context, time.Time, int, ...string) (int, error)
	AckSuccess(context.Context, *model.TaskMessage) error
	ScheduleRetry(context.Context, *model.TaskMessage, time.Time, string) error
	Archive(context.Context, *model.TaskMessage, string) error
	Requeue(context.Context, *model.TaskMessage) error
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
