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
	PendingCount(context.Context, string) int64
	RetryCount(context.Context, string) int64
	ArchivedCount(context.Context, string) int64
	ExtendLease(context.Context, string, []string, time.Time, time.Duration) error
}
