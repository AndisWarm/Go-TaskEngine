// Package model contains task data shared by the client, server, and Redis store.
package model

import (
	"errors"
	"fmt"
	"time"
)

// TaskState is the durable state of a task in Redis.
type TaskState string

const (
	StatePending   TaskState = "pending"
	StateScheduled TaskState = "scheduled"
	StateActive    TaskState = "active"
	StateRetry     TaskState = "retry"
	StateArchived  TaskState = "archived"
	StateCompleted TaskState = "completed"
)

func (s TaskState) String() string { return string(s) }

// TaskMessage is the serialized task envelope stored in Redis.
type TaskMessage struct {
	ID           string        `json:"id"`
	Type         string        `json:"type"`
	Payload      []byte        `json:"payload"`
	Queue        string        `json:"queue"`
	Priority     int           `json:"priority"`
	MaxRetry     int           `json:"max_retry"`
	RetryCount   int           `json:"retry_count"`
	Timeout      time.Duration `json:"timeout"`
	Deadline     time.Time     `json:"deadline,omitempty"`
	RunAt        time.Time     `json:"run_at"`
	State        TaskState     `json:"state"`
	LastError    string        `json:"last_error,omitempty"`
	LastFailedAt time.Time     `json:"last_failed_at,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
}

// Validate checks fields that are required for durable storage.
func (m *TaskMessage) Validate() error {
	if m == nil {
		return errors.New("task message is nil")
	}
	if m.ID == "" {
		return errors.New("task id is required")
	}
	if m.Type == "" {
		return errors.New("task type is required")
	}
	if m.Queue == "" {
		return errors.New("task queue is required")
	}
	if m.Priority < 0 {
		return fmt.Errorf("task priority must be non-negative: %d", m.Priority)
	}
	if m.MaxRetry < 0 {
		return fmt.Errorf("max retry must be non-negative: %d", m.MaxRetry)
	}
	if m.Timeout < 0 {
		return errors.New("task timeout cannot be negative")
	}
	return nil
}
