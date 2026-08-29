package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go-taskengine/internal/model"
)

const (
	DefaultQueue    = "default"
	DefaultMaxRetry = 3
)

// Store is the durable task store used by Client.
type Store interface {
	Enqueue(context.Context, *model.TaskMessage) error
	Schedule(context.Context, *model.TaskMessage) error
}

// Task is a task before it is persisted.
type Task struct {
	typeName string
	payload  []byte
	opts     []Option
}

func NewTask(typeName string, payload []byte, opts ...Option) *Task {
	return &Task{typeName: typeName, payload: append([]byte(nil), payload...), opts: opts}
}

func (t *Task) Type() string    { return t.typeName }
func (t *Task) Payload() []byte { return append([]byte(nil), t.payload...) }

// Option changes how a task is persisted.
type Option func(*taskOptions) error

type taskOptions struct {
	id       string
	queue    string
	priority int
	maxRetry int
	timeout  time.Duration
	deadline time.Time
	runAt    time.Time
	runAtSet bool
	runIn    time.Duration
	runInSet bool
}

func WithTaskID(id string) Option {
	return func(o *taskOptions) error {
		if id == "" {
			return errors.New("task id cannot be empty")
		}
		o.id = id
		return nil
	}
}

func WithQueue(queue string) Option {
	return func(o *taskOptions) error {
		if queue == "" {
			return errors.New("queue cannot be empty")
		}
		o.queue = queue
		return nil
	}
}

func WithPriority(priority int) Option {
	return func(o *taskOptions) error {
		if priority < 0 {
			return fmt.Errorf("priority must be non-negative: %d", priority)
		}
		o.priority = priority
		return nil
	}
}

func WithMaxRetry(maxRetry int) Option {
	return func(o *taskOptions) error {
		if maxRetry < 0 {
			return fmt.Errorf("max retry must be non-negative: %d", maxRetry)
		}
		o.maxRetry = maxRetry
		return nil
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *taskOptions) error {
		if timeout < 0 {
			return errors.New("timeout cannot be negative")
		}
		o.timeout = timeout
		return nil
	}
}

func WithDeadline(deadline time.Time) Option {
	return func(o *taskOptions) error {
		o.deadline = deadline
		return nil
	}
}

func ProcessAt(at time.Time) Option {
	return func(o *taskOptions) error {
		if at.IsZero() {
			return errors.New("process time cannot be zero")
		}
		o.runAt = at
		o.runAtSet = true
		o.runInSet = false
		return nil
	}
}

func ProcessIn(delay time.Duration) Option {
	return func(o *taskOptions) error {
		if delay < 0 {
			return errors.New("process delay cannot be negative")
		}
		o.runIn = delay
		o.runInSet = true
		o.runAtSet = false
		return nil
	}
}

// Client creates durable tasks. A Client is safe to use concurrently when its Store is safe.
type Client struct {
	store Store
	now   func() time.Time
	newID func() (string, error)
}

func NewClient(store Store) *Client {
	return &Client{store: store, now: time.Now, newID: randomID}
}

func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...Option) (*model.TaskMessage, error) {
	if c == nil || c.store == nil {
		return nil, errors.New("client store is nil")
	}
	if task == nil {
		return nil, errors.New("task is nil")
	}
	if task.typeName == "" {
		return nil, errors.New("task type cannot be empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.now()
	id, err := c.newID()
	if err != nil {
		return nil, fmt.Errorf("create task id: %w", err)
	}
	options := taskOptions{id: id, queue: DefaultQueue, maxRetry: DefaultMaxRetry, runAt: now}
	for _, option := range append(append([]Option(nil), task.opts...), opts...) {
		if option == nil {
			continue
		}
		if err := option(&options); err != nil {
			return nil, err
		}
	}
	if options.runInSet {
		options.runAt = now.Add(options.runIn)
	}
	msg := &model.TaskMessage{
		ID:        options.id,
		Type:      task.typeName,
		Payload:   task.Payload(),
		Queue:     options.queue,
		Priority:  options.priority,
		MaxRetry:  options.maxRetry,
		Timeout:   options.timeout,
		Deadline:  options.deadline,
		RunAt:     options.runAt,
		CreatedAt: now,
		State:     model.StatePending,
	}
	if msg.RunAt.After(now) {
		msg.State = model.StateScheduled
		if err := c.store.Schedule(ctx, msg); err != nil {
			return nil, err
		}
		return msg, nil
	}
	if err := c.store.Enqueue(ctx, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Client) EnqueueAt(ctx context.Context, task *Task, at time.Time, opts ...Option) (*model.TaskMessage, error) {
	return c.Enqueue(ctx, task, append([]Option{ProcessAt(at)}, opts...)...)
}

func (c *Client) EnqueueIn(ctx context.Context, task *Task, delay time.Duration, opts ...Option) (*model.TaskMessage, error) {
	return c.Enqueue(ctx, task, append([]Option{ProcessAt(c.now().Add(delay))}, opts...)...)
}

func randomID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
