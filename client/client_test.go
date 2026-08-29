package client

import (
	"context"
	"testing"
	"time"

	"go-taskengine/internal/model"
)

type recordingStore struct {
	queued   []*model.TaskMessage
	schedule []*model.TaskMessage
}

func (s *recordingStore) Enqueue(_ context.Context, msg *model.TaskMessage) error {
	s.queued = append(s.queued, msg)
	return nil
}
func (s *recordingStore) Schedule(_ context.Context, msg *model.TaskMessage) error {
	s.schedule = append(s.schedule, msg)
	return nil
}

func TestEnqueueBuildsImmediateMessage(t *testing.T) {
	store := new(recordingStore)
	c := NewClient(store)
	msg, err := c.Enqueue(context.Background(), NewTask("image:resize", []byte("payload")),
		WithTaskID("task-1"), WithQueue("compute"), WithPriority(7), WithMaxRetry(4), WithTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != "task-1" || msg.Queue != "compute" || msg.Priority != 7 || msg.MaxRetry != 4 {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if msg.State != model.StatePending || len(store.queued) != 1 || len(store.schedule) != 0 {
		t.Fatalf("message was not enqueued immediately: %+v", msg)
	}
	if msg.Timeout != 3*time.Second {
		t.Fatalf("timeout = %v", msg.Timeout)
	}
}

func TestEnqueueInRoutesToScheduleWithMillisecondTime(t *testing.T) {
	store := new(recordingStore)
	c := NewClient(store)
	c.now = func() time.Time { return time.UnixMilli(1000) }
	msg, err := c.Enqueue(context.Background(), NewTask("ai:infer", nil),
		WithTaskID("task-2"), ProcessIn(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if len(store.schedule) != 1 || len(store.queued) != 0 {
		t.Fatalf("message was not scheduled: %+v", msg)
	}
	if got := msg.RunAt.UnixMilli(); got != 1500 {
		t.Fatalf("run-at = %d, want 1500", got)
	}
	if msg.State != model.StateScheduled {
		t.Fatalf("state = %s", msg.State)
	}
}

func TestTaskOptionsAreOverriddenByEnqueueOptions(t *testing.T) {
	store := new(recordingStore)
	c := NewClient(store)
	msg, err := c.Enqueue(context.Background(),
		NewTask("transcode", nil, WithQueue("low"), WithPriority(1)),
		WithQueue("high"), WithPriority(10))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Queue != "high" || msg.Priority != 10 {
		t.Fatalf("options were not overridden: %+v", msg)
	}
}

func TestInvalidOptionsAreRejected(t *testing.T) {
	c := NewClient(new(recordingStore))
	if _, err := c.Enqueue(context.Background(), NewTask("x", nil), WithPriority(-1)); err == nil {
		t.Fatal("negative priority was accepted")
	}
	if _, err := c.Enqueue(context.Background(), NewTask("x", nil), ProcessIn(-time.Second)); err == nil {
		t.Fatal("negative delay was accepted")
	}
}
