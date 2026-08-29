package server

import (
	"context"
	"errors"
	"testing"

	"go-taskengine/model"
)

func TestHandlerMuxRoutesByTaskType(t *testing.T) {
	mux := NewHandlerMux()
	var called string
	if err := mux.Handle("image.resize", HandlerFunc(func(_ context.Context, msg *TaskMessage) error {
		called = msg.Type
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := mux.ProcessTask(context.Background(), &model.TaskMessage{Type: "image.resize"}); err != nil {
		t.Fatal(err)
	}
	if called != "image.resize" {
		t.Fatalf("called handler type = %q", called)
	}
}

func TestHandlerMuxRejectsUnknownTaskType(t *testing.T) {
	mux := NewHandlerMux()
	err := mux.ProcessTask(context.Background(), &model.TaskMessage{Type: "unknown"})
	if !errors.Is(err, ErrUnknownTaskType) {
		t.Fatalf("unknown task error = %v", err)
	}
}
