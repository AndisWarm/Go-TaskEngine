package imageprocessing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-taskengine/server"
)

// Payload describes a deterministic image-processing simulation.
type Payload struct {
	Source     string `json:"source"`
	DurationMS int    `json:"duration_ms"`
	Fail       bool   `json:"fail"`
}

// Handler simulates resizing or transcoding an image without external services.
type Handler struct{}

func (Handler) ProcessTask(ctx context.Context, msg *server.TaskMessage) error {
	var payload Payload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("decode image payload: %w: %w", err, server.ErrNonRetryable)
	}
	if payload.Source == "" {
		return fmt.Errorf("image source is empty: %w", server.ErrNonRetryable)
	}
	if payload.DurationMS < 0 {
		return fmt.Errorf("image duration is negative: %w", server.ErrNonRetryable)
	}
	timer := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload.Fail {
		return fmt.Errorf("simulated image processor failure")
	}
	return nil
}
