package c2pasigning

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-taskengine/server"
)

// Payload describes a deterministic C2PA signing simulation.
type Payload struct {
	Asset      string `json:"asset"`
	DurationMS int    `json:"duration_ms"`
	Fail       bool   `json:"fail"`
	Invalid    bool   `json:"invalid"`
}

// Handler simulates manifest creation and signing without a real C2PA library.
type Handler struct{}

func (Handler) ProcessTask(ctx context.Context, msg *server.TaskMessage) error {
	var payload Payload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("decode c2pa payload: %w: %w", err, server.ErrNonRetryable)
	}
	if payload.Asset == "" || payload.Invalid {
		return fmt.Errorf("invalid asset for c2pa signing: %w", server.ErrNonRetryable)
	}
	timer := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload.Fail {
		return fmt.Errorf("simulated signing service failure")
	}
	return nil
}
