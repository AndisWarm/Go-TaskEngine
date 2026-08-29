package model

import (
	"testing"
	"time"
)

func TestPublicTaskMessageCanBeValidated(t *testing.T) {
	msg := &TaskMessage{
		ID: "public-1", Type: "demo", Queue: "default", MaxRetry: 1,
		RunAt: time.UnixMilli(1000), CreatedAt: time.UnixMilli(1000),
	}
	if err := msg.Validate(); err != nil {
		t.Fatalf("public task message should validate: %v", err)
	}
}

func TestTaskMessageValidationRejectsNegativeTimeout(t *testing.T) {
	msg := &TaskMessage{ID: "bad-timeout", Type: "demo", Queue: "default", Timeout: -time.Second}
	if err := msg.Validate(); err == nil {
		t.Fatal("negative timeout was accepted")
	}
}
