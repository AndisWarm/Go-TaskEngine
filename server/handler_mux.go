package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrUnknownTaskType = errors.New("unknown task type")
	ErrHandlerExists   = errors.New("task handler already registered")
)

// HandlerMux routes tasks to handlers by TaskMessage.Type.
type HandlerMux struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewHandlerMux creates an empty task-type handler registry.
func NewHandlerMux() *HandlerMux {
	return &HandlerMux{handlers: make(map[string]Handler)}
}

// Handle registers one handler for a non-empty task type.
func (m *HandlerMux) Handle(taskType string, handler Handler) error {
	if m == nil {
		return errors.New("handler mux is nil")
	}
	if taskType == "" {
		return errors.New("task type cannot be empty")
	}
	if handler == nil {
		return errors.New("task handler cannot be nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handlers == nil {
		m.handlers = make(map[string]Handler)
	}
	if _, exists := m.handlers[taskType]; exists {
		return fmt.Errorf("%w: %s", ErrHandlerExists, taskType)
	}
	m.handlers[taskType] = handler
	return nil
}

// ProcessTask dispatches a task to the handler registered for its type.
func (m *HandlerMux) ProcessTask(ctx context.Context, msg *TaskMessage) error {
	if m == nil {
		return ErrUnknownTaskType
	}
	m.mu.RLock()
	handler := m.handlers[msg.Type]
	m.mu.RUnlock()
	if handler == nil {
		return fmt.Errorf("%w: %s", ErrUnknownTaskType, msg.Type)
	}
	return handler.ProcessTask(ctx, msg)
}
