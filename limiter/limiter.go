// Package limiter exposes the rate-limit contract and Redis token-bucket implementation.
package limiter

import (
	"context"

	internal "go-taskengine/internal/limiter"
)

// Result describes one token acquisition attempt.
type Result = internal.Result

// Limiter is the rate-limit contract required by the task server.
type Limiter interface {
	Acquire(context.Context, float64) (Result, error)
	Validate() error
	Capacity() float64
}

// TokenBucket is a Redis-backed, atomically updated token bucket.
type TokenBucket = internal.TokenBucket

var _ Limiter = (*TokenBucket)(nil)

var (
	ErrInvalidConfig     = internal.ErrInvalidConfig
	NewTokenBucket       = internal.NewTokenBucket
	NewScopedTokenBucket = internal.NewScopedTokenBucket
	ScopeKey             = internal.ScopeKey
)
