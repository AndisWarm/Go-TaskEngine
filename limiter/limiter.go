// Package limiter exposes the distributed Redis token bucket used by the server.
package limiter

import internal "go-taskengine/internal/limiter"

// Result describes one token acquisition attempt.
type Result = internal.Result

// TokenBucket is a Redis-backed, atomically updated token bucket.
type TokenBucket = internal.TokenBucket

var (
	ErrInvalidConfig     = internal.ErrInvalidConfig
	NewTokenBucket       = internal.NewTokenBucket
	NewScopedTokenBucket = internal.NewScopedTokenBucket
	ScopeKey             = internal.ScopeKey
)
