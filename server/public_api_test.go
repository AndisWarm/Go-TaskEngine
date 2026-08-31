package server_test

import (
	"context"
	"testing"

	"go-taskengine/limiter"
	"go-taskengine/server"
)

type customLimiter struct{}

func (customLimiter) Acquire(context.Context, float64) (limiter.Result, error) {
	return limiter.Result{Allowed: true}, nil
}
func (customLimiter) Validate() error   { return nil }
func (customLimiter) Capacity() float64 { return 1 }

func TestPublicLimiterCanConfigureServer(t *testing.T) {
	bucket := limiter.NewScopedTokenBucket(nil, "public", 1, 1)
	_ = server.Config{TokenBucket: bucket}
}

func TestCustomLimiterCanConfigureServer(t *testing.T) {
	_ = server.Config{TokenBucket: customLimiter{}}
}
