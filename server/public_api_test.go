package server_test

import (
	"testing"

	"go-taskengine/limiter"
	"go-taskengine/server"
)

func TestPublicLimiterCanConfigureServer(t *testing.T) {
	bucket := limiter.NewScopedTokenBucket(nil, "public", 1, 1)
	_ = server.Config{TokenBucket: bucket}
}
