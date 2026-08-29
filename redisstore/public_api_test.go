package redisstore_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/model"
	"go-taskengine/redisstore"
)

func TestPublicRedisStoreAcceptsPublicTaskModel(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()

	store := redisstore.New(rdb)
	msg := &model.TaskMessage{ID: "public-store-1", Type: "demo", Queue: "default", MaxRetry: 1}
	if err := store.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("public Redis store enqueue failed: %v", err)
	}
}
