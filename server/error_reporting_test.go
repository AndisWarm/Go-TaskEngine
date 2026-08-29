package server

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/redisstore"
)

func TestServerReportsRedisDispatchErrors(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	store := redisstore.New(rdb)
	errorsSeen := make(chan error, 1)
	s := New(store, HandlerFunc(func(context.Context, *TaskMessage) error { return nil }), Config{
		PollInterval: time.Millisecond,
		ErrorHandler: func(err error) {
			select {
			case errorsSeen <- err:
			default:
			}
		},
	})
	producer := client.NewClient(store)
	if _, err := producer.Enqueue(context.Background(), client.NewTask("error-test", nil), client.WithTaskID("error-test-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	_ = rdb.Close()
	select {
	case err := <-errorsSeen:
		if err == nil {
			t.Fatal("server reported a nil Redis error")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not report Redis dispatch error")
	}
	_ = s.Shutdown()
}
