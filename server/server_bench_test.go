package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	"go-taskengine/internal/redisstore"
)

func BenchmarkClientEnqueue(b *testing.B) {
	mini := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	producer := client.NewClient(redisstore.New(rdb))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := producer.Enqueue(context.Background(), client.NewTask("benchmark", []byte("payload")), client.WithTaskID(fmt.Sprintf("bench-%d", i)))
		if err != nil { b.Fatal(err) }
	}
}
