package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	c2pasigning "go-taskengine/examples/c2pa-signing"
	"go-taskengine/redisstore"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()
	producer := client.NewClient(redisstore.New(rdb))
	payload, _ := json.Marshal(c2pasigning.Payload{Asset: "input.jpg", DurationMS: 300})
	msg, err := producer.Enqueue(context.Background(), client.NewTask("c2pa:sign", payload), client.WithQueue("c2pa"), client.WithMaxRetry(3))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued C2PA simulation task %s", msg.ID)
}
