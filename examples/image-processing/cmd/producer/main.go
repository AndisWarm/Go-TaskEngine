package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	imageprocessing "go-taskengine/examples/image-processing"
	"go-taskengine/internal/redisstore"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()
	store := redisstore.New(rdb)
	producer := client.NewClient(store)
	payload, _ := json.Marshal(imageprocessing.Payload{Source: "input.jpg", DurationMS: 250})
	msg, err := producer.Enqueue(context.Background(), client.NewTask("image:process", payload), client.WithQueue("image"), client.WithMaxRetry(3))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued image task %s", msg.ID)
}
