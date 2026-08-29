package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"go-taskengine/client"
	imageprocessing "go-taskengine/examples/image-processing"
	"go-taskengine/examples/support"
	"go-taskengine/redisstore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	config, err := support.ParseProducerConfig("image-producer", args)
	if err != nil {
		return err
	}
	rdb, err := support.ConnectRedis(config.RedisAddr)
	if err != nil {
		return err
	}
	defer rdb.Close()

	payload, err := json.Marshal(imageprocessing.Payload{
		Source:     config.Input,
		DurationMS: int(config.Duration / time.Millisecond),
		Fail:       config.Fail,
	})
	if err != nil {
		return fmt.Errorf("encode image simulation payload: %w", err)
	}
	producer := client.NewClient(redisstore.New(rdb))
	ctx := context.Background()
	msg, err := producer.EnqueueIn(
		ctx,
		client.NewTask("image:process", payload),
		config.EnqueueDelay,
		client.WithQueue(config.Queue),
		client.WithMaxRetry(config.MaxRetry),
		client.WithTimeout(config.Timeout),
	)
	if err != nil {
		return err
	}
	log.Printf("Redis connected at %s; enqueued image simulation task %s queue=%s delay=%s duration=%s fail=%t max_retry=%d timeout=%s", config.RedisAddr, msg.ID, msg.Queue, config.EnqueueDelay, config.Duration, config.Fail, config.MaxRetry, config.Timeout)
	return nil
}
