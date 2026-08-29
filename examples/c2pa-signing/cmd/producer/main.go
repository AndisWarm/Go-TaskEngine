package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"go-taskengine/client"
	c2pasigning "go-taskengine/examples/c2pa-signing"
	"go-taskengine/examples/support"
	"go-taskengine/redisstore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	config, err := support.ParseProducerConfig("c2pa-producer", args)
	if err != nil {
		return err
	}
	rdb, err := support.ConnectRedis(config.RedisAddr)
	if err != nil {
		return err
	}
	defer rdb.Close()

	payload, err := json.Marshal(c2pasigning.Payload{
		Asset:      config.Input,
		DurationMS: int(config.Duration / time.Millisecond),
		Fail:       config.Fail,
		Invalid:    config.Invalid,
	})
	if err != nil {
		return fmt.Errorf("encode C2PA simulation payload: %w", err)
	}
	producer := client.NewClient(redisstore.New(rdb))
	ctx := context.Background()
	msg, err := producer.EnqueueIn(
		ctx,
		client.NewTask("c2pa:sign", payload),
		config.EnqueueDelay,
		client.WithQueue(config.Queue),
		client.WithMaxRetry(config.MaxRetry),
		client.WithTimeout(config.Timeout),
	)
	if err != nil {
		return err
	}
	log.Printf("Redis connected at %s; enqueued C2PA simulation task %s queue=%s delay=%s duration=%s fail=%t invalid=%t max_retry=%d timeout=%s", config.RedisAddr, msg.ID, msg.Queue, config.EnqueueDelay, config.Duration, config.Fail, config.Invalid, config.MaxRetry, config.Timeout)
	return nil
}
