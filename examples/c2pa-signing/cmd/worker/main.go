package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	c2pasigning "go-taskengine/examples/c2pa-signing"
	"go-taskengine/redisstore"
	"go-taskengine/server"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()
	s := server.New(redisstore.New(rdb), c2pasigning.Handler{}, server.Config{Queues: map[string]int{"c2pa": 1}})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	if err := s.RunSignals(context.Background(), signals); err != nil {
		log.Print(err)
	}
}
