package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	c2pasigning "go-taskengine/examples/c2pa-signing"
	"go-taskengine/examples/support"
	"go-taskengine/redisstore"
	"go-taskengine/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	config, err := support.ParseWorkerConfig("c2pa-worker", args)
	if err != nil {
		return err
	}
	rdb, err := support.ConnectRedis(config.RedisAddr)
	if err != nil {
		return err
	}
	defer rdb.Close()
	log.Printf("Redis connected at %s; starting C2PA simulation worker (Ctrl+C to stop)", config.RedisAddr)

	metrics := server.NewMetrics()
	s := server.New(redisstore.New(rdb), c2pasigning.Handler{}, server.Config{
		Queues:  map[string]int{"c2pa": 1},
		Metrics: metrics,
		ErrorHandler: func(err error) {
			log.Printf("server error: %v", err)
		},
	})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	ctx := context.Background()
	if config.RunFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.RunFor)
		defer cancel()
	}
	runErr := s.RunSignals(ctx, signals)
	log.Printf("C2PA worker %s", support.FormatMetrics(metrics.Snapshot()))
	return runErr
}
