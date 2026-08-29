package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	imageprocessing "go-taskengine/examples/image-processing"
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
	config, err := support.ParseWorkerConfig("image-worker", args)
	if err != nil {
		return err
	}
	rdb, err := support.ConnectRedis(config.RedisAddr)
	if err != nil {
		return err
	}
	defer rdb.Close()
	log.Printf("Redis connected at %s; starting image simulation worker (Ctrl+C to stop)", config.RedisAddr)

	metrics := server.NewMetrics()
	s := server.New(redisstore.New(rdb), imageprocessing.Handler{}, server.Config{
		Queues:  map[string]int{"image": 1},
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
	log.Printf("image worker %s", support.FormatMetrics(metrics.Snapshot()))
	return runErr
}
