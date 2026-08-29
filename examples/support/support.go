// Package support contains shared configuration and output helpers for the simulated examples.
package support

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/server"
)

const (
	DefaultRedisAddress = "127.0.0.1:6379"
	RedisAddressEnv     = "TASKENGINE_REDIS_ADDR"
	RedisPingTimeout    = 5 * time.Second
)

// RedisAddress resolves an explicit flag value, the environment, and the default address.
func RedisAddress(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if address := os.Getenv(RedisAddressEnv); address != "" {
		return address
	}
	return DefaultRedisAddress
}

// OpenRedis creates a client and verifies connectivity before returning it.
func OpenRedis(ctx context.Context, explicit string) (*redis.Client, error) {
	address := RedisAddress(explicit)
	client := redis.NewClient(&redis.Options{Addr: address})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping Redis at %s: %w", address, err)
	}
	return client, nil
}

// ConnectRedis verifies the configured Redis endpoint with a bounded timeout.
func ConnectRedis(explicit string) (*redis.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RedisPingTimeout)
	defer cancel()
	return OpenRedis(ctx, explicit)
}

// ProducerConfig contains the shared command-line controls for both simulations.
type ProducerConfig struct {
	RedisAddr    string
	Queue        string
	Input        string
	EnqueueDelay time.Duration
	Duration     time.Duration
	Timeout      time.Duration
	MaxRetry     int
	Fail         bool
	Invalid      bool
}

// ParseProducerConfig parses the common producer flags used by both examples.
func ParseProducerConfig(command string, args []string) (ProducerConfig, error) {
	config := ProducerConfig{
		RedisAddr: RedisAddress(""),
		Queue:     "default",
		Input:     "input.jpg",
		Duration:  300 * time.Millisecond,
		MaxRetry:  3,
	}
	if strings.Contains(command, "image") {
		config.Queue = "image"
	} else if strings.Contains(command, "c2pa") {
		config.Queue = "c2pa"
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&config.RedisAddr, "redis-addr", config.RedisAddr, "Redis address (or TASKENGINE_REDIS_ADDR)")
	flags.StringVar(&config.Queue, "queue", config.Queue, "task queue")
	flags.StringVar(&config.Input, "input", config.Input, "simulated input name")
	flags.DurationVar(&config.EnqueueDelay, "delay", 0, "delay before the task becomes ready")
	flags.DurationVar(&config.Duration, "duration", config.Duration, "simulated handler duration")
	flags.DurationVar(&config.Timeout, "timeout", 0, "handler timeout")
	flags.IntVar(&config.MaxRetry, "max-retry", config.MaxRetry, "maximum retries")
	flags.BoolVar(&config.Fail, "fail", false, "simulate a retryable failure")
	flags.BoolVar(&config.Invalid, "invalid", false, "simulate a non-retryable input error")
	if err := flags.Parse(args); err != nil {
		return ProducerConfig{}, err
	}
	if config.Queue == "" {
		return ProducerConfig{}, fmt.Errorf("queue cannot be empty")
	}
	if config.Input == "" {
		return ProducerConfig{}, fmt.Errorf("input cannot be empty")
	}
	if config.EnqueueDelay < 0 {
		return ProducerConfig{}, fmt.Errorf("delay cannot be negative: %s", config.EnqueueDelay)
	}
	if config.Duration < 0 {
		return ProducerConfig{}, fmt.Errorf("duration cannot be negative: %s", config.Duration)
	}
	if config.Timeout < 0 {
		return ProducerConfig{}, fmt.Errorf("timeout cannot be negative: %s", config.Timeout)
	}
	if config.MaxRetry < 0 {
		return ProducerConfig{}, fmt.Errorf("max-retry cannot be negative: %d", config.MaxRetry)
	}
	return config, nil
}

// WorkerConfig contains the shared command-line controls for both workers.
type WorkerConfig struct {
	RedisAddr string
	RunFor    time.Duration
}

// ParseWorkerConfig parses the common worker flags used by both examples.
func ParseWorkerConfig(command string, args []string) (WorkerConfig, error) {
	config := WorkerConfig{RedisAddr: RedisAddress("")}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&config.RedisAddr, "redis-addr", config.RedisAddr, "Redis address (or TASKENGINE_REDIS_ADDR)")
	flags.DurationVar(&config.RunFor, "run-for", 0, "stop automatically after this duration; useful for demonstrations")
	if err := flags.Parse(args); err != nil {
		return WorkerConfig{}, err
	}
	if config.RunFor < 0 {
		return WorkerConfig{}, fmt.Errorf("run-for cannot be negative: %s", config.RunFor)
	}
	return config, nil
}

// FormatMetrics renders the counters collected by the actual server worker path.
func FormatMetrics(snapshot server.MetricsSnapshot) string {
	return fmt.Sprintf(
		"metrics processed=%d failed=%d retried=%d archived=%d total_duration=%s",
		snapshot.Processed,
		snapshot.Failed,
		snapshot.Retried,
		snapshot.Archived,
		snapshot.TotalDuration,
	)
}
