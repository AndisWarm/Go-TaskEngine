package server_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go-taskengine/client"
	c2pasigning "go-taskengine/examples/c2pa-signing"
	imageprocessing "go-taskengine/examples/image-processing"
	"go-taskengine/internal/redisstore"
	"go-taskengine/server"
)

func TestExampleHandlersProcessSimulatedPayloads(t *testing.T) {
	imagePayload, _ := json.Marshal(imageprocessing.Payload{Source: "input.jpg", DurationMS: 1})
	if err := (imageprocessing.Handler{}).ProcessTask(context.Background(), &server.TaskMessage{Payload: imagePayload}); err != nil {
		t.Fatal(err)
	}
	c2paPayload, _ := json.Marshal(c2pasigning.Payload{Asset: "input.jpg", DurationMS: 1})
	if err := (c2pasigning.Handler{}).ProcessTask(context.Background(), &server.TaskMessage{Payload: c2paPayload}); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsSnapshotTracksTaskOutcomes(t *testing.T) {
	metrics := server.NewMetrics()
	metrics.RecordProcessed()
	metrics.RecordFailed()
	metrics.RecordRetried()
	metrics.RecordArchived()
	snapshot := metrics.Snapshot()
	if snapshot.Processed != 1 || snapshot.Failed != 1 || snapshot.Retried != 1 || snapshot.Archived != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestMetricsAreUpdatedByWorkerPath(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	store := redisstore.New(rdb)
	metrics := server.NewMetrics()
	s := server.New(store, server.HandlerFunc(func(context.Context, *server.TaskMessage) error { return nil }), server.Config{PollInterval: time.Millisecond, Metrics: metrics})
	producer := client.NewClient(store)
	if _, err := producer.Enqueue(context.Background(), client.NewTask("metric", nil), client.WithTaskID("metric-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for metrics.Snapshot().Processed == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Snapshot().Processed; got != 1 {
		t.Fatalf("processed metric=%d", got)
	}
}
