package support

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go-taskengine/server"
)

func TestRedisAddressPrecedence(t *testing.T) {
	t.Setenv(RedisAddressEnv, "env:6379")
	if got := RedisAddress("flag:6379"); got != "flag:6379" {
		t.Fatalf("explicit address = %q", got)
	}
	if got := RedisAddress(""); got != "env:6379" {
		t.Fatalf("environment address = %q", got)
	}
	t.Setenv(RedisAddressEnv, "")
	if got := RedisAddress(""); got != DefaultRedisAddress {
		t.Fatalf("default address = %q", got)
	}
}

func TestOpenRedisChecksConnectivity(t *testing.T) {
	mini := miniredis.RunT(t)
	client, err := OpenRedis(context.Background(), mini.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRedisReturnsPingError(t *testing.T) {
	client, err := OpenRedis(context.Background(), "127.0.0.1:1")
	if err == nil {
		_ = client.Close()
		t.Fatal("OpenRedis accepted an unavailable address")
	}
}

func TestParseProducerConfigSupportsSimulationControls(t *testing.T) {
	config, err := ParseProducerConfig("image-producer", []string{
		"-redis-addr", "localhost:6380",
		"-delay", "150ms",
		"-duration", "275ms",
		"-timeout", "500ms",
		"-fail",
		"-max-retry", "2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.RedisAddr != "localhost:6380" || config.EnqueueDelay != 150*time.Millisecond || config.Duration != 275*time.Millisecond {
		t.Fatalf("unexpected producer timing config: %+v", config)
	}
	if config.Timeout != 500*time.Millisecond || !config.Fail || config.MaxRetry != 2 {
		t.Fatalf("unexpected producer simulation config: %+v", config)
	}
}

func TestParseProducerConfigRejectsNegativeValues(t *testing.T) {
	if _, err := ParseProducerConfig("producer", []string{"-delay", "-1ms"}); err == nil {
		t.Fatal("negative enqueue delay was accepted")
	}
}

func TestParseWorkerConfigSupportsRedisAddress(t *testing.T) {
	config, err := ParseWorkerConfig("image-worker", []string{"-redis-addr", "localhost:6380", "-run-for", "2s"})
	if err != nil {
		t.Fatal(err)
	}
	if config.RedisAddr != "localhost:6380" || config.RunFor != 2*time.Second {
		t.Fatalf("unexpected worker config: %+v", config)
	}
}

func TestParseWorkerConfigRejectsNegativeRunFor(t *testing.T) {
	if _, err := ParseWorkerConfig("worker", []string{"-run-for", "-1s"}); err == nil {
		t.Fatal("negative run-for was accepted")
	}
}

func TestFormatMetricsUsesActualSnapshotValues(t *testing.T) {
	metrics := server.NewMetrics()
	metrics.RecordProcessed()
	metrics.RecordFailed()
	metrics.RecordRetried()
	metrics.RecordArchived()
	metrics.RecordDuration(275 * time.Millisecond)

	output := FormatMetrics(metrics.Snapshot())
	for _, want := range []string{
		"processed=1",
		"failed=1",
		"retried=1",
		"archived=1",
		"total_duration=275ms",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("metrics output %q does not contain %q", output, want)
		}
	}
}
