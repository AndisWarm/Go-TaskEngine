package limiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testBucket(t *testing.T, capacity, rate float64) (*TokenBucket, *miniredis.Miniredis) {
	t.Helper()
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewTokenBucket(rdb, "bucket:test", capacity, rate), mini
}

func TestTokenBucketHonorsBurstCapacity(t *testing.T) {
	bucket, _ := testBucket(t, 2, 0)
	for i := 0; i < 2; i++ {
		result, err := bucket.Acquire(context.Background(), 1)
		if err != nil || !result.Allowed {
			t.Fatalf("acquire %d: %+v, %v", i, result, err)
		}
	}
	result, err := bucket.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Allowed || result.RetryAfter <= 0 {
		t.Fatalf("third acquire: %+v", result)
	}
}

func TestTokenBucketRefills(t *testing.T) {
	bucket, _ := testBucket(t, 1, 20)
	if result, err := bucket.Acquire(context.Background(), 1); err != nil || !result.Allowed {
		t.Fatalf("initial: %+v %v", result, err)
	}
	time.Sleep(70 * time.Millisecond)
	result, err := bucket.Acquire(context.Background(), 1)
	if err != nil || !result.Allowed {
		t.Fatalf("refill: %+v %v", result, err)
	}
}

func TestTokenBucketIsAtomicUnderConcurrency(t *testing.T) {
	bucket, _ := testBucket(t, 3, 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := bucket.Acquire(context.Background(), 1)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if result.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 3 {
		t.Fatalf("allowed=%d, want 3", allowed)
	}
}
