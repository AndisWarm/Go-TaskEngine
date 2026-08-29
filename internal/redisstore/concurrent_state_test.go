package redisstore

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrentMoveReadyMovesEachTaskOnce(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1000)
	msg := message("move-once", 1, now)
	msg.State = "scheduled"
	if err := store.Schedule(ctx, msg); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			moved, err := store.MoveReady(ctx, now, 10, "default")
			if err != nil {
				t.Errorf("move ready: %v", err)
				return
			}
			results <- moved
		}()
	}
	wg.Wait()
	close(results)

	total := 0
	for moved := range results {
		total += moved
	}
	if total != 1 {
		t.Fatalf("concurrent moves = %d, want 1", total)
	}
	claimed, err := store.Claim(ctx, "default", now, time.Second)
	if err != nil || claimed == nil || claimed.ID != msg.ID {
		t.Fatalf("claim after concurrent move = %+v, err=%v", claimed, err)
	}
}
