package timer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTimeWheelRunsDueCallback(t *testing.T) {
	wheel := New()
	var ran chan struct{} = make(chan struct{})
	at := time.Now().Add(20 * time.Millisecond)
	wheel.Schedule(at, func() { close(ran) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wheel.Run(ctx)
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("timer callback did not run")
	}
}

func TestTimeWheelCancelPreventsCallback(t *testing.T) {
	wheel := New()
	var mu sync.Mutex
	ran := false
	cancel := wheel.Schedule(time.Now().Add(20*time.Millisecond), func() { mu.Lock(); ran = true; mu.Unlock() })
	cancel()
	ctx, stop := context.WithCancel(context.Background())
	go wheel.Run(ctx)
	time.Sleep(60 * time.Millisecond)
	stop()
	mu.Lock()
	defer mu.Unlock()
	if ran {
		t.Fatal("cancelled callback ran")
	}
}
