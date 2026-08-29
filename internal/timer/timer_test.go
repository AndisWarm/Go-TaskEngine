package timer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimeWheelUsesInjectedClock(t *testing.T) {
	current := time.Date(2026, time.August, 29, 20, 0, 0, 0, time.UTC)
	currentNanos := atomic.Int64{}
	currentNanos.Store(current.UnixNano())
	wheel := NewWithClock(func() time.Time { return time.Unix(0, currentNanos.Load()) })
	var ran atomic.Int32
	wheel.Schedule(current.Add(time.Second), func() { ran.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wheel.Run(ctx)
	time.Sleep(10 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatal("callback ran before injected clock reached its deadline")
	}
	currentNanos.Store(current.Add(2 * time.Second).UnixNano())
	wheel.Schedule(current, func() {})
	waitForTimer(t, time.Second, func() bool { return ran.Load() == 1 })
}

func TestTimeWheelSupportsConcurrentScheduleAndCancel(t *testing.T) {
	wheel := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wheel.Run(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stop := wheel.Schedule(time.Now().Add(time.Duration(i+20)*time.Millisecond), func() {})
			if i%2 == 0 {
				stop()
			}
		}(i)
	}
	wg.Wait()
}

func TestTimeWheelRunsSlowCallbacksSerially(t *testing.T) {
	wheel := New()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var secondRan atomic.Bool
	at := time.Now().Add(10 * time.Millisecond)
	wheel.Schedule(at, func() {
		close(firstStarted)
		<-releaseFirst
	})
	wheel.Schedule(at, func() { secondRan.Store(true) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wheel.Run(ctx)
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}
	time.Sleep(30 * time.Millisecond)
	if secondRan.Load() {
		t.Fatal("second callback ran while first callback was still running")
	}
	close(releaseFirst)
	waitForTimer(t, time.Second, secondRan.Load)
}

func waitForTimer(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

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
