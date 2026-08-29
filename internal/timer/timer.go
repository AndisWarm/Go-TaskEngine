package timer

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type event struct {
	at        time.Time
	callback  func()
	cancelled bool
	index     int
}

type eventHeap []*event

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *eventHeap) Push(x any)        { e := x.(*event); e.index = len(*h); *h = append(*h, e) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	e.index = -1
	*h = old[:n-1]
	return e
}

// TimeWheel is a small local timer scheduler. Redis remains the source of truth for task durability.
type TimeWheel struct {
	mu     sync.Mutex
	events eventHeap
	wake   chan struct{}
}

func New() *TimeWheel { return &TimeWheel{wake: make(chan struct{}, 1)} }

// Schedule registers callback and returns a cancellation function.
func (w *TimeWheel) Schedule(at time.Time, callback func()) func() {
	e := &event{at: at, callback: callback}
	w.mu.Lock()
	heap.Push(&w.events, e)
	heap.Init(&w.events)
	w.mu.Unlock()
	w.signal()
	return func() {
		w.mu.Lock()
		if e.index >= 0 {
			e.cancelled = true
		}
		w.mu.Unlock()
		w.signal()
	}
}

func (w *TimeWheel) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run executes due callbacks until ctx is canceled.
func (w *TimeWheel) Run(ctx context.Context) {
	for {
		w.mu.Lock()
		for len(w.events) > 0 && w.events[0].cancelled {
			heap.Pop(&w.events)
		}
		if len(w.events) == 0 {
			w.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
				continue
			}
		}
		next := w.events[0]
		wait := time.Until(next.at)
		w.mu.Unlock()
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-w.wake:
			if !timer.Stop() {
				<-timer.C
			}
			continue
		case <-timer.C:
			w.mu.Lock()
			if len(w.events) == 0 {
				w.mu.Unlock()
				continue
			}
			e := heap.Pop(&w.events).(*event)
			w.mu.Unlock()
			if !e.cancelled && e.callback != nil {
				e.callback()
			}
		}
	}
}
