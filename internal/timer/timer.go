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
	sequence  uint64
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].at.Before(h[j].at)
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *eventHeap) Push(x any)   { e := x.(*event); e.index = len(*h); *h = append(*h, e) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	e.index = -1
	*h = old[:n-1]
	return e
}

// TimeWheel is a local timer scheduler. Redis remains the source of truth for task durability.
// Callbacks run serially, so a slow callback delays later callbacks. A callback that has
// started cannot be cancelled; cancellation only prevents callbacks that are still queued.
type TimeWheel struct {
	mu       sync.Mutex
	events   eventHeap
	wake     chan struct{}
	now      func() time.Time
	sequence uint64
}

// New creates a timer using the system clock.
func New() *TimeWheel { return NewWithClock(time.Now) }

// NewWithClock creates a timer using now to determine whether an event is due.
// A nil clock falls back to the system clock.
func NewWithClock(now func() time.Time) *TimeWheel {
	if now == nil {
		now = time.Now
	}
	return &TimeWheel{wake: make(chan struct{}, 1), now: now}
}

// Schedule registers callback and returns a cancellation function.
func (w *TimeWheel) Schedule(at time.Time, callback func()) func() {
	w.mu.Lock()
	w.sequence++
	e := &event{at: at, callback: callback, index: -1, sequence: w.sequence}
	heap.Push(&w.events, e)
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

// Wake asks a running wheel to re-evaluate its next deadline. It is useful with
// an injected clock after the test or caller advances that clock.
func (w *TimeWheel) Wake() { w.signal() }

func (w *TimeWheel) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// Run executes due callbacks until ctx is canceled.
func (w *TimeWheel) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
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
		wait := next.at.Sub(w.now())
		w.mu.Unlock()
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-w.wake:
			stopTimer(timer)
			continue
		case <-timer.C:
			w.mu.Lock()
			if len(w.events) == 0 {
				w.mu.Unlock()
				continue
			}
			e := heap.Pop(&w.events).(*event)
			cancelled := e.cancelled
			callback := e.callback
			w.mu.Unlock()
			if !cancelled && callback != nil {
				callback()
			}
		}
	}
}
