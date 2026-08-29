package server

import (
	"sync/atomic"
	"time"
)

// Metrics stores counters that are updated by the actual worker path.
type Metrics struct {
	processed     atomic.Int64
	failed        atomic.Int64
	retried       atomic.Int64
	archived      atomic.Int64
	durationNanos atomic.Int64
}

type MetricsSnapshot struct {
	Processed     int64
	Failed        int64
	Retried       int64
	Archived      int64
	TotalDuration time.Duration
}

func NewMetrics() *Metrics { return new(Metrics) }
func (m *Metrics) RecordProcessed() {
	if m != nil {
		m.processed.Add(1)
	}
}
func (m *Metrics) RecordFailed() {
	if m != nil {
		m.failed.Add(1)
	}
}
func (m *Metrics) RecordRetried() {
	if m != nil {
		m.retried.Add(1)
	}
}
func (m *Metrics) RecordArchived() {
	if m != nil {
		m.archived.Add(1)
	}
}
func (m *Metrics) RecordDuration(d time.Duration) {
	if m != nil {
		m.durationNanos.Add(d.Nanoseconds())
	}
}
func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{Processed: m.processed.Load(), Failed: m.failed.Load(), Retried: m.retried.Load(), Archived: m.archived.Load(), TotalDuration: time.Duration(m.durationNanos.Load())}
}
