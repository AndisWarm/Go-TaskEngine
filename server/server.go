package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"go-taskengine/internal/timer"
	"go-taskengine/limiter"
	"go-taskengine/model"
	"go-taskengine/redisstore"
	"go-taskengine/storage"
)

// TaskMessage is the task envelope delivered to handlers.
type TaskMessage = model.TaskMessage

// Handler processes one task. Returning an error schedules a retry or dead-letters the task.
type Handler interface {
	ProcessTask(context.Context, *TaskMessage) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *TaskMessage) error

func (f HandlerFunc) ProcessTask(ctx context.Context, msg *TaskMessage) error { return f(ctx, msg) }

// ErrNonRetryable marks a failure that must go directly to the dead-letter archive.
var ErrNonRetryable = errors.New("non-retryable task failure")
var ErrServerClosed = errors.New("server is closed")
var ErrServerRunning = errors.New("server is already running")
var ErrInvalidConfig = errors.New("invalid server configuration")

// Config controls the server worker pool and lifecycle.
type Config struct {
	Concurrency       int
	Queues            map[string]int
	LeaseDuration     time.Duration
	PollInterval      time.Duration
	ShutdownTimeout   time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	RetryJitter       float64
	HeartbeatInterval time.Duration
	RecoveryInterval  time.Duration
	TokenBucket       *limiter.TokenBucket
	TokenAmount       float64
	Metrics           *Metrics
	// ErrorHandler receives storage and lifecycle errors that the server cannot recover from immediately.
	ErrorHandler func(error)
}

func (c *Config) applyDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 20 * time.Millisecond
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 8 * time.Second
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = 2 * time.Second
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = time.Hour
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.RecoveryInterval <= 0 {
		c.RecoveryInterval = time.Second
	}
	if c.TokenAmount <= 0 {
		c.TokenAmount = 1
	}
	if len(c.Queues) == 0 {
		c.Queues = map[string]int{"default": 1}
	}
}

func (c *Config) validate() error {
	if c.HeartbeatInterval >= c.LeaseDuration {
		return fmt.Errorf("%w: heartbeat interval %s must be less than lease duration %s", ErrInvalidConfig, c.HeartbeatInterval, c.LeaseDuration)
	}
	if c.RetryJitter < 0 || c.RetryJitter > 1 {
		return fmt.Errorf("%w: retry jitter must be between 0 and 1: %f", ErrInvalidConfig, c.RetryJitter)
	}
	if c.TokenBucket != nil {
		if err := c.TokenBucket.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		if math.IsNaN(c.TokenAmount) || math.IsInf(c.TokenAmount, 0) || c.TokenAmount > c.TokenBucket.Capacity() {
			return fmt.Errorf("%w: token amount must be finite and no greater than bucket capacity", ErrInvalidConfig)
		}
	}
	return nil
}

// Server runs a fixed-size worker pool over a Redis-backed queue.
type Server struct {
	store   storage.TaskStore
	handler Handler
	cfg     Config
	queues  []string

	mu           sync.Mutex
	state        serverState
	stopCh       chan struct{}
	stopOne      sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
	wheel        *timer.TimeWheel
	dispatchWake chan struct{}
	wg           sync.WaitGroup
	active       map[string]*model.TaskMessage
}

type serverState uint8

const (
	stateNew serverState = iota
	stateRunning
	stateStopped
)

func New(store storage.TaskStore, handler Handler, cfg Config) *Server {
	cfg.applyDefaults()
	queues := make([]string, 0, len(cfg.Queues))
	for queue, priority := range cfg.Queues {
		if priority > 0 {
			queues = append(queues, queue)
		}
	}
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	sort.SliceStable(queues, func(i, j int) bool { return cfg.Queues[queues[i]] > cfg.Queues[queues[j]] })
	return &Server{store: store, handler: handler, cfg: cfg, queues: queues, state: stateNew, active: make(map[string]*model.TaskMessage)}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateRunning {
		return ErrServerRunning
	}
	if s.state == stateStopped {
		return ErrServerClosed
	}
	if err := s.cfg.validate(); err != nil {
		return err
	}
	if s.store == nil || s.handler == nil {
		return errors.New("server store and handler are required")
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.stopCh = make(chan struct{})
	s.dispatchWake = make(chan struct{}, 1)
	s.wheel = timer.New()
	s.state = stateRunning
	jobs := make(chan *model.TaskMessage)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.wheel.Run(s.ctx)
	}()
	s.wg.Add(1)
	go s.dispatch(jobs)
	for i := 0; i < s.cfg.Concurrency; i++ {
		s.wg.Add(1)
		go s.worker(jobs)
	}
	s.wg.Add(2)
	go s.heartbeatLoop()
	go s.recoveryLoop()
	return nil
}

func (s *Server) reportError(err error) {
	if err != nil && s.cfg.ErrorHandler != nil {
		s.cfg.ErrorHandler(err)
	}
}

func (s *Server) dispatch(jobs chan<- *model.TaskMessage) {
	defer s.wg.Done()
	defer close(jobs)
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		if _, err := s.store.MoveReady(s.ctx, time.Now(), 100, s.queues...); err != nil {
			s.reportError(err)
			if !s.waitOrStop(s.cfg.PollInterval) {
				return
			}
			continue
		}
		hasPending := false
		for _, queue := range s.queues {
			pending, err := s.store.PendingCount(s.ctx, queue)
			if err != nil {
				s.reportError(err)
				continue
			}
			if pending > 0 {
				hasPending = true
				break
			}
		}
		if !hasPending {
			if !s.waitOrStop(s.cfg.PollInterval) {
				return
			}
			continue
		}
		if s.cfg.TokenBucket != nil {
			result, err := s.cfg.TokenBucket.Acquire(s.ctx, s.cfg.TokenAmount)
			if err != nil {
				s.reportError(err)
				if !s.waitOrStop(s.cfg.PollInterval) {
					return
				}
				continue
			}
			if !result.Allowed {
				if !s.waitOrStop(result.RetryAfter) {
					return
				}
				continue
			}
		}
		claimed := false
		for _, queue := range s.queues {
			msg, err := s.store.Claim(s.ctx, queue, time.Now(), s.cfg.LeaseDuration)
			if errors.Is(err, redisstore.ErrNoTask) {
				continue
			}
			if err != nil {
				s.reportError(err)
				continue
			}
			claimed = true
			select {
			case jobs <- msg:
			case <-s.stopCh:
				if err := s.store.Requeue(context.Background(), msg); err != nil {
					s.reportError(err)
				}
				return
			}
			break
		}
		if claimed {
			continue
		}
		if !s.waitOrStop(s.cfg.PollInterval) {
			return
		}
	}
}

func (s *Server) wakeDispatcher() {
	select {
	case s.dispatchWake <- struct{}{}:
	default:
	}
}

func (s *Server) waitOrStop(duration time.Duration) bool {
	if duration <= 0 {
		duration = time.Millisecond
	}
	cancel := s.wheel.Schedule(time.Now().Add(duration), s.wakeDispatcher)
	defer cancel()
	select {
	case <-s.dispatchWake:
		return true
	case <-s.stopCh:
		return false
	}
}

func (s *Server) worker(jobs <-chan *model.TaskMessage) {
	defer s.wg.Done()
	for msg := range jobs {
		s.trackActive(msg)
		s.process(msg)
		s.untrackActive(msg.ID)
	}
}

func (s *Server) process(msg *model.TaskMessage) {
	started := time.Now()
	defer func() {
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.RecordDuration(time.Since(started))
		}
	}()
	ctx := s.ctx
	cancel := func() {}
	if !msg.Deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, msg.Deadline)
	} else if msg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, msg.Timeout)
	}
	defer cancel()
	err := s.handler.ProcessTask(ctx, msg)
	if err == nil {
		if ackErr := s.store.AckSuccess(context.Background(), msg); ackErr != nil {
			s.reportError(ackErr)
			return
		}
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.RecordProcessed()
		}
		return
	}
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordFailed()
	}
	reason := err.Error()
	if errors.Is(err, ErrNonRetryable) || msg.RetryCount >= msg.MaxRetry {
		if archiveErr := s.store.Archive(context.Background(), msg, reason); archiveErr != nil {
			s.reportError(archiveErr)
			return
		}
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.RecordArchived()
		}
		return
	}
	at := time.Now().Add(ExponentialBackoffWithJitter(msg.RetryCount, s.cfg.RetryBaseDelay, s.cfg.RetryMaxDelay, s.cfg.RetryJitter))
	msg.RetryCount++
	if retryErr := s.store.ScheduleRetry(context.Background(), msg, at, reason); retryErr != nil {
		s.reportError(retryErr)
		return
	}
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordRetried()
	}
}

func (s *Server) heartbeatLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.Lock()
			byQueue := make(map[string][]string)
			for id, msg := range s.active {
				byQueue[msg.Queue] = append(byQueue[msg.Queue], id)
			}
			s.mu.Unlock()
			for queue, ids := range byQueue {
				if err := s.store.ExtendLease(context.Background(), queue, ids, now, s.cfg.LeaseDuration); err != nil {
					s.reportError(err)
				}
			}
		}
	}
}

func (s *Server) recoveryLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			for _, queue := range s.queues {
				ids, err := s.store.ExpiredIDs(context.Background(), now, queue, 100)
				if err != nil {
					s.reportError(err)
					continue
				}
				for _, id := range ids {
					msg, err := s.store.Get(context.Background(), queue, id)
					if err != nil {
						s.reportError(err)
						continue
					}
					if msg.State != model.StateActive {
						continue
					}
					if msg.RetryCount >= msg.MaxRetry {
						if err := s.store.Archive(context.Background(), msg, "task lease expired"); err != nil {
							s.reportError(err)
						} else if s.cfg.Metrics != nil {
							s.cfg.Metrics.RecordArchived()
						}
						continue
					}
					msg.RetryCount++
					if err := s.store.ScheduleRetry(context.Background(), msg, now.Add(ExponentialBackoffWithJitter(msg.RetryCount-1, s.cfg.RetryBaseDelay, s.cfg.RetryMaxDelay, s.cfg.RetryJitter)), "task lease expired"); err != nil {
						s.reportError(err)
					} else if s.cfg.Metrics != nil {
						s.cfg.Metrics.RecordRetried()
					}
				}
			}
		}
	}
}

func ExponentialBackoff(retryCount int, base, max time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	if base <= 0 {
		base = 2 * time.Second
	}
	if max <= 0 {
		max = time.Hour
	}
	result := base
	for i := 0; i < retryCount && result < max; i++ {
		if result > max/2 {
			return max
		}
		result *= 2
	}
	if result > max {
		return max
	}
	return result
}

func ExponentialBackoffWithJitter(retryCount int, base, max time.Duration, jitter float64) time.Duration {
	delay := ExponentialBackoff(retryCount, base, max)
	if jitter <= 0 {
		return delay
	}
	if jitter > 1 {
		jitter = 1
	}
	lower := float64(delay) * (1 - jitter)
	upper := float64(delay) * (1 + jitter)
	if upper > float64(max) {
		upper = float64(max)
	}
	if upper <= lower {
		return delay
	}
	result := lower + rand.Float64()*(upper-lower)
	if result < float64(time.Nanosecond) {
		result = float64(time.Nanosecond)
	}
	return time.Duration(result)
}

func (s *Server) trackActive(msg *model.TaskMessage) {
	s.mu.Lock()
	s.active[msg.ID] = msg
	s.mu.Unlock()
}
func (s *Server) untrackActive(id string) { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }

func (s *Server) Stop() {
	s.mu.Lock()
	if s.state != stateRunning {
		s.mu.Unlock()
		return
	}
	s.state = stateStopped
	s.stopOne.Do(func() { close(s.stopCh); s.cancel() })
	s.mu.Unlock()
}

func (s *Server) Shutdown() error {
	s.Stop()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(s.cfg.ShutdownTimeout):
		s.requeueActive()
		return fmt.Errorf("shutdown timeout after %s", s.cfg.ShutdownTimeout)
	}
}

func (s *Server) requeueActive() {
	s.mu.Lock()
	active := make([]*model.TaskMessage, 0, len(s.active))
	for _, msg := range s.active {
		active = append(active, msg)
	}
	s.mu.Unlock()
	for _, msg := range active {
		if err := s.store.Requeue(context.Background(), msg); err != nil {
			s.reportError(err)
		}
	}
}

// Run starts the server and stops it when ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	return s.Shutdown()
}

// RunSignals starts the server and shuts it down when ctx is canceled or a signal arrives.
func (s *Server) RunSignals(ctx context.Context, signals <-chan os.Signal) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.Start(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-signals:
	}
	return s.Shutdown()
}

func (s *Server) Concurrency() int { return s.cfg.Concurrency }
