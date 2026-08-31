package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go-taskengine/model"
	"go-taskengine/storage"
)

const keyPrefix = "gte:"

var (
	ErrNoTask            = storage.ErrNoTask
	ErrTaskExists        = storage.ErrTaskExists
	ErrInvalidTransition = storage.ErrInvalidTransition
)

// Store persists tasks and performs state transitions atomically in Redis.
type Store struct {
	client        redis.UniversalClient
	leaseDuration time.Duration
}

func New(client redis.UniversalClient) *Store {
	return &Store{client: client, leaseDuration: 30 * time.Second}
}

func (s *Store) Client() redis.UniversalClient { return s.client }
func (s *Store) SetLeaseDuration(d time.Duration) {
	if d > 0 {
		s.leaseDuration = d
	}
}

func queuePrefix(queue string) string    { return keyPrefix + "{" + queue + "}:" }
func TaskKey(queue, id string) string    { return queuePrefix(queue) + "task:" + id }
func PendingKey(queue string) string     { return queuePrefix(queue) + "pending" }
func PendingRankKey(queue string) string { return queuePrefix(queue) + "pending_rank" }
func ScheduledKey(queue string) string   { return queuePrefix(queue) + "scheduled" }
func RetryKey(queue string) string       { return queuePrefix(queue) + "retry" }
func ActiveKey(queue string) string      { return queuePrefix(queue) + "active" }
func LeaseKey(queue string) string       { return queuePrefix(queue) + "lease" }
func ArchivedKey(queue string) string    { return queuePrefix(queue) + "archived" }

var enqueueScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then return 0 end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "pending", "priority", ARGV[3], "rank", ARGV[4])
redis.call("LPUSH", KEYS[2], ARGV[2])
redis.call("ZADD", KEYS[3], ARGV[4], ARGV[2])
return 1
`)

var scheduleScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then return 0 end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "scheduled", "priority", ARGV[3], "rank", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[5], ARGV[2])
return 1
`)

var claimScript = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[2], 0, 0)
if #ids == 0 then return {0, ""} end
local id = ids[1]
if redis.call("ZREM", KEYS[2], id) == 0 then return {0, ""} end
redis.call("LREM", KEYS[3], 1, id)
local taskKey = KEYS[1] .. id
local msg = redis.call("HGET", taskKey, "msg")
if not msg then return {0, ""} end
redis.call("HSET", taskKey, "state", "active", "active_since", ARGV[1])
redis.call("RPUSH", KEYS[4], id)
redis.call("ZADD", KEYS[5], ARGV[2], id)
return {1, msg}
`)

var moveReadyScript = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
local moved = 0
local max = tonumber(ARGV[2])
for _, id in ipairs(ids) do
  if moved >= max then break end
  if redis.call("ZREM", KEYS[1], id) == 1 then
    local taskKey = ARGV[3] .. id
    local rank = redis.call("HGET", taskKey, "rank")
    if rank then
      redis.call("HSET", taskKey, "state", "pending")
      redis.call("LPUSH", KEYS[2], id)
      redis.call("ZADD", KEYS[3], rank, id)
      moved = moved + 1
    end
  end
end
return moved
`)

var moveOneScript = redis.NewScript(`
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then return 0 end
local rank = redis.call("HGET", KEYS[4], "rank")
if not rank then return 0 end
redis.call("HSET", KEYS[4], "state", "pending")
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], rank, ARGV[1])
return 1
`)

var ackScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "active" then return 0 end
redis.call("LREM", KEYS[2], 1, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "completed", "completed_at", ARGV[3])
return 1
`)

var retryScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "active" then return 0 end
redis.call("LREM", KEYS[2], 1, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry", "retry_at", ARGV[3], "last_error", ARGV[4])
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
return 1
`)

var archiveScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "active" then return 0 end
redis.call("LREM", KEYS[2], 1, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived", "last_error", ARGV[3])
redis.call("ZADD", KEYS[4], ARGV[4], ARGV[1])
return 1
`)

var requeueScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "active" then return 0 end
local rank = redis.call("HGET", KEYS[1], "rank")
redis.call("LREM", KEYS[2], 1, ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
redis.call("LPUSH", KEYS[4], ARGV[1])
redis.call("ZADD", KEYS[5], rank, ARGV[1])
return 1
`)

var replayArchivedScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "archived" then return 0 end
local rank = redis.call("HGET", KEYS[1], "rank")
if not rank then return 0 end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then return 0 end
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "pending")
redis.call("LPUSH", KEYS[3], ARGV[1])
redis.call("ZADD", KEYS[4], rank, ARGV[1])
return 1
`)

var deleteArchivedScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= "archived" then return 0 end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then return 0 end
redis.call("DEL", KEYS[1])
return 1
`)

var cleanupArchivedScript = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
local removed = 0
local max = tonumber(ARGV[2])
for _, id in ipairs(ids) do
  if removed >= max then break end
  local taskKey = ARGV[3] .. id
  if redis.call("HGET", taskKey, "state") == "archived" and redis.call("ZREM", KEYS[1], id) == 1 then
    redis.call("DEL", taskKey)
    removed = removed + 1
  end
end
return removed
`)

func (s *Store) Enqueue(ctx context.Context, msg *model.TaskMessage) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode task: %w", err)
	}
	rank := taskRank(msg)
	result, err := enqueueScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), PendingKey(msg.Queue), PendingRankKey(msg.Queue)}, encoded, msg.ID, msg.Priority, rank).Int()
	if err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}
	if result == 0 {
		return ErrTaskExists
	}
	msg.State = model.StatePending
	return nil
}

func (s *Store) Schedule(ctx context.Context, msg *model.TaskMessage) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	if msg.RunAt.IsZero() {
		return errors.New("scheduled task run time is required")
	}
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode task: %w", err)
	}
	rank := taskRank(msg)
	result, err := scheduleScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), ScheduledKey(msg.Queue)}, encoded, msg.ID, msg.Priority, rank, msg.RunAt.UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("schedule task: %w", err)
	}
	if result == 0 {
		return ErrTaskExists
	}
	msg.State = model.StateScheduled
	return nil
}

func (s *Store) Claim(ctx context.Context, queue string, now time.Time, lease time.Duration) (*model.TaskMessage, error) {
	if lease <= 0 {
		lease = s.leaseDuration
	}
	result, err := claimScript.Run(ctx, s.client, []string{TaskKey(queue, ""), PendingRankKey(queue), PendingKey(queue), ActiveKey(queue), LeaseKey(queue)}, now.UnixMilli(), now.Add(lease).UnixMilli()).Result()
	if err != nil {
		return nil, fmt.Errorf("claim task: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return nil, fmt.Errorf("claim task: unexpected result %T", result)
	}
	status, _ := strconv.ParseInt(fmt.Sprint(values[0]), 10, 64)
	if status == 0 {
		return nil, ErrNoTask
	}
	var msg model.TaskMessage
	if err := json.Unmarshal([]byte(fmt.Sprint(values[1])), &msg); err != nil {
		return nil, fmt.Errorf("decode claimed task: %w", err)
	}
	msg.State = model.StateActive
	return &msg, nil
}

func (s *Store) MoveReady(ctx context.Context, now time.Time, limit int, queues ...string) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	moved := 0
	for _, queue := range queues {
		for _, source := range []string{ScheduledKey(queue), RetryKey(queue)} {
			n, err := moveReadyScript.Run(ctx, s.client, []string{source, PendingKey(queue), PendingRankKey(queue)}, now.UnixMilli(), limit, queuePrefix(queue)+"task:").Int()
			if err != nil {
				return moved, fmt.Errorf("move ready tasks: %w", err)
			}
			moved += n
		}
	}
	return moved, nil
}

func (s *Store) AckSuccess(ctx context.Context, msg *model.TaskMessage) error {
	completed := *msg
	completed.State = model.StateCompleted
	completed.CompletedAt = time.Now()
	encoded, err := json.Marshal(&completed)
	if err != nil {
		return fmt.Errorf("encode completed task: %w", err)
	}
	result, err := ackScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), ActiveKey(msg.Queue), LeaseKey(msg.Queue)}, msg.ID, encoded, completed.CompletedAt.UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("ack task: %w", err)
	}
	if result == 0 {
		return ErrInvalidTransition
	}
	msg.State = model.StateCompleted
	msg.CompletedAt = completed.CompletedAt
	return nil
}

func (s *Store) ScheduleRetry(ctx context.Context, msg *model.TaskMessage, at time.Time, reason string) error {
	retry := *msg
	retry.State = model.StateRetry
	retry.LastError = reason
	retry.LastFailedAt = time.Now()
	encoded, err := json.Marshal(&retry)
	if err != nil {
		return fmt.Errorf("encode retry task: %w", err)
	}
	result, err := retryScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), ActiveKey(msg.Queue), LeaseKey(msg.Queue), RetryKey(msg.Queue)}, msg.ID, encoded, at.UnixMilli(), reason).Int()
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	if result == 0 {
		return ErrInvalidTransition
	}
	msg.State = model.StateRetry
	msg.LastError = reason
	msg.LastFailedAt = retry.LastFailedAt
	return nil
}

func (s *Store) Archive(ctx context.Context, msg *model.TaskMessage, reason string) error {
	archived := *msg
	archived.State = model.StateArchived
	archived.LastError = reason
	encoded, err := json.Marshal(&archived)
	if err != nil {
		return fmt.Errorf("encode archived task: %w", err)
	}
	result, err := archiveScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), ActiveKey(msg.Queue), LeaseKey(msg.Queue), ArchivedKey(msg.Queue)}, msg.ID, encoded, reason, time.Now().UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("archive task: %w", err)
	}
	if result == 0 {
		return ErrInvalidTransition
	}
	msg.State = model.StateArchived
	msg.LastError = reason
	return nil
}

func (s *Store) Requeue(ctx context.Context, msg *model.TaskMessage) error {
	result, err := requeueScript.Run(ctx, s.client, []string{TaskKey(msg.Queue, msg.ID), ActiveKey(msg.Queue), LeaseKey(msg.Queue), PendingKey(msg.Queue), PendingRankKey(msg.Queue)}, msg.ID).Int()
	if err != nil {
		return fmt.Errorf("requeue task: %w", err)
	}
	if result == 0 {
		return ErrInvalidTransition
	}
	msg.State = model.StatePending
	return nil
}

func (s *Store) Get(ctx context.Context, queue, id string) (*model.TaskMessage, error) {
	encoded, err := s.client.HGet(ctx, TaskKey(queue, id), "msg").Result()
	if err == redis.Nil {
		return nil, ErrNoTask
	}
	if err != nil {
		return nil, err
	}
	var msg model.TaskMessage
	if err := json.Unmarshal([]byte(encoded), &msg); err != nil {
		return nil, err
	}
	state, err := s.client.HGet(ctx, TaskKey(queue, id), "state").Result()
	if err == nil {
		msg.State = model.TaskState(state)
	}
	return &msg, nil
}

func (s *Store) ExpiredIDs(ctx context.Context, now time.Time, queue string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.client.ZRangeByScore(ctx, LeaseKey(queue), &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: int64(limit),
	}).Result()
}

// ListDeadLetters returns archived tasks in archive-time order. Offset is zero-based;
// a non-positive limit uses the default page size of 100.
func (s *Store) ListDeadLetters(ctx context.Context, queue string, offset, limit int) ([]*model.TaskMessage, error) {
	if offset < 0 {
		return nil, errors.New("dead-letter offset cannot be negative")
	}
	if limit <= 0 {
		limit = 100
	}
	ids, err := s.client.ZRange(ctx, ArchivedKey(queue), int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	result := make([]*model.TaskMessage, 0, len(ids))
	for _, id := range ids {
		msg, err := s.GetDeadLetter(ctx, queue, id)
		if errors.Is(err, ErrInvalidTransition) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read dead letter %s: %w", id, err)
		}
		result = append(result, msg)
	}
	return result, nil
}

// GetDeadLetter returns one archived task.
func (s *Store) GetDeadLetter(ctx context.Context, queue, id string) (*model.TaskMessage, error) {
	msg, err := s.Get(ctx, queue, id)
	if err != nil {
		return nil, err
	}
	if msg.State != model.StateArchived {
		return nil, ErrInvalidTransition
	}
	return msg, nil
}

// ReplayDeadLetter resets an archived task's retry metadata and returns it to pending.
func (s *Store) ReplayDeadLetter(ctx context.Context, queue, id string) error {
	msg, err := s.GetDeadLetter(ctx, queue, id)
	if err != nil {
		return err
	}
	msg.State = model.StatePending
	msg.RetryCount = 0
	msg.LastError = ""
	msg.LastFailedAt = time.Time{}
	msg.CompletedAt = time.Time{}
	msg.RunAt = time.Now()
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode replayed dead letter: %w", err)
	}
	result, err := replayArchivedScript.Run(ctx, s.client, []string{
		TaskKey(queue, id), ArchivedKey(queue), PendingKey(queue), PendingRankKey(queue),
	}, id, encoded).Int()
	if err != nil {
		return fmt.Errorf("replay dead letter: %w", err)
	}
	if result == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// DeleteDeadLetter permanently removes one archived task.
func (s *Store) DeleteDeadLetter(ctx context.Context, queue, id string) error {
	result, err := deleteArchivedScript.Run(ctx, s.client, []string{TaskKey(queue, id), ArchivedKey(queue)}, id).Int()
	if err != nil {
		return fmt.Errorf("delete dead letter: %w", err)
	}
	if result == 0 {
		if _, getErr := s.GetDeadLetter(ctx, queue, id); getErr != nil {
			return getErr
		}
		return ErrInvalidTransition
	}
	return nil
}

// CleanupDeadLetters deletes at most limit archived tasks older than before.
func (s *Store) CleanupDeadLetters(ctx context.Context, queue string, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	removed, err := cleanupArchivedScript.Run(ctx, s.client, []string{ArchivedKey(queue)}, before.UnixMilli(), limit, queuePrefix(queue)+"task:").Int()
	if err != nil {
		return 0, fmt.Errorf("cleanup dead letters: %w", err)
	}
	return removed, nil
}

// Archived aliases retain terminology used by the Redis key layout.
func (s *Store) ListArchived(ctx context.Context, queue string, offset, limit int) ([]*model.TaskMessage, error) {
	return s.ListDeadLetters(ctx, queue, offset, limit)
}
func (s *Store) GetArchived(ctx context.Context, queue, id string) (*model.TaskMessage, error) {
	return s.GetDeadLetter(ctx, queue, id)
}
func (s *Store) ReplayArchived(ctx context.Context, queue, id string) error {
	return s.ReplayDeadLetter(ctx, queue, id)
}
func (s *Store) DeleteArchived(ctx context.Context, queue, id string) error {
	return s.DeleteDeadLetter(ctx, queue, id)
}
func (s *Store) CleanupArchived(ctx context.Context, queue string, before time.Time, limit int) (int, error) {
	return s.CleanupDeadLetters(ctx, queue, before, limit)
}

func (s *Store) PendingCount(ctx context.Context, queue string) (int64, error) {
	return s.client.ZCard(ctx, PendingRankKey(queue)).Result()
}
func (s *Store) RetryCount(ctx context.Context, queue string) (int64, error) {
	return s.client.ZCard(ctx, RetryKey(queue)).Result()
}
func (s *Store) ArchivedCount(ctx context.Context, queue string) (int64, error) {
	return s.client.ZCard(ctx, ArchivedKey(queue)).Result()
}

func (s *Store) ExtendLease(ctx context.Context, queue string, ids []string, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		lease = s.leaseDuration
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for _, id := range ids {
		pipe.ZAdd(ctx, LeaseKey(queue), redis.Z{Score: float64(now.Add(lease).UnixMilli()), Member: id})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func taskRank(msg *model.TaskMessage) string {
	// Priority dominates; the millisecond component preserves FIFO within a priority.
	return fmt.Sprintf("%.3f", -float64(msg.Priority)*1e15+float64(msg.CreatedAt.UnixMilli()))
}
