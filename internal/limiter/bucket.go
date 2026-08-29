package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result describes one token acquisition attempt.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration
	Tokens     float64
}

// TokenBucket is a Redis-backed, atomically updated token bucket.
type TokenBucket struct {
	client        redis.UniversalClient
	key           string
	capacity      float64
	ratePerSecond float64
}

func NewTokenBucket(client redis.UniversalClient, key string, capacity, ratePerSecond float64) *TokenBucket {
	if capacity <= 0 || math.IsNaN(capacity) || math.IsInf(capacity, 0) {
		panic("token bucket capacity must be finite and positive")
	}
	if ratePerSecond < 0 || math.IsNaN(ratePerSecond) || math.IsInf(ratePerSecond, 0) {
		panic("token bucket rate must be finite and non-negative")
	}
	return &TokenBucket{client: client, key: key, capacity: capacity, ratePerSecond: ratePerSecond}
}

var ErrInvalidConfig = errors.New("invalid token bucket configuration")

func ScopeKey(scope string) string {
	if scope == "" {
		panic("token bucket scope cannot be empty")
	}
	return "gte:limiter:" + scope
}

func NewScopedTokenBucket(client redis.UniversalClient, scope string, capacity, ratePerSecond float64) *TokenBucket {
	return NewTokenBucket(client, ScopeKey(scope), capacity, ratePerSecond)
}

func (b *TokenBucket) Validate() error {
	if b == nil || b.client == nil {
		return fmt.Errorf("%w: client is nil", ErrInvalidConfig)
	}
	if b.key == "" {
		return fmt.Errorf("%w: key is empty", ErrInvalidConfig)
	}
	if b.capacity <= 0 || math.IsNaN(b.capacity) || math.IsInf(b.capacity, 0) {
		return fmt.Errorf("%w: capacity must be finite and positive", ErrInvalidConfig)
	}
	if b.ratePerSecond < 0 || math.IsNaN(b.ratePerSecond) || math.IsInf(b.ratePerSecond, 0) {
		return fmt.Errorf("%w: rate must be finite and non-negative", ErrInvalidConfig)
	}
	return nil
}

func (b *TokenBucket) Capacity() float64 {
	if b == nil {
		return 0
	}
	return b.capacity
}

func (b *TokenBucket) RatePerSecond() float64 {
	if b == nil {
		return 0
	}
	return b.ratePerSecond
}

var acquireScript = redis.NewScript(`
local nowParts = redis.call("TIME")
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
local tokens = tonumber(redis.call("HGET", KEYS[1], "tokens"))
local last = tonumber(redis.call("HGET", KEYS[1], "last_ms"))
if not tokens or not last then
  tokens = tonumber(ARGV[1])
  last = now
end
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])
if rate > 0 then
  tokens = math.min(capacity, tokens + ((now - last) / 1000) * rate)
end
local allowed = 0
local retry = 0
if tokens >= amount then
  tokens = tokens - amount
  allowed = 1
elseif rate > 0 then
  retry = math.ceil(((amount - tokens) / rate) * 1000)
else
  retry = 31536000000
end
redis.call("HSET", KEYS[1], "tokens", tokens, "last_ms", now)
if rate > 0 then redis.call("PEXPIRE", KEYS[1], math.ceil((capacity / rate) * 2000)) end
return {allowed, retry, tokens}
`)

func (b *TokenBucket) Acquire(ctx context.Context, amount float64) (Result, error) {
	if b == nil || b.client == nil {
		return Result{}, errors.New("token bucket client is nil")
	}
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return Result{}, errors.New("token amount must be finite and positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := acquireScript.Run(ctx, b.client, []string{b.key}, b.capacity, b.ratePerSecond, amount).Result()
	if err != nil {
		return Result{}, fmt.Errorf("acquire token: %w", err)
	}
	values, ok := value.([]interface{})
	if !ok || len(values) != 3 {
		return Result{}, fmt.Errorf("acquire token: unexpected result %T", value)
	}
	allowed, err := strconv.ParseInt(fmt.Sprint(values[0]), 10, 64)
	if err != nil {
		return Result{}, err
	}
	retryMs, err := strconv.ParseInt(fmt.Sprint(values[1]), 10, 64)
	if err != nil {
		return Result{}, err
	}
	tokens, err := strconv.ParseFloat(fmt.Sprint(values[2]), 64)
	if err != nil {
		return Result{}, err
	}
	return Result{Allowed: allowed == 1, RetryAfter: time.Duration(retryMs) * time.Millisecond, Tokens: tokens}, nil
}
