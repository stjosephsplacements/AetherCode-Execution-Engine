package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// rateLimitScript atomically checks and records a request using a sliding window.
// KEYS[1] = rate limit key
// ARGV[1] = now (ms), ARGV[2] = minute window (ms), ARGV[3] = burst window (ms),
// ARGV[4] = perMinute limit, ARGV[5] = burst limit, ARGV[6] = unique member string
// Returns {1, 0} on allow, {0, 0} on minute-limit exceeded, {0, 1} on burst-limit exceeded.
var rateLimitScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local burstWindow = tonumber(ARGV[3])
local perMinute = tonumber(ARGV[4])
local burst = tonumber(ARGV[5])
local member = ARGV[6]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local total = redis.call('ZCARD', key)
if total >= perMinute then return {0, 0} end
local burstCount = redis.call('ZCOUNT', key, now - burstWindow, '+inf')
if burstCount >= burst then return {0, 1} end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, math.ceil(window/1000) + 1)
return {1, 0}
`)

type Limiter struct {
	rdb            *redis.Client
	perMinute      int
	burst          int
	burstWindowSec int
}

// New creates a sliding-window rate limiter.
// perMinute: max requests per user per 60s window.
// burst: max requests per user per 10s window.
func New(rdb *redis.Client, perMinute, burst int) *Limiter {
	return &Limiter{
		rdb:            rdb,
		perMinute:      perMinute,
		burst:          burst,
		burstWindowSec: 10,
	}
}

// Result of a rate limit check.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Check tests whether an authenticated user may submit.
func (l *Limiter) Check(ctx context.Context, userID uuid.UUID) Result {
	return l.check(ctx, fmt.Sprintf("ac:ratelimit:%s", userID))
}

// CheckIP tests whether an IP may submit (for unauthenticated requests).
func (l *Limiter) CheckIP(ctx context.Context, ip string) Result {
	return l.check(ctx, fmt.Sprintf("ac:ratelimit:ip:%s", ip))
}

func (l *Limiter) check(ctx context.Context, key string) Result {
	nowMs := time.Now().UnixMilli()

	windowMs := int64(60_000)
	burstWindowMs := int64(l.burstWindowSec * 1000)
	member := fmt.Sprintf("%d-%d", nowMs, nowMs^int64(len(key)))

	res, err := rateLimitScript.Run(ctx, l.rdb, []string{key},
		nowMs, windowMs, burstWindowMs,
		l.perMinute, l.burst, member,
	).Int64Slice()
	if err != nil {
		slog.Error("ratelimit: lua script error, failing open", "err", err)
		return Result{Allowed: true}
	}

	allowed := res[0] == 1
	burstLimited := res[1] == 1

	if !allowed {
		if burstLimited {
			return Result{Allowed: false, RetryAfter: time.Duration(l.burstWindowSec) * time.Second}
		}
		retryAfter := l.calcRetryAfter(ctx, key, nowMs, windowMs)
		return Result{Allowed: false, RetryAfter: retryAfter}
	}

	return Result{Allowed: true}
}

func (l *Limiter) calcRetryAfter(ctx context.Context, key string, nowMs, windowMs int64) time.Duration {
	members, err := l.rdb.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil || len(members) == 0 {
		return 10 * time.Second
	}
	oldestMs := int64(members[0].Score)
	retryMs := (oldestMs + windowMs) - nowMs
	if retryMs < 1000 {
		retryMs = 1000
	}
	return time.Duration(retryMs) * time.Millisecond
}

// AdmissionCheck returns true if the queue depth is within the admission threshold.
func AdmissionCheck(ctx context.Context, rdb *redis.Client, streamName string, maxDepth int) (bool, error) {
	length, err := rdb.XLen(ctx, streamName).Result()
	if err != nil {
		return true, err // fail open
	}
	return length < int64(maxDepth), nil
}
