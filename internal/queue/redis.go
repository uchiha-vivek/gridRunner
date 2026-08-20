package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pendingKey  = "forgerun:jobs:pending"
	inflightKey = "forgerun:jobs:inflight"
	runnerKey   = "forgerun:runner:%s:mailbox"
)

// RedisQueue implements a reliable queue with two Redis lists.
//
// Reliability comes from BLMOVE: the job id leaves "pending" and appears in
// "inflight" in a single atomic step. If the scheduler dies mid-assignment the id
// is still in "inflight" and Recover() puts it back. This is simpler to reason
// about than Streams + consumer groups and is enough for one scheduler process;
// Streams become worthwhile when several schedulers share the queue.
type RedisQueue struct {
	rdb *redis.Client
}

func NewRedis(redisURL string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	return &RedisQueue{rdb: redis.NewClient(opts)}, nil
}

func (q *RedisQueue) Ping(ctx context.Context) error { return q.rdb.Ping(ctx).Err() }

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string) error {
	return q.rdb.LPush(ctx, pendingKey, jobID).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context, timeout time.Duration) (string, error) {
	id, err := q.rdb.BLMove(ctx, pendingKey, inflightKey, "right", "left", timeout).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrEmpty
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

func (q *RedisQueue) Ack(ctx context.Context, jobID string) error {
	return q.rdb.LRem(ctx, inflightKey, 1, jobID).Err()
}

func (q *RedisQueue) Requeue(ctx context.Context, jobID string) error {
	if err := q.Ack(ctx, jobID); err != nil {
		return err
	}
	return q.Enqueue(ctx, jobID)
}

func (q *RedisQueue) Recover(ctx context.Context) (int, error) {
	ids, err := q.rdb.LRange(ctx, inflightKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		if err := q.Requeue(ctx, id); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

// Dispatch/Receive are per-runner mailboxes. The runner never talks to Redis
// itself: it long-polls the API, which reads the mailbox on its behalf. That keeps
// the data plane off the control plane's infrastructure.
func (q *RedisQueue) Dispatch(ctx context.Context, runnerID, jobID string) error {
	key := fmt.Sprintf(runnerKey, runnerID)
	if err := q.rdb.LPush(ctx, key, jobID).Err(); err != nil {
		return err
	}
	// A mailbox for a runner that never comes back must not leak.
	return q.rdb.Expire(ctx, key, time.Hour).Err()
}

func (q *RedisQueue) Receive(ctx context.Context, runnerID string, timeout time.Duration) (string, error) {
	res, err := q.rdb.BRPop(ctx, timeout, fmt.Sprintf(runnerKey, runnerID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrEmpty
	}
	if err != nil {
		return "", err
	}
	return res[1], nil // BRPOP returns [key, value]
}

func (q *RedisQueue) Close() error { return q.rdb.Close() }
