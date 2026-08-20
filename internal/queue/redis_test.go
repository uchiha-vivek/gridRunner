package queue

import (
	"context"
	"errors"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// scratchDB points a Redis URL at a database reserved for tests. These tests
// delete the queue keys they use, so they must never share a database with a
// running stack or with the pipeline tests in ./tests.
func scratchDB(raw string) string {
	if raw == "" {
		raw = "redis://localhost:6379"
	}
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	u.Path = "/15"
	return u.String()
}

// newTestQueue connects to the Redis from docker-compose. These tests are skipped
// unless it is running, so `go test ./...` stays green on a bare checkout.
func newTestQueue(t *testing.T) *RedisQueue {
	t.Helper()
	url := scratchDB(os.Getenv("REDIS_URL"))
	q, err := NewRedis(url)
	if err != nil {
		t.Skipf("cannot parse REDIS_URL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := q.Ping(ctx); err != nil {
		t.Skipf("redis is not running (docker compose up -d redis): %v", err)
	}
	// A pre-6.2 Redis on the default port (a leftover host install, say) would
	// fail every test with "unknown command blmove". Skip with a useful message
	// instead of a confusing failure.
	if v := serverVersion(ctx, q); !supportsBLMove(v) {
		t.Skipf("redis at %s is version %s; ForgeRun needs 6.2+ for BLMOVE. "+
			"Point REDIS_URL at the compose Redis (see .env.example)", url, v)
	}
	// Start from a clean slate; leftovers from a previous run would break asserts.
	q.rdb.Del(ctx, pendingKey, inflightKey)
	t.Cleanup(func() {
		q.rdb.Del(context.Background(), pendingKey, inflightKey)
		q.Close()
	})
	return q
}

func TestEnqueueDequeueAck(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	got, err := q.Dequeue(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "job-1" {
		t.Fatalf("dequeued %q", got)
	}
	// Before the ack the job is in flight, not lost.
	if n := q.rdb.LLen(ctx, inflightKey).Val(); n != 1 {
		t.Fatalf("inflight length = %d, want 1", n)
	}
	if err := q.Ack(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if n := q.rdb.LLen(ctx, inflightKey).Val(); n != 0 {
		t.Fatalf("inflight length after ack = %d, want 0", n)
	}
}

func TestDequeueIsFIFO(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := q.Enqueue(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := q.Dequeue(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestDequeueTimesOutOnEmptyQueue(t *testing.T) {
	q := newTestQueue(t)
	_, err := q.Dequeue(context.Background(), time.Second)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

// A consumer that dies after Dequeue but before Ack must not lose the job.
func TestRecoverReturnsInFlightJobs(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "crashed-job"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Dequeue(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	// ...consumer crashes here, no Ack...

	n, err := q.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered %d jobs, want 1", n)
	}
	got, err := q.Dequeue(ctx, time.Second)
	if err != nil || got != "crashed-job" {
		t.Fatalf("after recovery got %q, %v", got, err)
	}
}

func TestRequeuePutsJobBack(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "retry-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Dequeue(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := q.Requeue(ctx, "retry-me"); err != nil {
		t.Fatal(err)
	}
	if n := q.rdb.LLen(ctx, inflightKey).Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0 after requeue", n)
	}
	got, err := q.Dequeue(ctx, time.Second)
	if err != nil || got != "retry-me" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestRunnerMailbox(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	defer q.rdb.Del(ctx, "forgerun:runner:r1:mailbox")

	if _, err := q.Receive(ctx, "r1", time.Second); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty mailbox err = %v, want ErrEmpty", err)
	}
	if err := q.Dispatch(ctx, "r1", "job-9"); err != nil {
		t.Fatal(err)
	}
	got, err := q.Receive(ctx, "r1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "job-9" {
		t.Fatalf("received %q", got)
	}
	// A different runner must not see it.
	if _, err := q.Receive(ctx, "r2", time.Second); !errors.Is(err, ErrEmpty) {
		t.Fatalf("other runner got work: %v", err)
	}
}

// serverVersion reads redis_version out of INFO server.
func serverVersion(ctx context.Context, q *RedisQueue) string {
	info, err := q.rdb.Info(ctx, "server").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		}
	}
	return ""
}

func supportsBLMove(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return major > 6 || (major == 6 && minor >= 2)
}
