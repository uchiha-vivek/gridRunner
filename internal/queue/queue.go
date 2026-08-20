// Package queue is the job queue abstraction plus its Redis implementation.
//
// The interface exists so Redis can be swapped for Kafka/SQS later without the
// scheduler knowing. It is deliberately tiny.
package queue

import (
	"context"
	"errors"
	"time"
)

// ErrEmpty is returned by Dequeue when the blocking wait expired with no job.
// It is a normal control-flow signal, not a failure.
var ErrEmpty = errors.New("queue is empty")

type Queue interface {
	// Enqueue adds a job id to the pending queue.
	Enqueue(ctx context.Context, jobID string) error

	// Dequeue blocks up to timeout and atomically moves a job id into an
	// in-flight list, so a crashed consumer never loses the job.
	Dequeue(ctx context.Context, timeout time.Duration) (string, error)

	// Ack removes a job id from the in-flight list once it is safely persisted.
	Ack(ctx context.Context, jobID string) error

	// Requeue puts an in-flight job back on the pending queue (runner died,
	// no capacity available yet, and so on).
	Requeue(ctx context.Context, jobID string) error

	// Recover returns every in-flight job id to the pending queue. Called on
	// scheduler startup to clean up after a crash.
	Recover(ctx context.Context) (int, error)

	// Dispatch hands an assigned job to one specific runner's mailbox.
	Dispatch(ctx context.Context, runnerID, jobID string) error

	// Receive blocks up to timeout waiting for work addressed to one runner.
	Receive(ctx context.Context, runnerID string, timeout time.Duration) (string, error)

	Close() error
}
