package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/store"
)

// Service is the scheduler process: one loop that assigns queued jobs and one
// loop that reaps runners which stopped sending heartbeats.
type Service struct {
	store     *store.Store
	queue     queue.Queue
	sched     Scheduler
	log       *slog.Logger
	heartbeat time.Duration
}

func NewService(st *store.Store, q queue.Queue, sched Scheduler, log *slog.Logger, heartbeatTimeout time.Duration) *Service {
	return &Service{store: st, queue: q, sched: sched, log: log, heartbeat: heartbeatTimeout}
}

func (s *Service) Run(ctx context.Context) error {
	// Anything left in-flight belongs to a previous scheduler that died.
	if n, err := s.queue.Recover(ctx); err != nil {
		s.log.Error("queue recovery failed", "error", err)
	} else if n > 0 {
		s.log.Info("recovered in-flight jobs from a previous run", "count", n)
	}

	go s.reapLoop(ctx)

	s.log.Info("scheduler started")
	for {
		if ctx.Err() != nil {
			s.log.Info("scheduler stopped")
			return nil
		}
		// A short blocking read keeps the loop responsive to shutdown while
		// avoiding a busy poll against Redis.
		jobID, err := s.queue.Dequeue(ctx, 2*time.Second)
		if errors.Is(err, queue.ErrEmpty) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.log.Error("dequeue failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		s.assign(ctx, jobID)
	}
}

func (s *Service) assign(ctx context.Context, jobID string) {
	log := s.log.With("job_id", jobID)

	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		log.Error("cannot load job, dropping it from the queue", "error", err)
		_ = s.queue.Ack(ctx, jobID) // a job that does not exist must not loop forever
		return
	}
	// Cancelled between enqueue and dequeue, or already handled: drop it.
	if job.Status != models.JobQueued {
		log.Info("job is no longer queued, skipping", "status", job.Status)
		_ = s.queue.Ack(ctx, jobID)
		return
	}

	candidates, err := s.store.IdleRunners(ctx, s.heartbeat)
	if err != nil {
		log.Error("cannot list runners", "error", err)
		s.requeue(ctx, jobID)
		return
	}

	runner, err := s.sched.Schedule(*job, candidates)
	if errors.Is(err, ErrNoRunner) {
		log.Info("no compatible runner, requeueing", "labels", job.Labels, "idle_runners", len(candidates))
		time.Sleep(2 * time.Second) // simple back-pressure: do not spin on an empty pool
		s.requeue(ctx, jobID)
		return
	}
	if err != nil {
		log.Error("scheduling failed", "error", err)
		s.requeue(ctx, jobID)
		return
	}

	if err := s.store.AssignJob(ctx, jobID, runner.ID); err != nil {
		log.Error("assignment failed", "runner_id", runner.ID, "error", err)
		s.requeue(ctx, jobID)
		return
	}
	if err := s.queue.Dispatch(ctx, runner.ID, jobID); err != nil {
		// The job is ASSIGNED in the database but never reached the runner's
		// mailbox; put it back so it is picked up again on the next pass.
		log.Error("dispatch failed, returning job to the queue", "runner_id", runner.ID, "error", err)
		_ = s.store.RequeueJob(ctx, jobID)
		s.requeue(ctx, jobID)
		return
	}

	// Ack only once the job is durably assigned and dispatched.
	if err := s.queue.Ack(ctx, jobID); err != nil {
		log.Error("ack failed", "error", err)
	}
	log.Info("job assigned", "runner_id", runner.ID, "runner_name", runner.Name)
}

func (s *Service) requeue(ctx context.Context, jobID string) {
	if err := s.queue.Requeue(ctx, jobID); err != nil {
		s.log.Error("requeue failed", "job_id", jobID, "error", err)
	}
}

// reapLoop marks silent runners OFFLINE and rescues the jobs they were holding.
// This is what stops a crashed runner from stalling a job forever.
func (s *Service) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(s.heartbeat / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			orphaned, err := s.store.ReapOfflineRunners(ctx, s.heartbeat)
			if err != nil {
				s.log.Error("reaping runners failed", "error", err)
				continue
			}
			for runnerID, jobID := range orphaned {
				s.log.Warn("runner missed heartbeats, marked offline", "runner_id", runnerID, "job_id", jobID)
				if jobID == "" {
					continue
				}
				if err := s.store.RequeueJob(ctx, jobID); err != nil {
					s.log.Error("cannot requeue orphaned job", "job_id", jobID, "error", err)
					continue
				}
				if err := s.queue.Enqueue(ctx, jobID); err != nil {
					s.log.Error("cannot re-enqueue orphaned job", "job_id", jobID, "error", err)
					continue
				}
				s.log.Info("orphaned job requeued", "job_id", jobID)
			}
		}
	}
}
