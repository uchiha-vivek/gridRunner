// Package store is the only place that talks SQL. Handlers and services call
// these methods; they never build queries themselves.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openmic/forgerun/internal/models"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrDuplicate         = errors.New("an active job already exists for this commit")
	ErrInvalidTransition = errors.New("invalid state transition")
)

type Store struct{ db *pgxpool.Pool }

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.db.Ping(ctx) }
func (s *Store) Close()                         { s.db.Close() }

// ---------- jobs ----------

const jobCols = `id, repository, clone_url, commit_sha, branch, event_type, config_ref,
	labels, status, runner_id, attempts, exit_code, error, created_at, started_at, completed_at`

func scanJob(row pgx.Row) (*models.Job, error) {
	var j models.Job
	err := row.Scan(&j.ID, &j.Repository, &j.CloneURL, &j.CommitSHA, &j.Branch, &j.EventType,
		&j.ConfigRef, &j.Labels, &j.Status, &j.RunnerID, &j.Attempts, &j.ExitCode, &j.Error,
		&j.CreatedAt, &j.StartedAt, &j.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) CreateJob(ctx context.Context, j *models.Job) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO jobs (id, repository, clone_url, commit_sha, branch, event_type, config_ref, labels, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		j.ID, j.Repository, j.CloneURL, j.CommitSHA, j.Branch, j.EventType, j.ConfigRef, j.Labels, j.Status)
	if err != nil {
		// 23505 = unique_violation: the idempotency index rejected a webhook redelivery.
		if isUniqueViolation(err) {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*models.Job, error) {
	return scanJob(s.db.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
}

func (s *Store) ListJobs(ctx context.Context, limit int) ([]models.Job, error) {
	rows, err := s.db.Query(ctx, `SELECT `+jobCols+` FROM jobs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []models.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

// transition applies a status change only if the state machine allows it AND the
// row is still in the state we read. The "WHERE status = $3" clause is the
// optimistic lock that keeps concurrent schedulers and runners from racing.
func (s *Store) transition(ctx context.Context, id string, to models.JobStatus, extra string, args ...any) (*models.Job, error) {
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if !job.Status.CanTransitionTo(to) {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, job.Status, to)
	}
	q := fmt.Sprintf(`UPDATE jobs SET status = $1 %s WHERE id = $2 AND status = $3`, extra)
	all := append([]any{to, id, job.Status}, args...)
	tag, err := s.db.Exec(ctx, q, all...)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: job changed concurrently", ErrInvalidTransition)
	}
	job.Status = to
	return job, nil
}

// AssignJob moves the job to ASSIGNED and the runner to BUSY in one transaction,
// so the two can never disagree about who owns the job.
func (s *Store) AssignJob(ctx context.Context, jobID, runnerID string) error {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if !job.Status.CanTransitionTo(models.JobAssigned) {
		return fmt.Errorf("%w: %s -> ASSIGNED", ErrInvalidTransition, job.Status)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'ASSIGNED', runner_id = $1, attempts = attempts + 1
		WHERE id = $2 AND status = $3`, runnerID, jobID, job.Status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: job changed concurrently", ErrInvalidTransition)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE runners SET status = 'BUSY', current_job_id = $1, last_assigned_at = now()
		WHERE id = $2 AND status = 'IDLE'`, jobID, runnerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("runner %s is no longer idle", runnerID)
	}
	return tx.Commit(ctx)
}

func (s *Store) StartJob(ctx context.Context, jobID string) error {
	_, err := s.transition(ctx, jobID, models.JobRunning, `, started_at = now()`)
	return err
}

// CompleteJob writes the terminal state and releases the runner.
func (s *Store) CompleteJob(ctx context.Context, jobID string, status models.JobStatus, exitCode int, errMsg string) error {
	job, err := s.transition(ctx, jobID, status,
		`, completed_at = now(), exit_code = $4, error = $5`, exitCode, errMsg)
	if err != nil {
		return err
	}
	if job.RunnerID != nil {
		return s.releaseRunner(ctx, *job.RunnerID)
	}
	return nil
}

// RequeueJob returns a job to the queue after a runner failure.
func (s *Store) RequeueJob(ctx context.Context, jobID string) error {
	job, err := s.transition(ctx, jobID, models.JobQueued, `, runner_id = NULL, started_at = NULL`)
	if err != nil {
		return err
	}
	if job.RunnerID != nil {
		return s.releaseRunner(ctx, *job.RunnerID)
	}
	return nil
}

func (s *Store) CancelJob(ctx context.Context, jobID string) error {
	job, err := s.transition(ctx, jobID, models.JobCancelled, `, completed_at = now()`)
	if err != nil {
		return err
	}
	if job.RunnerID != nil {
		return s.releaseRunner(ctx, *job.RunnerID)
	}
	return nil
}

// AppendLogs adds a chunk of build output. seq is the runner's chunk counter,
// starting at 1: the "$2 > log_seq" guard makes a redelivered chunk a no-op, so a
// runner that retries after a timeout cannot append the same output twice.
func (s *Store) AppendLogs(ctx context.Context, jobID string, seq int, chunk string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs SET logs = logs || $3, log_seq = $2
		WHERE id = $1 AND $2 > log_seq`, jobID, seq, chunk)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Nothing was written: either the job is gone or this chunk already landed.
	var current int
	err = s.db.QueryRow(ctx, `SELECT log_seq FROM jobs WHERE id = $1`, jobID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (s *Store) GetLogs(ctx context.Context, jobID string) (string, error) {
	var logs string
	err := s.db.QueryRow(ctx, `SELECT logs FROM jobs WHERE id = $1`, jobID).Scan(&logs)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return logs, err
}

// ---------- runners ----------

const runnerCols = `id, name, status, labels, architecture, operating_system,
	current_job_id, last_heartbeat, last_assigned_at, created_at`

func scanRunner(row pgx.Row) (*models.Runner, error) {
	var r models.Runner
	err := row.Scan(&r.ID, &r.Name, &r.Status, &r.Labels, &r.Architecture, &r.OS,
		&r.CurrentJobID, &r.LastHeartbeat, &r.LastAssigned, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Store) RegisterRunner(ctx context.Context, r *models.Runner, tokenHash string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO runners (id, name, token_hash, status, labels, architecture, operating_system, last_heartbeat)
		VALUES ($1,$2,$3,'IDLE',$4,$5,$6, now())`,
		r.ID, r.Name, tokenHash, r.Labels, r.Architecture, r.OS)
	return err
}

func (s *Store) GetRunner(ctx context.Context, id string) (*models.Runner, error) {
	return scanRunner(s.db.QueryRow(ctx, `SELECT `+runnerCols+` FROM runners WHERE id = $1`, id))
}

// RunnerTokenHash backs authentication for every runner request.
func (s *Store) RunnerTokenHash(ctx context.Context, id string) (string, error) {
	var h string
	err := s.db.QueryRow(ctx, `SELECT token_hash FROM runners WHERE id = $1`, id).Scan(&h)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return h, err
}

func (s *Store) ListRunners(ctx context.Context) ([]models.Runner, error) {
	rows, err := s.db.Query(ctx, `SELECT `+runnerCols+` FROM runners ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runners := []models.Runner{}
	for rows.Next() {
		r, err := scanRunner(rows)
		if err != nil {
			return nil, err
		}
		runners = append(runners, *r)
	}
	return runners, rows.Err()
}

// Heartbeat refreshes liveness. A runner that was reaped as OFFLINE comes back as
// IDLE, which is why OFFLINE -> IDLE is a legal runner transition.
func (s *Store) Heartbeat(ctx context.Context, id string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE runners
		SET last_heartbeat = now(),
		    status = CASE WHEN status = 'OFFLINE' THEN 'IDLE' ELSE status END
		WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// IdleRunners returns every live candidate, least recently used first. The
// scheduler decides which one wins; the database only supplies the pool.
func (s *Store) IdleRunners(ctx context.Context, heartbeatTimeout time.Duration) ([]models.Runner, error) {
	rows, err := s.db.Query(ctx, `
		SELECT `+runnerCols+` FROM runners
		WHERE status = 'IDLE' AND last_heartbeat > now() - $1::interval
		ORDER BY last_assigned_at ASC NULLS FIRST`,
		fmt.Sprintf("%d seconds", int(heartbeatTimeout.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runners := []models.Runner{}
	for rows.Next() {
		r, err := scanRunner(rows)
		if err != nil {
			return nil, err
		}
		runners = append(runners, *r)
	}
	return runners, rows.Err()
}

func (s *Store) releaseRunner(ctx context.Context, runnerID string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE runners SET status = 'IDLE', current_job_id = NULL
		WHERE id = $1 AND status = 'BUSY'`, runnerID)
	return err
}

// ReapOfflineRunners marks silent runners OFFLINE and returns the jobs they were
// holding, so the caller can requeue that work somewhere else.
func (s *Store) ReapOfflineRunners(ctx context.Context, timeout time.Duration) (map[string]string, error) {
	// The CTE reads current_job_id before the UPDATE clears it; a plain
	// UPDATE ... RETURNING would hand back the already-nulled value.
	rows, err := s.db.Query(ctx, `
		WITH stale AS (
			SELECT id, COALESCE(current_job_id, '') AS job_id FROM runners
			WHERE status <> 'OFFLINE' AND last_heartbeat < now() - $1::interval
		), reaped AS (
			UPDATE runners SET status = 'OFFLINE', current_job_id = NULL
			WHERE id IN (SELECT id FROM stale)
		)
		SELECT id, job_id FROM stale`,
		fmt.Sprintf("%d seconds", int(timeout.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	orphaned := map[string]string{}
	for rows.Next() {
		var runnerID, jobID string
		if err := rows.Scan(&runnerID, &jobID); err != nil {
			return nil, err
		}
		orphaned[runnerID] = jobID
	}
	return orphaned, rows.Err()
}
