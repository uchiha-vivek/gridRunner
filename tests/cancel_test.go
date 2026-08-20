package tests

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/openmic/forgerun/internal/api"
	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/executor"
	"github.com/openmic/forgerun/internal/github"
	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/runner"
	"github.com/openmic/forgerun/internal/scheduler"
	"github.com/openmic/forgerun/internal/store"
)

// slowExecutor holds one specific job open forever, so the only way that job ends
// is if the cancellation actually reaches the executor's context. Any other job
// finishes immediately: the queue is shared with the running stack, and a
// leftover job must not stall the test.
type slowExecutor struct {
	target    string
	started   chan struct{}
	cancelled chan struct{}
	begin     sync.Once
	stop      sync.Once
}

func newSlowExecutor(jobID string) *slowExecutor {
	return &slowExecutor{target: jobID, started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (e *slowExecutor) Execute(ctx context.Context, req executor.Request, logs io.Writer) (executor.Result, error) {
	if req.Job.ID != e.target {
		fmt.Fprintln(logs, "slow-executor: not the job under test, finishing immediately")
		return executor.Result{ExitCode: 0}, nil
	}
	fmt.Fprintln(logs, "slow-executor: this build runs until it is stopped")
	e.begin.Do(func() { close(e.started) })
	select {
	case <-ctx.Done():
		e.stop.Do(func() { close(e.cancelled) })
		return executor.Result{}, ctx.Err()
	case <-time.After(2 * time.Minute):
		return executor.Result{ExitCode: 0}, nil
	}
}

// TestCancelStopsARunningJob is the Phase 6 gate: cancelling a job that is
// already executing on a runner must kill the work, not just relabel the row.
func TestCancelStopsARunningJob(t *testing.T) {
	if os.Getenv("FORGERUN_INTEGRATION") == "" {
		t.Skip("set FORGERUN_INTEGRATION=1 and run `docker compose up -d postgres redis`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	st, err := store.New(ctx, env("DATABASE_URL", "postgres://forgerun:forgerun@localhost:5432/forgerun?sslmode=disable"))
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	q, err := queue.NewRedis(env("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// A short heartbeat keeps the test quick: the runner learns about the
	// cancellation on its next heartbeat, which is how the signal travels.
	cfg := config.Config{
		RunnerRegistrationToken: registrationToken,
		HeartbeatTimeout:        3 * time.Second,
		JobTimeout:              time.Minute,
		WorkspaceRoot:           t.TempDir(),
		RunnerName:              "cancel-runner",
		RunnerLabels:            "linux,docker",
	}
	srv := httptest.NewServer(api.NewServer(cfg, st, q, github.NoopClient{}, log).Routes())
	defer func() {
		cancel() // end the runner's long poll first, or Close waits it out
		srv.Close()
	}()
	cfg.ServerURL = srv.URL

	jobID := uuid.NewString()
	exec := newSlowExecutor(jobID)
	go scheduler.NewService(st, q, scheduler.CapabilityScheduler{}, log, cfg.HeartbeatTimeout).Run(ctx)
	go runner.New(cfg, exec, log).Run(ctx)

	repoDir, sha := makeRepo(t, "jobs:\n  test:\n    image: alpine:3.20\n    commands: [\"sleep 600\"]\n")
	job := &models.Job{
		ID: jobID, Repository: "forgerun/cancel-" + uuid.NewString()[:8],
		CloneURL: fileURL(repoDir), CommitSHA: sha, Branch: "main", EventType: "push",
		ConfigRef: "forgerun.yml", Labels: []string{"linux", "docker"}, Status: models.JobQueued,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, job.ID); err != nil {
		t.Fatal(err)
	}

	begin := time.Now()
	select {
	case <-exec.started:
		t.Logf("job reached the executor after %s", time.Since(begin).Round(time.Millisecond))
	case <-time.After(45 * time.Second):
		current, _ := st.GetJob(ctx, job.ID)
		t.Fatalf("the job never reached the executor, status = %s", current.Status)
	}

	// Cancel the way an operator would.
	requested := time.Now()
	resp, err := http.Post(srv.URL+"/api/v1/jobs/"+job.ID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel returned %d, want 200", resp.StatusCode)
	}

	// The point of the phase: the work stops, not just the database row.
	select {
	case <-exec.cancelled:
		// Bounded by the busy heartbeat interval, not by the job timeout.
		t.Logf("work stopped %s after the cancel request", time.Since(requested).Round(time.Millisecond))
	case <-time.After(30 * time.Second):
		t.Fatal("the executor was never cancelled: the job would have kept burning CPU")
	}

	final, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != models.JobCancelled {
		t.Errorf("status = %s, want CANCELLED", final.Status)
	}
	if final.RunnerID == nil {
		t.Fatal("cancelled job lost its runner reference")
	}

	// The runner must be usable again, and must say so in the build log.
	waitFor(t, 20*time.Second, func() bool {
		r, err := st.GetRunner(ctx, *final.RunnerID)
		return err == nil && r.Status == models.RunnerIdle
	}, "runner never returned to IDLE after the cancellation")

	waitFor(t, 20*time.Second, func() bool {
		logs, err := st.GetLogs(ctx, job.ID)
		return err == nil && strings.Contains(logs, "cancelled by the control plane")
	}, "the build log never recorded why the job stopped")
}

// TestLogChunksAreIdempotent covers the retry a runner performs when it never saw
// the response to a chunk it had in fact delivered.
func TestLogChunksAreIdempotent(t *testing.T) {
	if os.Getenv("FORGERUN_INTEGRATION") == "" {
		t.Skip("set FORGERUN_INTEGRATION=1")
	}
	ctx := context.Background()
	st, err := store.New(ctx, env("DATABASE_URL", "postgres://forgerun:forgerun@localhost:5432/forgerun?sslmode=disable"))
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	job := &models.Job{
		ID: uuid.NewString(), Repository: "forgerun/logs-" + uuid.NewString()[:8],
		CloneURL: "https://example.invalid/repo.git", CommitSHA: strings.Repeat("a", 40),
		Branch: "main", EventType: "push", ConfigRef: "forgerun.yml",
		Labels: []string{"linux"}, Status: models.JobQueued,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	for _, chunk := range []struct {
		seq  int
		text string
	}{
		{1, "first\n"},
		{2, "second\n"},
		{2, "second\n"}, // the retry
		{3, "third\n"},
	} {
		if err := st.AppendLogs(ctx, job.ID, chunk.seq, chunk.text); err != nil {
			t.Fatalf("append seq %d: %v", chunk.seq, err)
		}
	}

	logs, err := st.GetLogs(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "first\nsecond\nthird\n"; logs != want {
		t.Errorf("logs = %q, want %q", logs, want)
	}
	if err := st.AppendLogs(ctx, uuid.NewString(), 1, "orphan"); err == nil {
		t.Error("appending to an unknown job should fail")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Error(msg)
}
