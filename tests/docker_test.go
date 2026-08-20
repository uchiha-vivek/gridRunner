package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/openmic/forgerun/internal/api"
	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/executor"
	"github.com/openmic/forgerun/internal/github"
	"github.com/openmic/forgerun/internal/jobspec"
	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/runner"
	"github.com/openmic/forgerun/internal/scheduler"
	"github.com/openmic/forgerun/internal/store"
)

// These tests start real containers. They are opt-in because they need a Docker
// engine and pull an image on first run.
func requireDocker(t *testing.T) *executor.DockerExecutor {
	t.Helper()
	if os.Getenv("FORGERUN_DOCKER") == "" {
		t.Skip("set FORGERUN_DOCKER=1 to run tests that start real containers")
	}
	exec, err := executor.NewDocker()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.Ping(ctx); err != nil {
		t.Skipf("docker engine not responding: %v", err)
	}
	t.Cleanup(func() { exec.Close() })
	return exec
}

func TestDockerExecutorSuccess(t *testing.T) {
	exec := requireDocker(t)
	ws := t.TempDir()
	if err := os.WriteFile(filepath.Join(ws, "marker.txt"), []byte("from the workspace"), 0o600); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	res, err := exec.Execute(context.Background(), executor.Request{
		Job: models.Job{ID: uuid.NewString(), CommitSHA: "abc123", Branch: "main"},
		Spec: jobspec.Job{
			Name:     "test",
			Image:    "alpine:3.20",
			Commands: []string{"cat marker.txt", "echo commit=$FORGERUN_COMMIT"},
		},
		Workspace: ws,
		Timeout:   2 * time.Minute,
		CPUs:      1,
		MemoryMB:  256,
		Network:   "none",
	}, &logs)
	if err != nil {
		t.Fatalf("execute: %v\nlogs:\n%s", err, logs.String())
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0\nlogs:\n%s", res.ExitCode, logs.String())
	}
	out := logs.String()
	if !strings.Contains(out, "from the workspace") {
		t.Errorf("the workspace was not mounted; logs:\n%s", out)
	}
	if !strings.Contains(out, "commit=abc123") {
		t.Errorf("job metadata did not reach the container; logs:\n%s", out)
	}
}

// A failing command must surface as a non-zero exit code, not an error.
func TestDockerExecutorFailure(t *testing.T) {
	exec := requireDocker(t)
	var logs bytes.Buffer
	res, err := exec.Execute(context.Background(), executor.Request{
		Job:       models.Job{ID: uuid.NewString()},
		Spec:      jobspec.Job{Image: "alpine:3.20", Commands: []string{"echo before", "exit 3", "echo never"}},
		Workspace: t.TempDir(),
		Timeout:   time.Minute,
		CPUs:      1,
		MemoryMB:  256,
		Network:   "none",
	}, &logs)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("exit code = %d, want 3", res.ExitCode)
	}
	if strings.Contains(logs.String(), "never") {
		t.Error("commands after a failure should not run (set -e)")
	}
}

// A job that runs forever must be killed at the timeout.
func TestDockerExecutorTimeout(t *testing.T) {
	exec := requireDocker(t)
	var logs bytes.Buffer
	res, err := exec.Execute(context.Background(), executor.Request{
		Job:       models.Job{ID: uuid.NewString()},
		Spec:      jobspec.Job{Image: "alpine:3.20", Commands: []string{"sleep 120"}},
		Workspace: t.TempDir(),
		Timeout:   5 * time.Second,
		CPUs:      1,
		MemoryMB:  256,
		Network:   "none",
	}, &logs)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.TimedOut {
		t.Errorf("expected TimedOut, got %+v", res)
	}
}

// A cancelled job must be killed too, and must say so: a build stopped by an
// operator is not the same event as one that ran out of time.
func TestDockerExecutorCancellation(t *testing.T) {
	exec := requireDocker(t)
	ctx, cancel := context.WithCancel(context.Background())

	var logs safeBuffer
	go func() {
		// Give the container time to start before pulling the rug out.
		time.Sleep(5 * time.Second)
		cancel()
	}()

	res, err := exec.Execute(ctx, executor.Request{
		Job:       models.Job{ID: uuid.NewString()},
		Spec:      jobspec.Job{Image: "alpine:3.20", Commands: []string{"sleep 120"}},
		Workspace: t.TempDir(),
		Timeout:   10 * time.Minute, // far away: only the cancellation can end this
		CPUs:      1,
		MemoryMB:  256,
		Network:   "none",
	}, &logs)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if res.TimedOut {
		t.Error("a cancelled job must not be reported as a timeout")
	}
	if out := logs.String(); !strings.Contains(out, "job stopped") {
		t.Errorf("build log does not explain the stop: %q", out)
	}
}

// safeBuffer is a bytes.Buffer the executor's log goroutine and the test can both
// touch without racing.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestEndToEndWithRealContainer is the full demo: a queued job travels through
// Redis, the scheduler and a runner, and is executed in an actual container.
func TestEndToEndWithRealContainer(t *testing.T) {
	exec := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	st, err := store.New(ctx, env("DATABASE_URL", "postgres://forgerun:forgerun@localhost:5432/forgerun?sslmode=disable"))
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
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

	cfg := config.Config{
		RunnerRegistrationToken: registrationToken,
		HeartbeatTimeout:        45 * time.Second,
		JobTimeout:              2 * time.Minute,
		WorkspaceRoot:           t.TempDir(),
		RunnerName:              "docker-e2e-runner",
		RunnerLabels:            "linux,docker",
		DefaultImage:            "alpine:3.20",
		DockerNetwork:           "none",
		JobCPUs:                 1,
		JobMemoryMB:             512,
	}
	srv := httptest.NewServer(api.NewServer(cfg, st, q, github.NoopClient{}, log).Routes())
	defer srv.Close()
	cfg.ServerURL = srv.URL

	go scheduler.NewService(st, q, scheduler.CapabilityScheduler{}, log, cfg.HeartbeatTimeout).Run(ctx)
	go runner.New(cfg, exec, log).Run(ctx)

	repoDir, sha := makeRepo(t, `
jobs:
  test:
    image: alpine:3.20
    commands:
      - echo "running in a real container"
      - cat forgerun.yml
`)
	job := &models.Job{
		ID: uuid.NewString(), Repository: "forgerun/docker-" + uuid.NewString()[:8],
		CloneURL: fileURL(repoDir), CommitSHA: sha, Branch: "main", EventType: "push",
		ConfigRef: "forgerun.yml", Labels: []string{"linux", "docker"}, Status: models.JobQueued,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, job.ID); err != nil {
		t.Fatal(err)
	}

	final := waitForTerminal(t, ctx, st, job.ID, 150*time.Second)
	logs, _ := st.GetLogs(ctx, job.ID)
	if final.Status != models.JobSuccess {
		t.Fatalf("status = %s (error=%q)\nlogs:\n%s", final.Status, final.Error, logs)
	}
	if !strings.Contains(logs, "running in a real container") {
		t.Errorf("container output missing from the job log:\n%s", logs)
	}
}
