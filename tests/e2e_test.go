// Package tests holds the end-to-end test: a job travels through Redis, the
// scheduler, a real runner agent (including a real git checkout) and back.
//
// It needs the control-plane dependencies from docker-compose:
//
//	docker compose up -d postgres redis
//	FORGERUN_INTEGRATION=1 go test ./tests/...
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

const registrationToken = "test-registration-token"

// fakeExecutor stands in for Docker so the pipeline can be tested without a
// container runtime. It still asserts that the runner handed it a real checkout.
type fakeExecutor struct{ exitCode int }

func (f fakeExecutor) Execute(ctx context.Context, req executor.Request, logs io.Writer) (executor.Result, error) {
	if _, err := os.Stat(filepath.Join(req.Workspace, "forgerun.yml")); err != nil {
		return executor.Result{}, fmt.Errorf("workspace is missing the checkout: %w", err)
	}
	fmt.Fprintf(logs, "fake-executor: image=%s commands=%v\n", req.Spec.Image, req.Spec.Commands)
	return executor.Result{ExitCode: f.exitCode}, nil
}

func TestEndToEndJobPipeline(t *testing.T) {
	if os.Getenv("FORGERUN_INTEGRATION") == "" {
		t.Skip("set FORGERUN_INTEGRATION=1 and run `docker compose up -d postgres redis`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	st, err := store.New(ctx, env("DATABASE_URL", "postgres://forgerun:forgerun@localhost:5432/forgerun?sslmode=disable"))
	if err != nil {
		t.Fatalf("postgres: %v (is docker compose up?)", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	q, err := queue.NewRedis(env("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer q.Close()

	// --- control plane ---
	cfg := config.Config{
		RunnerRegistrationToken: registrationToken,
		HeartbeatTimeout:        45 * time.Second,
		JobTimeout:              time.Minute,
		DefaultImage:            "alpine:3.20",
		DockerNetwork:           "none",
		JobCPUs:                 1,
		JobMemoryMB:             512,
		WorkspaceRoot:           t.TempDir(),
	}
	srv := httptest.NewServer(api.NewServer(cfg, st, q, github.NoopClient{}, log).Routes())
	defer srv.Close()
	cfg.ServerURL = srv.URL

	go scheduler.NewService(st, q, scheduler.CapabilityScheduler{}, log, cfg.HeartbeatTimeout).Run(ctx)

	// --- data plane ---
	cfg.RunnerName = "e2e-runner"
	cfg.RunnerLabels = "linux,docker"
	agent := runner.New(cfg, fakeExecutor{exitCode: 0}, log)
	go agent.Run(ctx)

	// --- a real repository with a real commit ---
	repoDir, sha := makeRepo(t, `
jobs:
  test:
    image: alpine:3.20
    commands:
      - echo hello
`)

	job := &models.Job{
		ID:         uuid.NewString(),
		Repository: "forgerun/e2e-" + uuid.NewString()[:8],
		CloneURL:   fileURL(repoDir),
		CommitSHA:  sha,
		Branch:     "main",
		EventType:  "push",
		ConfigRef:  "forgerun.yml",
		Labels:     []string{"linux", "docker"},
		Status:     models.JobQueued,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := q.Enqueue(ctx, job.ID); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	final := waitForTerminal(t, ctx, st, job.ID, 60*time.Second)

	if final.Status != models.JobSuccess {
		logs, _ := st.GetLogs(ctx, job.ID)
		t.Fatalf("job ended as %s (error=%q)\nlogs:\n%s", final.Status, final.Error, logs)
	}
	if final.ExitCode == nil || *final.ExitCode != 0 {
		t.Errorf("exit code = %v, want 0", final.ExitCode)
	}
	if final.RunnerID == nil {
		t.Fatal("finished job has no runner recorded")
	}
	if final.StartedAt == nil || final.CompletedAt == nil {
		t.Error("started_at and completed_at should both be set")
	}

	logs, err := st.GetLogs(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"checked out main", "fake-executor: image=alpine:3.20"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs do not contain %q\ngot:\n%s", want, logs)
		}
	}

	// The runner must be free again once the job is done.
	r, err := st.GetRunner(ctx, *final.RunnerID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != models.RunnerIdle {
		t.Errorf("runner status = %s, want IDLE", r.Status)
	}
}

// TestFailingJobIsReportedAsFailed proves a non-zero exit is not swallowed.
func TestFailingJobIsReportedAsFailed(t *testing.T) {
	if os.Getenv("FORGERUN_INTEGRATION") == "" {
		t.Skip("set FORGERUN_INTEGRATION=1")
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

	cfg := config.Config{
		RunnerRegistrationToken: registrationToken,
		HeartbeatTimeout:        45 * time.Second,
		JobTimeout:              time.Minute,
		WorkspaceRoot:           t.TempDir(),
		RunnerName:              "e2e-failing-runner",
		RunnerLabels:            "linux,docker",
	}
	srv := httptest.NewServer(api.NewServer(cfg, st, q, github.NoopClient{}, log).Routes())
	defer srv.Close()
	cfg.ServerURL = srv.URL

	go scheduler.NewService(st, q, scheduler.CapabilityScheduler{}, log, cfg.HeartbeatTimeout).Run(ctx)
	go runner.New(cfg, fakeExecutor{exitCode: 7}, log).Run(ctx)

	repoDir, sha := makeRepo(t, "jobs:\n  test:\n    image: alpine:3.20\n    commands: [\"false\"]\n")
	job := &models.Job{
		ID: uuid.NewString(), Repository: "forgerun/fail-" + uuid.NewString()[:8],
		CloneURL: fileURL(repoDir), CommitSHA: sha, Branch: "main", EventType: "push",
		ConfigRef: "forgerun.yml", Labels: []string{"linux", "docker"}, Status: models.JobQueued,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, job.ID); err != nil {
		t.Fatal(err)
	}

	final := waitForTerminal(t, ctx, st, job.ID, 60*time.Second)
	if final.Status != models.JobFailed {
		t.Fatalf("status = %s, want FAILED", final.Status)
	}
	if final.ExitCode == nil || *final.ExitCode != 7 {
		t.Errorf("exit code = %v, want 7", final.ExitCode)
	}
}

// TestRunnerRegistrationRequiresToken guards the registration endpoint.
func TestRunnerRegistrationRequiresToken(t *testing.T) {
	if os.Getenv("FORGERUN_INTEGRATION") == "" {
		t.Skip("set FORGERUN_INTEGRATION=1")
	}
	ctx := context.Background()
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

	cfg := config.Config{RunnerRegistrationToken: registrationToken}
	srv := httptest.NewServer(api.NewServer(cfg, st, q, github.NoopClient{}, log).Routes())
	defer srv.Close()

	body := strings.NewReader(`{"name":"intruder","labels":["linux"]}`)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/runners/register", body)
	req.Header.Set("X-Registration-Token", "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}

	// With the right token the runner gets an id and a token back.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/api/v1/runners/register",
		strings.NewReader(`{"name":"legit","labels":["linux"]}`))
	req.Header.Set("X-Registration-Token", registrationToken)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp2.StatusCode)
	}
	var out struct {
		RunnerID string `json:"runner_id"`
		Token    string `json:"token"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.RunnerID == "" || out.Token == "" {
		t.Fatalf("registration response = %+v", out)
	}

	// Heartbeats need that token.
	hb, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/runners/"+out.RunnerID+"/heartbeat", nil)
	hb.Header.Set("Authorization", "Bearer "+out.Token)
	resp3, err := http.DefaultClient.Do(hb)
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat status = %d, want 200", resp3.StatusCode)
	}

	bad, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/runners/"+out.RunnerID+"/heartbeat", nil)
	bad.Header.Set("Authorization", "Bearer not-the-token")
	resp4, err := http.DefaultClient.Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad-token heartbeat = %d, want 401", resp4.StatusCode)
	}
}

// --- helpers ---

func waitForTerminal(t *testing.T, ctx context.Context, st *store.Store, jobID string, timeout time.Duration) *models.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := st.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if job.Status.Terminal() {
			return job
		}
		time.Sleep(250 * time.Millisecond)
	}
	job, _ := st.GetJob(ctx, jobID)
	t.Fatalf("job did not finish within %s, last status = %s", timeout, job.Status)
	return nil
}

// makeRepo builds a throwaway git repository containing forgerun.yml and returns
// its path and commit sha.
func makeRepo(t *testing.T, spec string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "forgerun.yml"), []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}
	cmds := [][]string{
		{"git", "init", "-q", "-b", "main"},
		{"git", "config", "user.email", "test@forgerun.local"},
		{"git", "config", "user.name", "forgerun test"},
		{"git", "add", "."},
		{"git", "commit", "-q", "-m", "initial"},
	}
	for _, c := range cmds {
		cmd := exec.Command(c[0], c[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return dir, strings.TrimSpace(string(out))
}

// fileURL turns a local path into a file:// URL git accepts on every platform.
func fileURL(dir string) string {
	abs, _ := filepath.Abs(dir)
	return "file:///" + strings.TrimPrefix(strings.ReplaceAll(abs, `\`, "/"), "/")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
