package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/executor"
	"github.com/openmic/forgerun/internal/jobspec"
	"github.com/openmic/forgerun/internal/models"
)

// Agent is the data plane: it owns no database and no queue, and talks to the
// control plane over plain HTTP. That is what lets it run on any machine.
type Agent struct {
	cfg  config.Config
	exec executor.Executor
	log  *slog.Logger
	http *http.Client

	id    string
	token string

	mu     sync.Mutex
	active *activeJob
}

// activeJob is the job this runner is executing right now. The heartbeat loop
// uses it to name the lease it is renewing, and to stop the work when the control
// plane says the lease is gone.
type activeJob struct {
	id        string
	cancel    context.CancelFunc
	abandoned atomic.Bool
}

func New(cfg config.Config, exec executor.Executor, log *slog.Logger) *Agent {
	return &Agent{
		cfg:  cfg,
		exec: exec,
		log:  log,
		// Longer than the server's 25s long-poll, so a normal empty poll is not
		// mistaken for a timeout.
		http: &http.Client{Timeout: 40 * time.Second},
	}
}

func (a *Agent) Run(ctx context.Context) error {
	if err := a.register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	go a.heartbeatLoop(ctx)

	a.log.Info("runner ready, waiting for work", "runner_id", a.id)
	for {
		if ctx.Err() != nil {
			a.log.Info("runner stopped", "runner_id", a.id)
			return nil
		}
		job, err := a.poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			a.log.Error("poll failed, retrying", "error", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if job == nil {
			continue // no work this round
		}
		a.execute(ctx, job)
	}
}

// --- control plane protocol ---

func (a *Agent) register(ctx context.Context) error {
	body, _ := json.Marshal(map[string]any{
		"name":             a.cfg.RunnerName,
		"labels":           strings.Split(a.cfg.RunnerLabels, ","),
		"architecture":     runtime.GOARCH,
		"operating_system": runtime.GOOS,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.ServerURL+"/api/v1/runners/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Registration-Token", a.cfg.RunnerRegistrationToken)

	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("server said %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		RunnerID string `json:"runner_id"`
		Token    string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	a.id, a.token = out.RunnerID, out.Token
	a.log.Info("registered with control plane",
		"runner_id", a.id, "name", a.cfg.RunnerName, "labels", a.cfg.RunnerLabels)
	return nil
}

// heartbeatLoop proves the runner is alive and renews the lease on its current
// job. It is also the only channel the control plane has back into a busy runner,
// so it is how a cancellation arrives: hence the shorter interval while a job is
// running, which bounds how long a cancelled build keeps burning CPU.
func (a *Agent) heartbeatLoop(ctx context.Context) {
	idle := a.cfg.HeartbeatTimeout / 3 // well inside the server's timeout
	busy := min(idle, 5*time.Second)   // cancellation latency, not liveness, sets this

	for {
		interval := idle
		if a.currentJob() != nil {
			interval = busy
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		job := a.currentJob()
		jobID := ""
		if job != nil {
			jobID = job.id
		}
		body, _ := json.Marshal(map[string]string{"job_id": jobID})

		var resp struct {
			Cancel bool `json:"cancel"`
		}
		if err := a.do(ctx, http.MethodPost, "/api/v1/runners/"+a.id+"/heartbeat", body, &resp); err != nil {
			a.log.Warn("heartbeat failed", "runner_id", a.id, "error", err)
			continue
		}
		if resp.Cancel && job != nil {
			// Mark first, then cancel: execute() checks the flag to tell a
			// cancellation apart from a container that simply exited.
			job.abandoned.Store(true)
			job.cancel()
			a.log.Info("control plane cancelled the job, stopping", "job_id", job.id, "runner_id", a.id)
		}
	}
}

// jobDelivery is what the poll endpoint returns: the job, plus any credential
// needed to check it out.
type jobDelivery struct {
	models.Job
	CloneToken string `json:"clone_token"`
}

func (a *Agent) poll(ctx context.Context) (*jobDelivery, error) {
	req, err := a.request(ctx, http.MethodGet, "/api/v1/runners/"+a.id+"/job", nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil
	case http.StatusOK:
		var delivery jobDelivery
		if err := json.NewDecoder(resp.Body).Decode(&delivery); err != nil {
			return nil, err
		}
		return &delivery, nil
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("poll returned %d: %s", resp.StatusCode, msg)
	}
}

// --- job execution ---

func (a *Agent) execute(ctx context.Context, delivery *jobDelivery) {
	job := &delivery.Job
	log := a.log.With("job_id", job.ID, "runner_id", a.id, "repository", job.Repository)
	log.Info("job received", "commit", job.CommitSHA, "branch", job.Branch)

	// A context for this job alone, so a cancellation kills the container without
	// stopping the agent.
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	current := &activeJob{id: job.ID, cancel: cancelJob}
	a.setActive(current)
	defer a.clearActive(current)

	if err := a.do(ctx, http.MethodPost, "/api/v1/jobs/"+job.ID+"/start", nil, nil); err != nil {
		log.Error("cannot mark job running", "error", err)
		return
	}

	stream := newLogStreamer(jobCtx, 500*time.Millisecond, func(seq int, chunk []byte) {
		// Log shipping deliberately uses the parent context: the tail of a
		// cancelled build is the part worth keeping.
		if err := a.shipLogs(ctx, job.ID, seq, chunk); err != nil {
			log.Warn("cannot ship log chunk", "seq", seq, "error", err)
		}
	})
	fail := func(reason string, err error) {
		fmt.Fprintf(stream, "forgerun: %s: %v\n", reason, err)
		stream.Close()
		log.Error(reason, "error", err)
		a.reportResult(ctx, job.ID, models.JobFailed, -1, reason+": "+err.Error())
	}

	// 1. Fresh workspace, removed no matter how the job ends.
	dir, err := prepareWorkspace(jobCtx, checkout{
		Root:      a.cfg.WorkspaceRoot,
		JobID:     job.ID,
		CloneURL:  job.CloneURL,
		CommitSHA: job.CommitSHA,
		Branch:    job.Branch,
		Token:     delivery.CloneToken,
	})
	if err != nil {
		if current.abandoned.Load() {
			a.abandon(current, stream, log)
			return
		}
		fail("checkout failed", err)
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Error("cannot clean workspace", "dir", dir, "error", err)
		}
	}()
	fmt.Fprintf(stream, "forgerun: checked out %s at %s\n", job.Branch, job.CommitSHA[:8])

	// 2. The build definition comes from the repository itself.
	spec, err := jobspec.ParseFile(filepath.Join(dir, job.ConfigRef))
	if err != nil {
		fail("cannot read "+job.ConfigRef, err)
		return
	}
	step := spec.Primary()
	if step.Image == "" {
		step.Image = a.cfg.DefaultImage
	}
	fmt.Fprintf(stream, "forgerun: running job %q in %s\n\n", step.Name, step.Image)

	// 3. Run it in a throwaway container.
	result, err := a.exec.Execute(jobCtx, executor.Request{
		Job:       *job,
		Spec:      step,
		Workspace: dir,
		Timeout:   a.cfg.JobTimeout,
		CPUs:      a.cfg.JobCPUs,
		MemoryMB:  a.cfg.JobMemoryMB,
		Network:   a.cfg.DockerNetwork,
	}, stream)

	// The lease is over either way; from here on a cancellation changes nothing.
	a.clearActive(current)

	if current.abandoned.Load() {
		a.abandon(current, stream, log)
		return
	}
	stream.Close()

	if err != nil {
		log.Error("execution failed", "error", err)
		a.reportResult(ctx, job.ID, models.JobFailed, -1, err.Error())
		return
	}

	status, msg := models.JobSuccess, ""
	if result.TimedOut {
		status, msg = models.JobFailed, "job exceeded timeout"
	} else if result.ExitCode != 0 {
		status = models.JobFailed
	}
	log.Info("job complete", "status", status, "exit_code", result.ExitCode)
	a.reportResult(ctx, job.ID, status, result.ExitCode, msg)
}

// abandon closes out a job the control plane took away. No result is reported:
// the job already has a terminal status (CANCELLED) or belongs to another runner,
// so posting one would either be rejected or overwrite the truth.
func (a *Agent) abandon(job *activeJob, stream *logStreamer, log *slog.Logger) {
	fmt.Fprint(stream, "\nforgerun: job cancelled by the control plane\n")
	stream.Close()
	log.Info("job abandoned", "job_id", job.id)
}

func (a *Agent) shipLogs(ctx context.Context, jobID string, seq int, chunk []byte) error {
	req, err := a.request(ctx, http.MethodPost, "/api/v1/jobs/"+jobID+"/logs", chunk)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Log-Seq", strconv.Itoa(seq))
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("log chunk %d returned %d: %s", seq, resp.StatusCode, msg)
	}
	return nil
}

func (a *Agent) reportResult(ctx context.Context, jobID string, status models.JobStatus, exitCode int, errMsg string) {
	body, _ := json.Marshal(map[string]any{"status": status, "exit_code": exitCode, "error": errMsg})
	// Reporting must survive a cancelled parent context, otherwise a shutdown
	// mid-job would leave the job RUNNING until the reaper notices.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := a.do(reportCtx, http.MethodPost, "/api/v1/jobs/"+jobID+"/result", body, nil); err != nil {
		a.log.Error("cannot report result", "job_id", jobID, "error", err)
	}
}

// --- active job bookkeeping ---

func (a *Agent) setActive(job *activeJob) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active = job
}

// clearActive only clears the job it was given, so a late call cannot wipe the
// bookkeeping for the next job.
func (a *Agent) clearActive(job *activeJob) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active == job {
		a.active = nil
	}
}

func (a *Agent) currentJob() *activeJob {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active
}

// --- tiny HTTP helper ---

func (a *Agent) request(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.cfg.ServerURL+path, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("X-Runner-ID", a.id)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (a *Agent) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := a.request(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, msg)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
