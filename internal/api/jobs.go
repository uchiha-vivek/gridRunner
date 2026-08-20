package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/store"
)

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 500 {
		limit = v
	}
	jobs, err := s.store.ListJobs(r.Context(), limit)
	if err != nil {
		s.log.Error("cannot list jobs", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("id"))
	if isNotFound(err) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	logs, err := s.store.GetLogs(r.Context(), r.PathValue("id"))
	if isNotFound(err) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load logs")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, logs)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.store.CancelJob(r.Context(), id)
	switch {
	case isNotFound(err):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrInvalidTransition):
		// Cancelling a finished job is a client mistake, not a server error.
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		s.log.Error("cancel failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "cancel failed")
	default:
		s.log.Info("job cancelled", "job_id", id, "request_id", RequestID(r.Context()))
		writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED"})
	}
}

// --- runner-facing job lifecycle ---

func (s *Server) handleJobStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.runnerOwnsJob(r, id) {
		writeError(w, http.StatusForbidden, "job is not assigned to this runner")
		return
	}
	if err := s.store.StartJob(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrInvalidTransition) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		s.log.Error("cannot start job", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "cannot start job")
		return
	}
	s.log.Info("job started", "job_id", id, "runner_id", r.Header.Get("X-Runner-ID"))

	// Tell GitHub a build is in progress. Best effort: a reporting outage must
	// not stop the job from running.
	if job, err := s.store.GetJob(r.Context(), id); err == nil {
		go s.report(job, "pending", "build running")
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "RUNNING"})
}

// handleJobLogs appends a chunk of container output.
//
// X-Log-Seq is the runner's chunk counter. Chunks arrive in order from a single
// runner, so the sequence number is not for sorting: it makes the append
// idempotent, so a chunk the runner re-sends after a timeout it never saw the
// response to is stored once rather than twice.
func (s *Server) handleJobLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.runnerOwnsJob(r, id) {
		writeError(w, http.StatusForbidden, "job is not assigned to this runner")
		return
	}
	seq, err := strconv.Atoi(r.Header.Get("X-Log-Seq"))
	if err != nil || seq <= 0 {
		writeError(w, http.StatusBadRequest, "X-Log-Seq must be a positive integer")
		return
	}
	// Cap a single chunk so a runaway job cannot exhaust memory or disk in one POST.
	chunk, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "log chunk too large")
		return
	}
	if err := s.store.AppendLogs(r.Context(), id, seq, string(chunk)); err != nil {
		s.log.Error("cannot append logs", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "cannot append logs")
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type resultRequest struct {
	Status   string `json:"status"` // SUCCESS or FAILED
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error"`
}

func (s *Server) handleJobResult(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.runnerOwnsJob(r, id) {
		writeError(w, http.StatusForbidden, "job is not assigned to this runner")
		return
	}
	var req resultRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	status, err := models.ParseJobStatus(req.Status)
	if err != nil || (status != models.JobSuccess && status != models.JobFailed) {
		writeError(w, http.StatusBadRequest, "status must be SUCCESS or FAILED")
		return
	}

	if err := s.store.CompleteJob(r.Context(), id, status, req.ExitCode, req.Error); err != nil {
		if errors.Is(err, store.ErrInvalidTransition) {
			// The job was cancelled while it ran: accept the report, keep the
			// terminal state we already have.
			s.log.Info("late result for a finished job, ignoring", "job_id", id, "error", err)
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		s.log.Error("cannot complete job", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "cannot complete job")
		return
	}

	s.log.Info("job finished",
		"job_id", id,
		"runner_id", r.Header.Get("X-Runner-ID"),
		"status", status,
		"exit_code", req.ExitCode)

	if job, err := s.store.GetJob(r.Context(), id); err == nil {
		state, desc := "success", "build passed"
		if status == models.JobFailed {
			state, desc = "failure", "build failed with exit code "+strconv.Itoa(req.ExitCode)
		}
		go s.report(job, state, desc)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(status)})
}

// report posts a commit status to GitHub. It runs off the request goroutine with
// its own context so a slow GitHub never holds up a runner.

func (s *Server) report(job *models.Job, state, description string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	target := s.cfg.ServerURL + "/api/v1/jobs/" + job.ID + "/logs"
	if err := s.github.SetStatus(ctx, job.Repository, job.CommitSHA, state, description, target); err != nil {
		s.log.Error("cannot report status to github",
			"job_id", job.ID, "repository", job.Repository, "state", state, "error", err)
		return
	}
	s.log.Info("github status reported", "job_id", job.ID, "state", state)
}
