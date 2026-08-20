package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/queue"
)

type registerRequest struct {
	Name         string   `json:"name"`
	Labels       []string `json:"labels"`
	Architecture string   `json:"architecture"`
	OS           string   `json:"operating_system"`
}

type registerResponse struct {
	RunnerID string `json:"runner_id"`
	Token    string `json:"token"`
}

// handleRegister issues a runner identity and a bearer token.
//
// Registration itself is gated by a shared registration token: without it, anyone
// who can reach the API could join the pool and receive other people's source code.
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	presented := r.Header.Get("X-Registration-Token")
	if subtle.ConstantTimeCompare([]byte(presented), []byte(s.cfg.RunnerRegistrationToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "invalid registration token")
		return
	}

	var req registerRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	token, err := newToken()
	if err != nil {
		s.log.Error("cannot generate runner token", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot generate token")
		return
	}

	runner := &models.Runner{
		ID:           uuid.NewString(),
		Name:         req.Name,
		Status:       models.RunnerIdle,
		Labels:       req.Labels,
		Architecture: req.Architecture,
		OS:           req.OS,
	}
	if err := s.store.RegisterRunner(r.Context(), runner, hashToken(token)); err != nil {
		s.log.Error("runner registration failed", "error", err)
		writeError(w, http.StatusInternalServerError, "registration failed")
		return
	}

	s.log.Info("runner registered",
		"request_id", RequestID(r.Context()),
		"runner_id", runner.ID,
		"runner_name", runner.Name,
		"labels", runner.Labels)

	writeJSON(w, http.StatusCreated, registerResponse{RunnerID: runner.ID, Token: token})
}

type heartbeatRequest struct {
	JobID string `json:"job_id"` // the job this runner believes it is executing
}

type heartbeatResponse struct {
	Status string `json:"status"`
	Cancel bool   `json:"cancel"` // stop work on JobID immediately
}

// handleHeartbeat is liveness and lease renewal in one call.
//
// The response is the control plane's only way to reach a busy runner: a runner
// that is executing a job has stopped polling for work, so the heartbeat is where
// it learns that its job was cancelled or taken away from it. Answering on the
// runner's own request keeps every connection outbound, so a runner still needs
// no inbound connectivity.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Heartbeat(r.Context(), id); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "unknown runner")
			return
		}
		s.log.Error("heartbeat failed", "runner_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "heartbeat failed")
		return
	}

	// The body is optional: an idle runner has no lease to renew.
	var req heartbeatRequest
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req)

	resp := heartbeatResponse{Status: "ok"}
	if req.JobID != "" && s.leaseLost(r, id, req.JobID) {
		resp.Cancel = true
		s.log.Info("telling runner to abandon job", "runner_id", id, "job_id", req.JobID)
	}
	writeJSON(w, http.StatusOK, resp)
}

// leaseLost reports whether a runner must stop working on a job: it was
// cancelled, it already reached a terminal state, or the reaper decided the
// runner was dead and gave the job to someone else. Saying so here is what stops
// two runners executing the same job after a requeue, and what makes cancelling a
// running build actually kill the container.
func (s *Server) leaseLost(r *http.Request, runnerID, jobID string) bool {
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		return true // an unknown job is nothing worth continuing
	}
	return job.Status.Terminal() || job.RunnerID == nil || *job.RunnerID != runnerID
}

// handlePollJob is how a job reaches a runner: the runner long-polls, the API
// reads that runner's Redis mailbox on its behalf.
//
// Long-polling over HTTP means runners need no inbound connectivity and no Redis
// access, so a runner can sit on any machine behind any NAT.
func (s *Server) handlePollJob(w http.ResponseWriter, r *http.Request) {
	runnerID := r.PathValue("id")

	jobID, err := s.queue.Receive(r.Context(), runnerID, 25*time.Second)
	if errors.Is(err, queue.ErrEmpty) {
		w.WriteHeader(http.StatusNoContent) // no work; the runner polls again
		return
	}
	if err != nil {
		if r.Context().Err() != nil {
			return // client went away mid-poll
		}
		s.log.Error("mailbox read failed", "runner_id", runnerID, "error", err)
		writeError(w, http.StatusInternalServerError, "cannot read mailbox")
		return
	}

	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		s.log.Error("assigned job disappeared", "runner_id", runnerID, "job_id", jobID, "error", err)
		writeError(w, http.StatusInternalServerError, "job not found")
		return
	}
	// A private repository needs a credential to clone. It is minted per delivery,
	// scoped to this one repository, read-only, and expires within the hour, so a
	// compromised runner cannot use it to reach anything else. Public repositories
	// get an empty token and an anonymous clone.
	delivery := jobDelivery{Job: job}
	token, err := s.github.CloneToken(r.Context(), job.Repository)
	if err != nil {
		s.log.Warn("cannot mint clone token, falling back to anonymous checkout",
			"job_id", job.ID, "repository", job.Repository, "error", err)
	}
	delivery.CloneToken = token

	s.log.Info("job delivered to runner",
		"job_id", job.ID, "runner_id", runnerID, "authenticated_checkout", token != "")
	writeJSON(w, http.StatusOK, delivery)
}

// jobDelivery is a job plus the credentials needed to run it. The embedded
// pointer marshals inline, so a runner still receives a plain job object with one
// extra field. The token is never stored and never logged.
type jobDelivery struct {
	*models.Job
	CloneToken string `json:"clone_token,omitempty"`
}

func (s *Server) handleListRunners(w http.ResponseWriter, r *http.Request) {
	runners, err := s.store.ListRunners(r.Context())
	if err != nil {
		s.log.Error("cannot list runners", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot list runners")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": runners})
}

func (s *Server) handleGetRunner(w http.ResponseWriter, r *http.Request) {
	runner, err := s.store.GetRunner(r.Context(), r.PathValue("id"))
	if isNotFound(err) {
		writeError(w, http.StatusNotFound, "runner not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load runner")
		return
	}
	writeJSON(w, http.StatusOK, runner)
}

func newToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
