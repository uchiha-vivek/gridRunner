package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/openmic/forgerun/internal/github"
	"github.com/openmic/forgerun/internal/models"
	"github.com/openmic/forgerun/internal/store"
)

// handleWebhook is the entry point for all CI work.
//
// Order matters: read the body, verify the signature over those exact bytes, and
// only then parse. Parsing before verifying would mean acting on unauthenticated
// input.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	log := s.log.With("request_id", RequestID(r.Context()))

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "payload too large")
		return
	}
	if err := github.VerifySignature(s.cfg.GitHubWebhookSecret, body, r.Header.Get("X-Hub-Signature-256")); err != nil {
		log.Warn("rejected webhook", "error", err, "remote", r.RemoteAddr)
		writeError(w, http.StatusUnauthorized, "invalid signature")
		return
	}

	eventType := r.Header.Get("X-GitHub-Event")
	event, err := github.ParseEvent(eventType, body)
	if errors.Is(err, github.ErrIgnored) {
		log.Info("webhook ignored", "event", eventType)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
		return
	}
	if err != nil {
		log.Warn("malformed webhook payload", "event", eventType, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	job := &models.Job{
		ID:         uuid.NewString(),
		Repository: event.Repository,
		CloneURL:   event.CloneURL,
		CommitSHA:  event.CommitSHA,
		Branch:     event.Branch,
		EventType:  event.EventType,
		ConfigRef:  "forgerun.yml",
		// Every job needs a Docker-capable Linux runner. Richer requirements
		// (gpu, arm64) will come from forgerun.yml once it is read control-plane
		// side; today the runner enforces them locally.
		Labels: []string{"linux", "docker"},
		Status: models.JobQueued,
	}

	if err := s.store.CreateJob(r.Context(), job); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			// GitHub retries deliveries; a retry must not start a second build.
			log.Info("duplicate delivery ignored", "repository", job.Repository, "commit", job.CommitSHA)
			writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
			return
		}
		log.Error("cannot create job", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot create job")
		return
	}

	if err := s.queue.Enqueue(r.Context(), job.ID); err != nil {
		// The job row exists but never reached the queue. Fail it loudly rather
		// than leaving a job that is QUEUED forever with nothing to pick it up.
		log.Error("cannot enqueue job", "job_id", job.ID, "error", err)
		_ = s.store.CompleteJob(r.Context(), job.ID, models.JobFailed, -1, "could not be queued")
		writeError(w, http.StatusInternalServerError, "cannot enqueue job")
		return
	}

	log.Info("job queued",
		"job_id", job.ID,
		"repository", job.Repository,
		"commit", job.CommitSHA,
		"branch", job.Branch,
		"event", job.EventType)

	go s.report(job, "pending", "queued by forgerun")

	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID, "status": "QUEUED"})
}
