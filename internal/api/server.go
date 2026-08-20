// Package api is the control-plane HTTP surface: GitHub webhooks, the runner
// protocol, and read APIs for humans.
//
// Handlers stay thin. They decode, authorise, call the store or queue, and encode.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/github"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/store"
)

type Server struct {
	cfg    config.Config
	store  *store.Store
	queue  queue.Queue
	github github.Client
	log    *slog.Logger
}

func NewServer(cfg config.Config, st *store.Store, q queue.Queue, gh github.Client, log *slog.Logger) *Server {
	return &Server{cfg: cfg, store: st, queue: q, github: gh, log: log}
}

// Routes uses the standard library's method+pattern routing (Go 1.22), which
// removes the need for a third-party router entirely.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)

	mux.HandleFunc("POST /webhooks/github", s.handleWebhook)

	// Runner protocol (authenticated with the token issued at registration).
	mux.HandleFunc("POST /api/v1/runners/register", s.handleRegister)
	mux.HandleFunc("POST /api/v1/runners/{id}/heartbeat", s.withRunnerAuth(s.handleHeartbeat))
	mux.HandleFunc("GET /api/v1/runners/{id}/job", s.withRunnerAuth(s.handlePollJob))
	mux.HandleFunc("POST /api/v1/jobs/{id}/start", s.withRunnerAuth(s.handleJobStart))
	mux.HandleFunc("POST /api/v1/jobs/{id}/logs", s.withRunnerAuth(s.handleJobLogs))
	mux.HandleFunc("POST /api/v1/jobs/{id}/result", s.withRunnerAuth(s.handleJobResult))

	// Read/operate APIs.
	mux.HandleFunc("GET /api/v1/runners", s.handleListRunners)
	mux.HandleFunc("GET /api/v1/runners/{id}", s.handleGetRunner)
	mux.HandleFunc("GET /api/v1/jobs", s.handleListJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/logs", s.handleGetJobLogs)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", s.handleCancelJob)

	return withRequestID(withLogging(s.log, withRecovery(s.log, mux)))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady probes the dependencies. Liveness (/health) deliberately does not,
// so a Redis blip restarts nothing.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"postgres": "ok", "redis": "ok"}
	code := http.StatusOK

	if err := s.store.Ping(ctx); err != nil {
		checks["postgres"] = err.Error()
		code = http.StatusServiceUnavailable
	}
	if p, ok := s.queue.(interface {
		Ping(context.Context) error
	}); ok {
		if err := p.Ping(ctx); err != nil {
			checks["redis"] = err.Error()
			code = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, checks)
}

// --- small helpers shared by every handler ---

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
