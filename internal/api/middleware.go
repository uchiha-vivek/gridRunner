package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openmic/forgerun/internal/store"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

// withRequestID gives every request a correlation id, echoed in the response so a
// user can quote it, and attached to every log line for that request.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func withLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Info("http request",
			"request_id", RequestID(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// withRecovery keeps one panicking handler from taking down the control plane.
func withRecovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in handler",
					"request_id", RequestID(r.Context()),
					"path", r.URL.Path,
					"panic", rec)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// hashToken stores and compares runner tokens as SHA-256 digests, so a database
// dump does not hand out working runner credentials.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// withRunnerAuth authenticates a runner against the token issued at registration.
//
// The runner id comes from the URL for runner-scoped routes, and from the
// X-Runner-ID header for job-scoped routes, since a job path carries a job id.
func (s *Server) withRunnerAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runnerID := r.Header.Get("X-Runner-ID")
		if runnerID == "" {
			runnerID = r.PathValue("id")
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if runnerID == "" || token == "" {
			writeError(w, http.StatusUnauthorized, "missing runner credentials")
			return
		}
		want, err := s.store.RunnerTokenHash(r.Context(), runnerID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unknown runner")
			return
		}
		// Constant-time compare: a plain == leaks the token through timing.
		if subtle.ConstantTimeCompare([]byte(want), []byte(hashToken(token))) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid runner token")
			return
		}
		next(w, r)
	}
}

// runnerOwnsJob makes sure a runner can only touch the job it was assigned.
// Without this, any authenticated runner could complete or corrupt another
// runner's job.
func (s *Server) runnerOwnsJob(r *http.Request, jobID string) bool {
	runnerID := r.Header.Get("X-Runner-ID")
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		return false
	}
	return job.RunnerID != nil && *job.RunnerID == runnerID
}

func isNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }
