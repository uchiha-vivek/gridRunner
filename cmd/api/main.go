// Command api is the control plane's HTTP entry point: GitHub webhooks in,
// runner protocol, and read APIs. It also applies database migrations on boot.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/openmic/forgerun/internal/api"
	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/github"
	"github.com/openmic/forgerun/internal/logging"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/store"
)

func main() {
	// Rollback is a manual escape hatch for a bad deploy; migrations otherwise
	// apply automatically at startup.
	rollback := flag.Int("rollback", 0, "undo the last N migrations and exit")
	flag.Parse()

	cfg := config.Load()
	log := logging.New(cfg.LogLevel).With("service", "api")

	if err := cfg.Validate(); err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// SIGINT/SIGTERM cancel this context, which unwinds everything below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("cannot connect to postgres", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if *rollback > 0 {
		if err := st.Rollback(ctx, *rollback); err != nil {
			log.Error("rollback failed", "error", err)
			os.Exit(1)
		}
		applied, _ := st.AppliedMigrations(ctx)
		log.Info("rolled back", "steps", *rollback, "still_applied", applied)
		return
	}

	if err := st.Migrate(ctx); err != nil {
		log.Error("migrations failed", "error", err)
		os.Exit(1)
	}
	log.Info("database ready")

	q, err := queue.NewRedis(cfg.RedisURL)
	if err != nil {
		log.Error("cannot connect to redis", "error", err)
		os.Exit(1)
	}
	defer q.Close()

	// A GitHub App if one is configured, a personal access token otherwise, and a
	// no-op client when neither is set so local development needs no credentials.
	gh, mode, err := github.Configure(github.Options{
		AppID:          cfg.GitHubAppID,
		PrivateKeyPEM:  cfg.GitHubAppPrivateKey,
		PrivateKeyPath: cfg.GitHubAppPrivateKeyPath,
		Token:          cfg.GitHubToken,
	})
	if err != nil {
		log.Error("invalid github credentials", "error", err)
		os.Exit(1)
	}
	switch mode {
	case "disabled":
		log.Warn("no github credentials, status reporting and private clones are disabled")
	case "personal-access-token":
		log.Info("github authentication ready", "mode", mode,
			"note", "private repositories need a GitHub App")
	default:
		log.Info("github authentication ready", "mode", mode)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           api.NewServer(cfg, st, q, gh, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		// Long enough to cover the 25s job long-poll.
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  90 * time.Second,
	}

	go func() {
		log.Info("api listening", "port", cfg.ServerPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down, draining in-flight requests")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	log.Info("api stopped")
}
