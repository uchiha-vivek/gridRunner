// Command scheduler pulls queued jobs off Redis and assigns them to runners.
// It is a separate process from the API so scheduling can be scaled, restarted
// or rewritten without touching the request path.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/logging"
	"github.com/openmic/forgerun/internal/queue"
	"github.com/openmic/forgerun/internal/scheduler"
	"github.com/openmic/forgerun/internal/store"
)

func main() {
	cfg := config.Load()
	log := logging.New(cfg.LogLevel).With("service", "scheduler")

	if err := cfg.Validate(); err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("cannot connect to postgres", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	q, err := queue.NewRedis(cfg.RedisURL)
	if err != nil {
		log.Error("cannot connect to redis", "error", err)
		os.Exit(1)
	}
	defer q.Close()

	svc := scheduler.NewService(st, q, scheduler.CapabilityScheduler{}, log, cfg.HeartbeatTimeout)
	if err := svc.Run(ctx); err != nil {
		log.Error("scheduler exited with an error", "error", err)
		os.Exit(1)
	}
}
