// Command runner is the data plane. It registers with the control plane, waits
// for work, and executes each job in a throwaway Docker container.
//
//	go run ./cmd/runner --server http://localhost:8080 --labels linux,docker
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/openmic/forgerun/internal/config"
	"github.com/openmic/forgerun/internal/executor"
	"github.com/openmic/forgerun/internal/logging"
	"github.com/openmic/forgerun/internal/runner"
)

func main() {
	cfg := config.Load()

	// Flags override the environment, so one machine can host several runners.
	flag.StringVar(&cfg.ServerURL, "server", cfg.ServerURL, "control plane base URL")
	flag.StringVar(&cfg.RunnerName, "name", cfg.RunnerName, "runner name")
	flag.StringVar(&cfg.RunnerLabels, "labels", cfg.RunnerLabels, "comma-separated capability labels")
	flag.Parse()

	log := logging.New(cfg.LogLevel).With("service", "runner", "runner_name", cfg.RunnerName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exec, err := executor.NewDocker()
	if err != nil {
		log.Error("cannot reach the docker engine", "error", err)
		os.Exit(1)
	}
	defer exec.Close()

	if err := exec.Ping(ctx); err != nil {
		log.Error("docker engine is not responding", "error", err)
		os.Exit(1)
	}

	if err := runner.New(cfg, exec, log).Run(ctx); err != nil {
		log.Error("runner exited with an error", "error", err)
		os.Exit(1)
	}
}
