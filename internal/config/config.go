// Package config loads all runtime configuration from environment variables.
// There is no config file: everything a process needs comes from the environment,
// which keeps local development, docker-compose and any future orchestrator identical.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Control plane
	DatabaseURL string
	RedisURL    string
	ServerPort  string

	// Runner agent
	ServerURL    string // control plane base URL the runner talks to
	RunnerName   string
	RunnerLabels string

	// GitHub. An App is preferred: it can mint short-lived, repository-scoped
	// tokens, which is what makes private-repository checkout safe. A personal
	// access token is the fallback, and neither is required for local work.
	GitHubWebhookSecret     string
	GitHubToken             string
	GitHubAppID             string
	GitHubAppPrivateKey     string // PEM contents
	GitHubAppPrivateKeyPath string // or a path to the .pem file

	// Security / limits
	RunnerRegistrationToken string
	JobTimeout              time.Duration
	HeartbeatTimeout        time.Duration

	// Execution
	DockerNetwork string
	DefaultImage  string
	WorkspaceRoot string
	JobCPUs       float64
	JobMemoryMB   int64

	LogLevel string
}

func Load() Config {
	return Config{
		DatabaseURL: env("DATABASE_URL", "postgres://forgerun:forgerun@localhost:5432/forgerun?sslmode=disable"),
		RedisURL:    env("REDIS_URL", "redis://localhost:6379/0"),
		ServerPort:  env("SERVER_PORT", "8080"),

		ServerURL:    env("FORGERUN_SERVER_URL", "http://localhost:8080"),
		RunnerName:   env("RUNNER_NAME", hostname()),
		RunnerLabels: env("RUNNER_LABELS", "linux,amd64,docker"),

		GitHubWebhookSecret:     env("GITHUB_WEBHOOK_SECRET", ""),
		GitHubToken:             env("GITHUB_TOKEN", ""),
		GitHubAppID:             env("GITHUB_APP_ID", ""),
		GitHubAppPrivateKey:     env("GITHUB_APP_PRIVATE_KEY", ""),
		GitHubAppPrivateKeyPath: env("GITHUB_APP_PRIVATE_KEY_PATH", ""),

		RunnerRegistrationToken: env("RUNNER_REGISTRATION_TOKEN", "dev-registration-token"),
		JobTimeout:              envDuration("JOB_TIMEOUT", 10*time.Minute),
		HeartbeatTimeout:        envDuration("HEARTBEAT_TIMEOUT", 45*time.Second),

		DockerNetwork: env("DOCKER_NETWORK", "none"), // jobs get no network by default
		DefaultImage:  env("DEFAULT_IMAGE", "alpine:3.20"),
		WorkspaceRoot: env("WORKSPACE_ROOT", os.TempDir()),
		JobCPUs:       envFloat("JOB_CPUS", 1.0),
		JobMemoryMB:   int64(envInt("JOB_MEMORY_MB", 1024)),

		LogLevel: env("LOG_LEVEL", "info"),
	}
}

// Validate checks the settings the control plane cannot start without.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.RedisURL == "" {
		return fmt.Errorf("REDIS_URL is required")
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "runner"
	}
	return h
}
