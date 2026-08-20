// Package executor runs one CI job and reports how it ended.
//
// The interface is what keeps the data plane pluggable: a Kubernetes, Firecracker
// or remote-VM executor implements the same three lines and the runner agent does
// not change.
package executor

import (
	"context"
	"io"
	"time"

	"github.com/openmic/forgerun/internal/jobspec"
	"github.com/openmic/forgerun/internal/models"
)

// Request is everything needed to run a job, already resolved by the runner.
type Request struct {
	Job       models.Job
	Spec      jobspec.Job // image + commands from forgerun.yml
	Workspace string      // host directory holding the checked-out repository
	Timeout   time.Duration
	CPUs      float64
	MemoryMB  int64
	Network   string // Docker network mode; "none" means the job cannot reach the network
}

type Result struct {
	ExitCode int
	TimedOut bool
}

type Executor interface {
	// Execute runs the job to completion, streaming combined output to logs.
	// A non-nil error means the job could not be run; a job that ran and failed
	// is reported through Result.ExitCode instead.
	Execute(ctx context.Context, req Request, logs io.Writer) (Result, error)
}
