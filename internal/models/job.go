package models

import (
	"fmt"
	"time"
)

type JobStatus string

const (
	JobQueued    JobStatus = "QUEUED"
	JobAssigned  JobStatus = "ASSIGNED"
	JobRunning   JobStatus = "RUNNING"
	JobSuccess   JobStatus = "SUCCESS"
	JobFailed    JobStatus = "FAILED"
	JobCancelled JobStatus = "CANCELLED"
)

// jobTransitions is the whole job state machine. Anything not listed here is
// rejected, which is what stops a crashed runner from resurrecting a finished job.
var jobTransitions = map[JobStatus][]JobStatus{
	JobQueued:    {JobAssigned, JobCancelled},
	JobAssigned:  {JobRunning, JobQueued, JobFailed, JobCancelled}, // back to QUEUED when a runner dies
	JobRunning:   {JobSuccess, JobFailed, JobCancelled, JobQueued},
	JobSuccess:   {},
	JobFailed:    {},
	JobCancelled: {},
}

func (s JobStatus) CanTransitionTo(next JobStatus) bool {
	for _, allowed := range jobTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

func (s JobStatus) Terminal() bool { return len(jobTransitions[s]) == 0 }

type Job struct {
	ID          string     `json:"id"`
	Repository  string     `json:"repository"` // "owner/name"
	CloneURL    string     `json:"clone_url"`
	CommitSHA   string     `json:"commit_sha"`
	Branch      string     `json:"branch"`
	EventType   string     `json:"event_type"` // push | pull_request
	ConfigRef   string     `json:"config_ref"` // path to forgerun.yml in the repo
	Labels      []string   `json:"labels"`     // capabilities the job requires
	Status      JobStatus  `json:"status"`
	RunnerID    *string    `json:"runner_id,omitempty"`
	Attempts    int        `json:"attempts"`
	ExitCode    *int       `json:"exit_code,omitempty"`
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

func ParseJobStatus(s string) (JobStatus, error) {
	switch JobStatus(s) {
	case JobQueued, JobAssigned, JobRunning, JobSuccess, JobFailed, JobCancelled:
		return JobStatus(s), nil
	}
	return "", fmt.Errorf("unknown job status %q", s)
}
