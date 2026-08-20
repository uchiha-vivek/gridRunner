package models

import "time"

type RunnerStatus string

const (
	RunnerIdle     RunnerStatus = "IDLE"
	RunnerBusy     RunnerStatus = "BUSY"
	RunnerOffline  RunnerStatus = "OFFLINE"
	RunnerDraining RunnerStatus = "DRAINING"
)

var runnerTransitions = map[RunnerStatus][]RunnerStatus{
	RunnerIdle:     {RunnerBusy, RunnerOffline, RunnerDraining},
	RunnerBusy:     {RunnerIdle, RunnerOffline, RunnerDraining},
	RunnerDraining: {RunnerOffline, RunnerIdle},
	RunnerOffline:  {RunnerIdle}, // a runner that comes back re-registers as IDLE
}

func (s RunnerStatus) CanTransitionTo(next RunnerStatus) bool {
	for _, allowed := range runnerTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

type Runner struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Status        RunnerStatus `json:"status"`
	Labels        []string     `json:"labels"`
	Architecture  string       `json:"architecture"`
	OS            string       `json:"operating_system"`
	CurrentJobID  *string      `json:"current_job_id,omitempty"`
	LastHeartbeat time.Time    `json:"last_heartbeat"`
	LastAssigned  *time.Time   `json:"last_assigned_at,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
}

// CanRun reports whether the runner carries every label the job asked for.
// Labels are the only scheduling constraint in the MVP; GPU pools, arch pools
// and OS pools are all expressed as labels rather than new concepts.
func (r Runner) CanRun(required []string) bool {
	have := make(map[string]bool, len(r.Labels))
	for _, l := range r.Labels {
		have[l] = true
	}
	for _, need := range required {
		if !have[need] {
			return false
		}
	}
	return true
}
