package models

import "testing"

func TestJobStateTransitions(t *testing.T) {
	valid := []struct{ from, to JobStatus }{
		{JobQueued, JobAssigned},
		{JobAssigned, JobRunning},
		{JobRunning, JobSuccess},
		{JobRunning, JobFailed},
		{JobAssigned, JobQueued}, // runner died before it started
		{JobQueued, JobCancelled},
	}
	for _, c := range valid {
		if !c.from.CanTransitionTo(c.to) {
			t.Errorf("expected %s -> %s to be allowed", c.from, c.to)
		}
	}

	invalid := []struct{ from, to JobStatus }{
		{JobSuccess, JobRunning},
		{JobFailed, JobQueued},
		{JobCancelled, JobRunning},
		{JobQueued, JobSuccess}, // must be scheduled and run first
		{JobQueued, JobRunning},
	}
	for _, c := range invalid {
		if c.from.CanTransitionTo(c.to) {
			t.Errorf("expected %s -> %s to be rejected", c.from, c.to)
		}
	}
}

func TestTerminalStates(t *testing.T) {
	for _, s := range []JobStatus{JobSuccess, JobFailed, JobCancelled} {
		if !s.Terminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []JobStatus{JobQueued, JobAssigned, JobRunning} {
		if s.Terminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestRunnerCanRun(t *testing.T) {
	r := Runner{Labels: []string{"linux", "amd64", "docker"}}
	if !r.CanRun([]string{"linux", "docker"}) {
		t.Error("runner should satisfy a subset of its labels")
	}
	if r.CanRun([]string{"gpu"}) {
		t.Error("runner without gpu label should not match")
	}
	if !r.CanRun(nil) {
		t.Error("a job with no label requirements should run anywhere")
	}
}

func TestRunnerStateTransitions(t *testing.T) {
	if !RunnerIdle.CanTransitionTo(RunnerBusy) {
		t.Error("IDLE -> BUSY should be allowed")
	}
	if RunnerOffline.CanTransitionTo(RunnerBusy) {
		t.Error("an OFFLINE runner must re-register before taking work")
	}
}
