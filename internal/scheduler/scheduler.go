// Package scheduler decides which runner gets which job, and drives the loop
// that moves jobs from the queue onto runners.
package scheduler

import (
	"errors"

	"github.com/openmic/forgerun/internal/models"
)

// ErrNoRunner means no live runner can satisfy the job right now. It is a
// back-pressure signal, not a failure: the job goes back on the queue.
var ErrNoRunner = errors.New("no compatible runner available")

// Scheduler picks a runner for a job out of the currently available pool.
type Scheduler interface {
	Schedule(job models.Job, candidates []models.Runner) (*models.Runner, error)
}

// CapabilityScheduler filters by labels, then picks the least recently used
// runner among the matches.
//
// Two deliberate choices:
//   - Filtering is on labels only, so "GPU pool" or "ARM pool" needs no new
//     concept in the scheduler; it is a label on a runner and on a job.
//   - Among equally capable runners we take the one idle the longest rather than
//     the first in the list, which spreads load and surfaces broken runners
//     instead of hammering whichever one the database happened to return first.
type CapabilityScheduler struct{}

func (CapabilityScheduler) Schedule(job models.Job, candidates []models.Runner) (*models.Runner, error) {
	var best *models.Runner
	for i := range candidates {
		r := &candidates[i]
		if r.Status != models.RunnerIdle || !r.CanRun(job.Labels) {
			continue
		}
		if best == nil || lessRecentlyUsed(*r, *best) {
			best = r
		}
	}
	if best == nil {
		return nil, ErrNoRunner
	}
	return best, nil
}

// lessRecentlyUsed reports whether a should be preferred over b.
// A runner that has never run a job wins, so new capacity is used immediately.
func lessRecentlyUsed(a, b models.Runner) bool {
	switch {
	case a.LastAssigned == nil && b.LastAssigned == nil:
		return a.ID < b.ID // stable tie-break, so tests and behaviour are deterministic
	case a.LastAssigned == nil:
		return true
	case b.LastAssigned == nil:
		return false
	default:
		return a.LastAssigned.Before(*b.LastAssigned)
	}
}
