package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/openmic/forgerun/internal/models"
)

func at(min int) *time.Time {
	t := time.Date(2026, 1, 1, 0, min, 0, 0, time.UTC)
	return &t
}

func TestSchedulePicksLabelMatch(t *testing.T) {
	job := models.Job{Labels: []string{"linux", "gpu"}}
	candidates := []models.Runner{
		{ID: "a", Status: models.RunnerIdle, Labels: []string{"linux", "amd64"}},
		{ID: "b", Status: models.RunnerIdle, Labels: []string{"linux", "gpu", "amd64"}},
	}
	got, err := CapabilityScheduler{}.Schedule(job, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "b" {
		t.Errorf("picked %s, want the gpu runner b", got.ID)
	}
}

func TestScheduleIsNotFirstFit(t *testing.T) {
	job := models.Job{Labels: []string{"linux"}}
	candidates := []models.Runner{
		{ID: "busy-recently", Status: models.RunnerIdle, Labels: []string{"linux"}, LastAssigned: at(30)},
		{ID: "idle-longest", Status: models.RunnerIdle, Labels: []string{"linux"}, LastAssigned: at(5)},
	}
	got, err := CapabilityScheduler{}.Schedule(job, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "idle-longest" {
		t.Errorf("picked %s, want the least recently used runner", got.ID)
	}
}

func TestScheduleFavoursNeverUsedRunner(t *testing.T) {
	job := models.Job{}
	candidates := []models.Runner{
		{ID: "used", Status: models.RunnerIdle, LastAssigned: at(1)},
		{ID: "fresh", Status: models.RunnerIdle},
	}
	got, err := CapabilityScheduler{}.Schedule(job, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "fresh" {
		t.Errorf("picked %s, want the never-used runner", got.ID)
	}
}

func TestScheduleSkipsUnusableRunners(t *testing.T) {
	job := models.Job{Labels: []string{"linux"}}
	candidates := []models.Runner{
		{ID: "busy", Status: models.RunnerBusy, Labels: []string{"linux"}},
		{ID: "draining", Status: models.RunnerDraining, Labels: []string{"linux"}},
		{ID: "wrong-labels", Status: models.RunnerIdle, Labels: []string{"windows"}},
	}
	if _, err := (CapabilityScheduler{}).Schedule(job, candidates); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("expected ErrNoRunner, got %v", err)
	}
}

func TestScheduleIsDeterministic(t *testing.T) {
	job := models.Job{}
	candidates := []models.Runner{
		{ID: "b", Status: models.RunnerIdle},
		{ID: "a", Status: models.RunnerIdle},
	}
	for i := 0; i < 10; i++ {
		got, err := CapabilityScheduler{}.Schedule(job, candidates)
		if err != nil || got.ID != "a" {
			t.Fatalf("unstable pick: %v %v", got, err)
		}
	}
}
