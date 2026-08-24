package adaptive

import (
	"testing"
	"time"
)

func TestAdaptiveControllerScaling(t *testing.T) {
	ac := NewAdaptiveController(1, 10, 4, 10*time.Millisecond)

	if ac.CurrentWorkers() != 4 {
		t.Fatalf("expected initial workers 4, got %d", ac.CurrentWorkers())
	}

	time.Sleep(15 * time.Millisecond)
	ac.RecordProgress(100)

	time.Sleep(15 * time.Millisecond)
	// Throughput increase #1
	ac.RecordProgress(500)

	time.Sleep(15 * time.Millisecond)
	// Throughput increase #2 -> trigger scale up
	workers, changed := ac.RecordProgress(2000)

	if !changed {
		t.Errorf("expected worker count to change")
	}
	if workers != 5 {
		t.Errorf("expected 5 workers, got %d", workers)
	}
}
