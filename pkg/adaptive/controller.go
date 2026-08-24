package adaptive

import (
	"sync"
	"time"
)

type AdaptiveController struct {
	minWorkers          int
	maxWorkers          int
	currentWorkers      int
	sampleInterval      time.Duration
	lastSampleTime      time.Time
	lastDownloadedBytes int64
	lastThroughput      float64
	increasingTrend     int
	decreasingTrend     int
	mu                  sync.Mutex
}

func NewAdaptiveController(min, max, initial int, sampleInterval time.Duration) *AdaptiveController {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	if initial < min {
		initial = min
	}
	if initial > max {
		initial = max
	}
	if sampleInterval <= 0 {
		sampleInterval = 500 * time.Millisecond
	}

	return &AdaptiveController{
		minWorkers:     min,
		maxWorkers:     max,
		currentWorkers: initial,
		sampleInterval: sampleInterval,
		lastSampleTime: time.Now(),
	}
}

func (ac *AdaptiveController) CurrentWorkers() int {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.currentWorkers
}

func (ac *AdaptiveController) RecordProgress(currentDownloadedBytes int64) (int, bool) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(ac.lastSampleTime)

	if elapsed < ac.sampleInterval {
		return ac.currentWorkers, false
	}

	bytesDiff := currentDownloadedBytes - ac.lastDownloadedBytes
	if bytesDiff < 0 {
		bytesDiff = 0
	}

	throughput := float64(bytesDiff) / elapsed.Seconds() // Bytes per second

	ac.lastSampleTime = now
	ac.lastDownloadedBytes = currentDownloadedBytes

	changed := false

	if ac.lastThroughput > 0 {
		throughputDiffRatio := (throughput - ac.lastThroughput) / ac.lastThroughput

		if throughputDiffRatio > 0.05 {
			ac.increasingTrend++
			ac.decreasingTrend = 0

			if ac.increasingTrend >= 2 && ac.currentWorkers < ac.maxWorkers {
				ac.currentWorkers++
				ac.increasingTrend = 0
				changed = true
			}
		} else if throughputDiffRatio < -0.10 {
			ac.decreasingTrend++
			ac.increasingTrend = 0

			if ac.decreasingTrend >= 2 && ac.currentWorkers > ac.minWorkers {
				ac.currentWorkers--
				ac.decreasingTrend = 0
				changed = true
			}
		} else {
			ac.increasingTrend = 0
			ac.decreasingTrend = 0
		}
	}

	ac.lastThroughput = throughput
	return ac.currentWorkers, changed
}
