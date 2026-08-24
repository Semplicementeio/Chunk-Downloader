package downloader

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type RetryConfig struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Jitter          bool
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:     5,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
	}
}

func DoWithRetry(ctx context.Context, config RetryConfig, fn func(attempt int) error) error {
	var lastErr error
	interval := config.InitialInterval

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled during retry (attempt %d): %w", attempt, ctx.Err())
		default:
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}

		lastErr = err

		if attempt == config.MaxAttempts {
			break
		}

		sleepDuration := interval
		if config.Jitter {
			jitterDelta := time.Duration(rand.Float64() * float64(sleepDuration) * 0.2)
			sleepDuration = sleepDuration + jitterDelta
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled waiting for retry (attempt %d): %w", attempt, ctx.Err())
		case <-time.After(sleepDuration):
		}

		interval = time.Duration(float64(interval) * config.Multiplier)
		if interval > config.MaxInterval {
			interval = config.MaxInterval
		}
	}

	return fmt.Errorf("failed after %d attempts, last error: %w", config.MaxAttempts, lastErr)
}
