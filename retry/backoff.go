package retry

import (
	"math"
	"time"
)

const maxDuration = time.Duration(1<<63 - 1)

// Backoff computes the base delay for a retry attempt before jitter is
// applied.
//
// The attempt argument is 1-based and represents the failed attempt that just
// completed. Built-in backoffs and BackoffFunc treat negative delays as zero.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// BackoffFunc adapts a function into a Backoff.
type BackoffFunc func(attempt int) time.Duration

// Delay implements Backoff.
func (f BackoffFunc) Delay(attempt int) time.Duration {
	return clampDuration(f(attempt))
}

// Constant returns a Backoff that always waits the same amount of time.
// Negative delays are treated as zero.
func Constant(delay time.Duration) Backoff {
	delay = clampDuration(delay)
	return BackoffFunc(func(int) time.Duration { return delay })
}

// Linear returns a Backoff that grows by one step per attempt.
//
// Negative step and max values are treated as zero. When max is positive, the
// delay is capped at max.
func Linear(step, max time.Duration) Backoff {
	step = clampDuration(step)
	max = clampDuration(max)
	return BackoffFunc(func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		if step == 0 {
			return 0
		}
		limit := maxDuration
		if max > 0 {
			limit = max
		}
		if int64(attempt) > int64(limit)/int64(step) {
			return limit
		}
		return time.Duration(attempt) * step
	})
}

// Exponential returns a Backoff that grows exponentially from the initial
// delay.
//
// Negative initial and max values are treated as zero. Multiplier values below
// 1 or NaN are treated as 1. When max is positive, the delay is capped at max.
func Exponential(initial time.Duration, multiplier float64, max time.Duration) Backoff {
	initial = clampDuration(initial)
	max = clampDuration(max)
	if math.IsNaN(multiplier) || multiplier < 1 {
		multiplier = 1
	}

	return BackoffFunc(func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		if initial == 0 {
			return 0
		}
		limit := maxDuration
		if max > 0 {
			limit = max
		}
		factor := math.Pow(multiplier, float64(attempt-1))
		delay := float64(initial) * factor
		return durationFromBackoffFloat(delay, limit)
	})
}

func clampDuration(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func durationFromBackoffFloat(delay float64, limit time.Duration) time.Duration {
	if math.IsInf(delay, 0) || delay >= float64(limit) {
		return limit
	}
	if math.IsNaN(delay) || delay <= 0 {
		return 0
	}
	return time.Duration(delay)
}
