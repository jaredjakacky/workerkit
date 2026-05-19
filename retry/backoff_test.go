package retry

import (
	"math"
	"testing"
	"time"
)

func TestBackoffFuncClampsNegativeDelay(t *testing.T) {
	t.Parallel()

	backoff := BackoffFunc(func(int) time.Duration {
		return -time.Second
	})

	if got := backoff.Delay(1); got != 0 {
		t.Fatalf("Delay = %s, want 0", got)
	}
}

func TestConstant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		delay time.Duration
		want  time.Duration
	}{
		{name: "positive", delay: 10 * time.Millisecond, want: 10 * time.Millisecond},
		{name: "zero", delay: 0, want: 0},
		{name: "negative clamps", delay: -time.Second, want: 0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backoff := Constant(tc.delay)
			for _, attempt := range []int{-1, 0, 1, 10} {
				if got := backoff.Delay(attempt); got != tc.want {
					t.Fatalf("Delay(%d) = %s, want %s", attempt, got, tc.want)
				}
			}
		})
	}
}

func TestLinear(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		step    time.Duration
		max     time.Duration
		attempt int
		want    time.Duration
	}{
		{name: "attempt below one is first attempt", step: 10 * time.Millisecond, attempt: 0, want: 10 * time.Millisecond},
		{name: "grows by step", step: 10 * time.Millisecond, attempt: 3, want: 30 * time.Millisecond},
		{name: "zero step", step: 0, attempt: 3, want: 0},
		{name: "negative step clamps", step: -time.Second, attempt: 3, want: 0},
		{name: "max caps delay", step: 10 * time.Millisecond, max: 25 * time.Millisecond, attempt: 3, want: 25 * time.Millisecond},
		{name: "negative max means uncapped", step: 10 * time.Millisecond, max: -time.Second, attempt: 3, want: 30 * time.Millisecond},
		{name: "overflow caps at max duration", step: maxDuration / 2, attempt: 3, want: maxDuration},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := Linear(tc.step, tc.max).Delay(tc.attempt); got != tc.want {
				t.Fatalf("Delay(%d) = %s, want %s", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestExponential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    time.Duration
		multiplier float64
		max        time.Duration
		attempt    int
		want       time.Duration
	}{
		{name: "first attempt", initial: 10 * time.Millisecond, multiplier: 2, attempt: 1, want: 10 * time.Millisecond},
		{name: "attempt below one is first attempt", initial: 10 * time.Millisecond, multiplier: 2, attempt: 0, want: 10 * time.Millisecond},
		{name: "grows exponentially", initial: 10 * time.Millisecond, multiplier: 2, attempt: 4, want: 80 * time.Millisecond},
		{name: "zero initial", initial: 0, multiplier: 2, attempt: 4, want: 0},
		{name: "negative initial clamps", initial: -time.Second, multiplier: 2, attempt: 4, want: 0},
		{name: "multiplier below one becomes one", initial: 10 * time.Millisecond, multiplier: 0.5, attempt: 4, want: 10 * time.Millisecond},
		{name: "nan multiplier becomes one", initial: 10 * time.Millisecond, multiplier: math.NaN(), attempt: 4, want: 10 * time.Millisecond},
		{name: "max caps delay", initial: 10 * time.Millisecond, multiplier: 2, max: 50 * time.Millisecond, attempt: 4, want: 50 * time.Millisecond},
		{name: "negative max means uncapped", initial: 10 * time.Millisecond, multiplier: 2, max: -time.Second, attempt: 4, want: 80 * time.Millisecond},
		{name: "infinite multiplier caps at max duration", initial: time.Second, multiplier: math.Inf(1), attempt: 2, want: maxDuration},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := Exponential(tc.initial, tc.multiplier, tc.max).Delay(tc.attempt); got != tc.want {
				t.Fatalf("Delay(%d) = %s, want %s", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestExponentialCapsLargeFiniteDelay(t *testing.T) {
	t.Parallel()

	got := Exponential(maxDuration/2, 2, 0).Delay(2)
	if got != maxDuration {
		t.Fatalf("Delay = %s, want maxDuration", got)
	}
}

func TestClampDuration(t *testing.T) {
	t.Parallel()

	if got := clampDuration(-time.Nanosecond); got != 0 {
		t.Fatalf("negative clamp = %s, want 0", got)
	}
	if got := clampDuration(time.Second); got != time.Second {
		t.Fatalf("positive clamp = %s, want 1s", got)
	}
}

func TestDurationFromBackoffFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		delay float64
		limit time.Duration
		want  time.Duration
	}{
		{name: "positive", delay: float64(10 * time.Millisecond), limit: time.Second, want: 10 * time.Millisecond},
		{name: "zero", delay: 0, limit: time.Second, want: 0},
		{name: "negative", delay: -1, limit: time.Second, want: 0},
		{name: "nan", delay: math.NaN(), limit: time.Second, want: 0},
		{name: "positive infinity", delay: math.Inf(1), limit: time.Second, want: time.Second},
		{name: "negative infinity", delay: math.Inf(-1), limit: time.Second, want: time.Second},
		{name: "limit", delay: float64(time.Second), limit: time.Second, want: time.Second},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := durationFromBackoffFloat(tc.delay, tc.limit); got != tc.want {
				t.Fatalf("durationFromBackoffFloat() = %s, want %s", got, tc.want)
			}
		})
	}
}
