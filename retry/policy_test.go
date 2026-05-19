package retry

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPolicyFunc(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("failed")
	policy := PolicyFunc(func(attempt int, err error) (time.Duration, bool) {
		if attempt != 2 {
			t.Fatalf("attempt = %d, want 2", attempt)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
		return time.Second, true
	})

	delay, ok := policy.NextDelay(2, wantErr)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if delay != time.Second {
		t.Fatalf("delay = %s, want 1s", delay)
	}
}

func TestNever(t *testing.T) {
	t.Parallel()

	delay, ok := Never().NextDelay(1, errors.New("failed"))
	if ok {
		t.Fatal("ok = true, want false")
	}
	if delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}

func TestAttempts(t *testing.T) {
	t.Parallel()

	policy := Attempts(3, Constant(10*time.Millisecond), nil)

	tests := []struct {
		name        string
		attempt     int
		wantDelay   time.Duration
		wantAllowed bool
	}{
		{name: "attempt below one normalized", attempt: 0, wantDelay: 10 * time.Millisecond, wantAllowed: true},
		{name: "first failed attempt", attempt: 1, wantDelay: 10 * time.Millisecond, wantAllowed: true},
		{name: "second failed attempt", attempt: 2, wantDelay: 10 * time.Millisecond, wantAllowed: true},
		{name: "max exhausted", attempt: 3, wantAllowed: false},
		{name: "above max exhausted", attempt: 4, wantAllowed: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delay, ok := policy.NextDelay(tc.attempt, errors.New("failed"))
			if ok != tc.wantAllowed {
				t.Fatalf("ok = %v, want %v", ok, tc.wantAllowed)
			}
			if delay != tc.wantDelay {
				t.Fatalf("delay = %s, want %s", delay, tc.wantDelay)
			}
		})
	}
}

func TestAttemptsDisablesRetriesBelowTwoAttempts(t *testing.T) {
	t.Parallel()

	for _, maxAttempts := range []int{-1, 0, 1} {
		maxAttempts := maxAttempts
		t.Run(fmt.Sprint(maxAttempts), func(t *testing.T) {
			t.Parallel()

			delay, ok := Attempts(maxAttempts, Constant(time.Second), nil).NextDelay(1, errors.New("failed"))
			if ok {
				t.Fatalf("maxAttempts=%d ok = true, want false", maxAttempts)
			}
			if delay != 0 {
				t.Fatalf("maxAttempts=%d delay = %s, want 0", maxAttempts, delay)
			}
		})
	}
}

func TestAttemptsIf(t *testing.T) {
	t.Parallel()

	retryableErr := errors.New("retryable")
	terminalErr := errors.New("terminal")
	policy := AttemptsIf(2, Constant(time.Second), nil, func(err error) bool {
		return errors.Is(err, retryableErr)
	})

	delay, ok := policy.NextDelay(1, retryableErr)
	if !ok {
		t.Fatal("retryable ok = false, want true")
	}
	if delay != time.Second {
		t.Fatalf("retryable delay = %s, want 1s", delay)
	}

	delay, ok = policy.NextDelay(1, terminalErr)
	if ok {
		t.Fatal("terminal ok = true, want false")
	}
	if delay != 0 {
		t.Fatalf("terminal delay = %s, want 0", delay)
	}
}

func TestAttemptsIfNilPredicateNeverRetries(t *testing.T) {
	t.Parallel()

	delay, ok := AttemptsIf(3, Constant(time.Second), nil, nil).NextDelay(1, errors.New("failed"))
	if ok {
		t.Fatal("ok = true, want false")
	}
	if delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}

func TestNewComposesBackoffAndJitter(t *testing.T) {
	t.Parallel()

	var backoffAttempt int
	var jitterAttempt int
	policy := New(Config{
		MaxAttempts: 2,
		Backoff: BackoffFunc(func(attempt int) time.Duration {
			backoffAttempt = attempt
			return 10 * time.Millisecond
		}),
		Jitter: JitterFunc(func(base time.Duration, attempt int) time.Duration {
			jitterAttempt = attempt
			if base != 10*time.Millisecond {
				t.Fatalf("jitter base = %s, want 10ms", base)
			}
			return base + time.Millisecond
		}),
	})

	delay, ok := policy.NextDelay(1, errors.New("failed"))
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if delay != 11*time.Millisecond {
		t.Fatalf("delay = %s, want 11ms", delay)
	}
	if backoffAttempt != 1 {
		t.Fatalf("backoff attempt = %d, want 1", backoffAttempt)
	}
	if jitterAttempt != 1 {
		t.Fatalf("jitter attempt = %d, want 1", jitterAttempt)
	}
}

func TestNewNilBackoffAndJitterRetryImmediately(t *testing.T) {
	t.Parallel()

	delay, ok := New(Config{MaxAttempts: 2}).NextDelay(1, errors.New("failed"))
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}

func TestPolicyClampsNegativeDelayAfterJitter(t *testing.T) {
	t.Parallel()

	policy := New(Config{
		MaxAttempts: 2,
		Backoff:     Constant(time.Second),
		Jitter: JitterFunc(func(time.Duration, int) time.Duration {
			return -time.Second
		}),
	})

	delay, ok := policy.NextDelay(1, errors.New("failed"))
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}

func TestConfigPolicyClampsNegativeDelayFromCustomJitter(t *testing.T) {
	t.Parallel()

	policy := configPolicy{
		maxAttempts: 2,
		backoff:     Constant(time.Second),
		jitter: rawJitterFunc(func(time.Duration, int) time.Duration {
			return -time.Second
		}),
	}

	delay, ok := policy.NextDelay(1, errors.New("failed"))
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if delay != 0 {
		t.Fatalf("delay = %s, want 0", delay)
	}
}

type rawJitterFunc func(time.Duration, int) time.Duration

func (f rawJitterFunc) Apply(base time.Duration, attempt int) time.Duration {
	return f(base, attempt)
}
