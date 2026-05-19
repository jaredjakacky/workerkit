package retry

import "time"

// Policy decides whether a failed operation should be retried and how long the
// caller should wait before the next attempt.
//
// The attempt argument is 1-based and represents the failed attempt that just
// completed. A policy that returns ok=false tells the caller to stop retrying.
type Policy interface {
	NextDelay(attempt int, err error) (delay time.Duration, ok bool)
}

// PolicyFunc adapts a function into a Policy.
type PolicyFunc func(attempt int, err error) (delay time.Duration, ok bool)

// NextDelay implements Policy.
func (f PolicyFunc) NextDelay(attempt int, err error) (time.Duration, bool) {
	return f(attempt, err)
}

// RetryableFunc reports whether a given error should be retried.
type RetryableFunc func(error) bool

// Config describes a bounded retry policy.
//
// A zero Config disables retries. Setting MaxAttempts without Backoff causes
// immediate retries, which is rarely appropriate for distributed systems.
type Config struct {
	// MaxAttempts is the total number of attempts, including the first call.
	// Values below 2 disable retries.
	MaxAttempts int

	// Backoff computes the delay before the next attempt. Nil means retry
	// immediately.
	Backoff Backoff

	// Jitter perturbs the backoff delay to reduce synchronized retries. Nil
	// leaves the backoff delay unchanged.
	Jitter Jitter

	// Retryable decides whether an error should be retried. Nil retries every
	// error until MaxAttempts is exhausted.
	Retryable RetryableFunc
}

// New constructs a Policy from cfg.
//
// A zero Config returns a policy that never retries.
func New(cfg Config) Policy {
	return configPolicy{
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		jitter:      cfg.Jitter,
		retryable:   cfg.Retryable,
	}
}

// Attempts returns a bounded Policy that retries every failure.
//
// maxAttempts includes the first attempt. Values below 2 disable retries.
func Attempts(maxAttempts int, backoff Backoff, jitter Jitter) Policy {
	return New(Config{
		MaxAttempts: maxAttempts,
		Backoff:     backoff,
		Jitter:      jitter,
	})
}

// AttemptsIf returns a bounded Policy that retries failures accepted by
// retryable.
//
// maxAttempts includes the first attempt. Values below 2 disable retries. A nil
// retryable predicate disables retries.
func AttemptsIf(maxAttempts int, backoff Backoff, jitter Jitter, retryable RetryableFunc) Policy {
	if retryable == nil {
		return Never()
	}
	return New(Config{
		MaxAttempts: maxAttempts,
		Backoff:     backoff,
		Jitter:      jitter,
		Retryable:   retryable,
	})
}

// Never returns a Policy that never retries.
func Never() Policy {
	return PolicyFunc(func(int, error) (time.Duration, bool) { return 0, false })
}

type configPolicy struct {
	maxAttempts int
	backoff     Backoff
	jitter      Jitter
	retryable   RetryableFunc
}

func (p configPolicy) NextDelay(attempt int, err error) (time.Duration, bool) {
	if attempt < 1 {
		attempt = 1
	}
	if p.maxAttempts < 2 || attempt >= p.maxAttempts {
		return 0, false
	}
	if p.retryable != nil && !p.retryable(err) {
		return 0, false
	}

	var delay time.Duration
	if p.backoff != nil {
		delay = p.backoff.Delay(attempt)
	}
	if p.jitter != nil {
		delay = p.jitter.Apply(delay, attempt)
	}
	if delay < 0 {
		delay = 0
	}
	return delay, true
}
