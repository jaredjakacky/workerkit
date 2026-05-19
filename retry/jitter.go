package retry

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// Jitter perturbs a backoff delay to avoid synchronized retries.
type Jitter interface {
	Apply(base time.Duration, attempt int) time.Duration
}

// JitterFunc adapts a function into a Jitter.
type JitterFunc func(base time.Duration, attempt int) time.Duration

// Apply implements Jitter.
func (f JitterFunc) Apply(base time.Duration, attempt int) time.Duration {
	return clampDuration(f(base, attempt))
}

// None returns a Jitter that leaves the base delay unchanged.
func None() Jitter {
	return JitterFunc(func(base time.Duration, _ int) time.Duration {
		return clampDuration(base)
	})
}

// Full returns a Jitter that randomizes the delay between zero and the base
// delay.
//
// Full jitter is usually the safest default for fleets of workers because it
// spreads retries across the whole backoff window.
func Full() Jitter {
	return FullWithRand(nil)
}

// FullWithRand returns a full Jitter that uses r as its random source.
//
// Most callers should use Full. This constructor is for deterministic
// simulations, tests, and advanced callers that need to supply their own random
// source. Nil r uses a private source. The returned Jitter serializes access to
// r so a shared retry policy can be used by multiple goroutines.
func FullWithRand(r *rand.Rand) Jitter {
	random := newLockedRand(r)
	return JitterFunc(func(base time.Duration, _ int) time.Duration {
		base = clampDuration(base)
		if base == 0 {
			return 0
		}
		return durationFromFloat(random.Float64() * float64(base))
	})
}

// Symmetric returns a Jitter that perturbs the delay around its base value by
// the provided fraction.
//
// For example, a fraction of 0.25 produces a delay between base*0.75 and
// base*1.25. Fractions below zero or NaN are treated as zero, and fractions
// above one are treated as one.
func Symmetric(fraction float64) Jitter {
	return SymmetricWithRand(fraction, nil)
}

// SymmetricWithRand returns a symmetric Jitter that uses r as its random source.
//
// Most callers should use Symmetric. This constructor is for deterministic
// simulations, tests, and advanced callers that need to supply their own random
// source. Nil r uses a private source. The returned Jitter serializes access to
// r so a shared retry policy can be used by multiple goroutines.
func SymmetricWithRand(fraction float64, r *rand.Rand) Jitter {
	if math.IsNaN(fraction) || fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	random := newLockedRand(r)
	return JitterFunc(func(base time.Duration, _ int) time.Duration {
		base = clampDuration(base)
		if base == 0 {
			return 0
		}
		min := 1 - fraction
		max := 1 + fraction
		return durationFromFloat(float64(base) * (min + random.Float64()*(max-min)))
	})
}

type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newLockedRand(r *rand.Rand) *lockedRand {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &lockedRand{r: r}
}

func (r *lockedRand) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Float64()
}

func durationFromFloat(delay float64) time.Duration {
	if math.IsNaN(delay) || delay <= 0 {
		return 0
	}
	if math.IsInf(delay, 0) || delay >= float64(maxDuration) {
		return maxDuration
	}
	return time.Duration(delay)
}
