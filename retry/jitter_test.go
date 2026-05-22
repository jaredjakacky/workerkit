package retry_test

import (
	. "github.com/jaredjakacky/workerkit/retry"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestJitterFuncClampsNegativeDelay(t *testing.T) {
	t.Parallel()

	jitter := JitterFunc(func(time.Duration, int) time.Duration {
		return -time.Second
	})

	if got := jitter.Apply(time.Second, 1); got != 0 {
		t.Fatalf("Apply = %s, want 0", got)
	}
}

func TestNone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base time.Duration
		want time.Duration
	}{
		{name: "positive", base: time.Second, want: time.Second},
		{name: "zero", base: 0, want: 0},
		{name: "negative clamps", base: -time.Second, want: 0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := None().Apply(tc.base, 1); got != tc.want {
				t.Fatalf("Apply(%s) = %s, want %s", tc.base, got, tc.want)
			}
		})
	}
}

func TestFullWithRand(t *testing.T) {
	t.Parallel()

	base := time.Second
	source := rand.New(rand.NewSource(7))
	want := time.Duration(rand.New(rand.NewSource(7)).Float64() * float64(base))
	got := FullWithRand(source).Apply(base, 3)
	if got != want {
		t.Fatalf("Apply = %s, want deterministic %s", got, want)
	}
	if got < 0 || got > base {
		t.Fatalf("Apply = %s, want within [0,%s]", got, base)
	}
}

func TestFullWithRandZeroAndNegativeBase(t *testing.T) {
	t.Parallel()

	jitter := FullWithRand(rand.New(rand.NewSource(1)))
	if got := jitter.Apply(0, 1); got != 0 {
		t.Fatalf("zero base Apply = %s, want 0", got)
	}
	if got := jitter.Apply(-time.Second, 1); got != 0 {
		t.Fatalf("negative base Apply = %s, want 0", got)
	}
}

func TestFullUsesPrivateRand(t *testing.T) {
	t.Parallel()

	got := Full().Apply(time.Second, 1)
	if got < 0 || got > time.Second {
		t.Fatalf("Apply = %s, want within [0,1s]", got)
	}
}

func TestSymmetricWithRand(t *testing.T) {
	t.Parallel()

	base := time.Second
	fraction := 0.25
	randomValue := rand.New(rand.NewSource(11)).Float64()
	want := time.Duration(float64(base) * ((1 - fraction) + randomValue*((1+fraction)-(1-fraction))))
	got := SymmetricWithRand(fraction, rand.New(rand.NewSource(11))).Apply(base, 2)
	if got != want {
		t.Fatalf("Apply = %s, want deterministic %s", got, want)
	}
	if got < 750*time.Millisecond || got > 1250*time.Millisecond {
		t.Fatalf("Apply = %s, want within symmetric bounds", got)
	}
}

func TestSymmetricFractionNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fraction float64
		min      time.Duration
		max      time.Duration
	}{
		{name: "negative becomes zero", fraction: -0.5, min: time.Second, max: time.Second},
		{name: "nan becomes zero", fraction: math.NaN(), min: time.Second, max: time.Second},
		{name: "above one becomes one", fraction: 2, min: 0, max: 2 * time.Second},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := SymmetricWithRand(tc.fraction, rand.New(rand.NewSource(3))).Apply(time.Second, 1)
			if got < tc.min || got > tc.max {
				t.Fatalf("Apply = %s, want within [%s,%s]", got, tc.min, tc.max)
			}
		})
	}
}

func TestSymmetricWithRandZeroAndNegativeBase(t *testing.T) {
	t.Parallel()

	jitter := SymmetricWithRand(0.5, rand.New(rand.NewSource(1)))
	if got := jitter.Apply(0, 1); got != 0 {
		t.Fatalf("zero base Apply = %s, want 0", got)
	}
	if got := jitter.Apply(-time.Second, 1); got != 0 {
		t.Fatalf("negative base Apply = %s, want 0", got)
	}
}

func TestSymmetricUsesPrivateRand(t *testing.T) {
	t.Parallel()

	got := Symmetric(0.25).Apply(time.Second, 1)
	if got < 750*time.Millisecond || got > 1250*time.Millisecond {
		t.Fatalf("Apply = %s, want within symmetric bounds", got)
	}
}
