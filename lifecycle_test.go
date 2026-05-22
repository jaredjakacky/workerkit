package workerkit_test

import (
	. "github.com/jaredjakacky/workerkit"
	"testing"
)

func TestLifecycleStateStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state LifecycleState
		want  string
	}{
		{state: StateRegistered, want: "registered"},
		{state: StateStarting, want: "starting"},
		{state: StateRunning, want: "running"},
		{state: StateDraining, want: "draining"},
		{state: StateStopping, want: "stopping"},
		{state: StateStopped, want: "stopped"},
		{state: StateFailed, want: "failed"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			if got := string(tc.state); got != tc.want {
				t.Fatalf("string(%q) = %q, want %q", tc.state, got, tc.want)
			}
		})
	}
}
