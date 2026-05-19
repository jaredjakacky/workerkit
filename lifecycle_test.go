package workerkit

import "testing"

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

func TestLifecycleAllowsWorkerRuntimeMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state LifecycleState
		want  bool
	}{
		{state: StateRegistered, want: false},
		{state: StateStarting, want: true},
		{state: StateRunning, want: true},
		{state: StateDraining, want: true},
		{state: StateStopping, want: false},
		{state: StateStopped, want: false},
		{state: StateFailed, want: false},
		{state: LifecycleState("unknown"), want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(string(tc.state), func(t *testing.T) {
			t.Parallel()

			if got := lifecycleAllowsWorkerRuntimeMutation(tc.state); got != tc.want {
				t.Fatalf("lifecycleAllowsWorkerRuntimeMutation(%q) = %v, want %v", tc.state, got, tc.want)
			}
		})
	}
}

func TestLifecycleAllowsWorkerRuntimePositiveSignal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state LifecycleState
		want  bool
	}{
		{state: StateRegistered, want: false},
		{state: StateStarting, want: true},
		{state: StateRunning, want: true},
		{state: StateDraining, want: false},
		{state: StateStopping, want: false},
		{state: StateStopped, want: false},
		{state: StateFailed, want: false},
		{state: LifecycleState("unknown"), want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(string(tc.state), func(t *testing.T) {
			t.Parallel()

			if got := lifecycleAllowsWorkerRuntimePositiveSignal(tc.state); got != tc.want {
				t.Fatalf("lifecycleAllowsWorkerRuntimePositiveSignal(%q) = %v, want %v", tc.state, got, tc.want)
			}
		})
	}
}
