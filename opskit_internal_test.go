package workerkit

import (
	"testing"

	opskit "github.com/jaredjakacky/opskit"
)

func TestOpskitStateFromRuntimeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status RuntimeStatus
		want   opskit.State
	}{
		{
			name:   "registered maps to stopped",
			status: RuntimeStatus{State: StateRegistered},
			want:   opskit.StateStopped,
		},
		{
			name:   "starting maps to initializing",
			status: RuntimeStatus{State: StateStarting},
			want:   opskit.StateInitializing,
		},
		{
			name:   "running ready maps to ready",
			status: RuntimeStatus{State: StateRunning, Ready: true},
			want:   opskit.StateReady,
		},
		{
			name:   "running not ready maps to not ready",
			status: RuntimeStatus{State: StateRunning},
			want:   opskit.StateNotReady,
		},
		{
			name:   "draining maps to not ready",
			status: RuntimeStatus{State: StateDraining},
			want:   opskit.StateNotReady,
		},
		{
			name:   "stopping maps to not ready",
			status: RuntimeStatus{State: StateStopping},
			want:   opskit.StateNotReady,
		},
		{
			name:   "stopped maps to stopped",
			status: RuntimeStatus{State: StateStopped},
			want:   opskit.StateStopped,
		},
		{
			name:   "failed maps to failed",
			status: RuntimeStatus{State: StateFailed},
			want:   opskit.StateFailed,
		},
		{
			name:   "unknown maps to unknown",
			status: RuntimeStatus{State: LifecycleState("other")},
			want:   opskit.StateUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := opskitStateFromRuntimeStatus(tt.status); got != tt.want {
				t.Fatalf("opskitStateFromRuntimeStatus() = %s, want %s", got, tt.want)
			}
		})
	}
}
