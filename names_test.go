package workerkit

import (
	"strings"
	"testing"
)

func TestValidateRuntimeName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"runtime",
		"runtime-1",
		"runtime_1",
		"a0-b_1",
	}
	for _, name := range valid {
		name := name
		t.Run("valid "+name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateRuntimeName(name); err != nil {
				t.Fatalf("ValidateRuntimeName(%q) returned error: %v", name, err)
			}
		})
	}

	invalid := []string{
		"",
		"Runtime",
		"runtime.name",
		"runtime name",
		" runtime",
		"runtime ",
		"runtime/worker",
		"runtimé",
	}
	for _, name := range invalid {
		name := name
		t.Run("invalid "+testName(name), func(t *testing.T) {
			t.Parallel()

			if err := ValidateRuntimeName(name); err == nil {
				t.Fatalf("ValidateRuntimeName(%q) returned nil, want error", name)
			}
		})
	}
}

func TestValidateCommandName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"command",
		"queue/drain",
		"snapshots/prune_old",
		"search/reindex-1",
	}
	for _, name := range valid {
		name := name
		t.Run("valid "+name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateCommandName(name); err != nil {
				t.Fatalf("ValidateCommandName(%q) returned error: %v", name, err)
			}
		})
	}

	invalid := []string{
		"",
		"Command",
		"/command",
		"command/",
		"queue//drain",
		"queue/Drain",
		"queue/drain now",
		"queue/drain.now",
		" queue/drain",
		"queue/drain ",
	}
	for _, name := range invalid {
		name := name
		t.Run("invalid "+testName(name), func(t *testing.T) {
			t.Parallel()

			if err := ValidateCommandName(name); err == nil {
				t.Fatalf("ValidateCommandName(%q) returned nil, want error", name)
			}
		})
	}
}

func TestValidateQualifiedWorkerName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"runtime/worker",
		"runtime-1/worker_1",
	}
	for _, name := range valid {
		name := name
		t.Run("valid "+name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateQualifiedWorkerName(name); err != nil {
				t.Fatalf("ValidateQualifiedWorkerName(%q) returned error: %v", name, err)
			}
		})
	}

	invalid := []string{
		"",
		"worker",
		"runtime/",
		"/worker",
		"runtime/worker/extra",
		"Runtime/worker",
		"runtime/Worker",
		"runtime/worker.name",
	}
	for _, name := range invalid {
		name := name
		t.Run("invalid "+testName(name), func(t *testing.T) {
			t.Parallel()

			if err := ValidateQualifiedWorkerName(name); err == nil {
				t.Fatalf("ValidateQualifiedWorkerName(%q) returned nil, want error", name)
			}
		})
	}
}

func TestResolveWorkerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		runtimeName string
		workerName  string
		want        string
	}{
		{
			name:        "local worker",
			runtimeName: "runtime",
			workerName:  "worker",
			want:        "runtime/worker",
		},
		{
			name:        "qualified worker",
			runtimeName: "runtime",
			workerName:  "other/worker",
			want:        "other/worker",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveWorkerName(tc.runtimeName, tc.workerName)
			if err != nil {
				t.Fatalf("resolveWorkerName returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveWorkerName() = %q, want %q", got, tc.want)
			}
		})
	}

	invalid := []string{
		"",
		"runtime/",
		"/worker",
		"runtime/worker/extra",
		"Worker",
	}
	for _, name := range invalid {
		name := name
		t.Run("invalid "+testName(name), func(t *testing.T) {
			t.Parallel()

			if _, err := resolveWorkerName("runtime", name); err == nil {
				t.Fatalf("resolveWorkerName(%q) returned nil, want error", name)
			}
		})
	}
}

func TestSplitQualifiedWorkerName(t *testing.T) {
	t.Parallel()

	runtimeName, workerName, ok := splitQualifiedWorkerName("runtime/worker")
	if !ok {
		t.Fatal("splitQualifiedWorkerName ok = false, want true")
	}
	if runtimeName != "runtime" || workerName != "worker" {
		t.Fatalf("splitQualifiedWorkerName = %q %q, want runtime worker", runtimeName, workerName)
	}

	invalid := []string{
		"",
		"worker",
		"runtime/",
		"/worker",
		"runtime/worker/extra",
	}
	for _, name := range invalid {
		name := name
		t.Run("invalid "+testName(name), func(t *testing.T) {
			t.Parallel()

			if runtimeName, workerName, ok := splitQualifiedWorkerName(name); ok {
				t.Fatalf("splitQualifiedWorkerName(%q) = %q %q true, want false", name, runtimeName, workerName)
			}
		})
	}
}

func TestQualifyWorkerNameDoesNotValidate(t *testing.T) {
	t.Parallel()

	got := qualifyWorkerName("Runtime", "Worker")
	if got != "Runtime/Worker" {
		t.Fatalf("qualifyWorkerName = %q, want Runtime/Worker", got)
	}
}

func testName(name string) string {
	if name == "" {
		return "empty"
	}
	replacer := strings.NewReplacer("/", "_", " ", "_", ".", "_")
	return replacer.Replace(strings.TrimSpace(name))
}
