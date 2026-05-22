package workerkit_test

import (
	. "github.com/jaredjakacky/workerkit"
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

func testName(name string) string {
	if name == "" {
		return "empty"
	}
	replacer := strings.NewReplacer("/", "_", " ", "_", ".", "_")
	return replacer.Replace(strings.TrimSpace(name))
}
