package workerkit

import (
	"fmt"
	"strings"
	"unicode"
)

// ValidateRuntimeName validates a runtime operational identifier.
//
// Runtime names are flat identifiers. They must use lowercase ASCII letters,
// digits, '-' and '_' only. Workerkit intentionally restricts names to this
// small character set so they remain portable across logs, metrics, traces,
// config, operations APIs, and command routing.
func ValidateRuntimeName(name string) error {
	return validateIdentifier(name, "runtime name")
}

// ValidateCommandName validates the syntax of a worker-owned command name.
//
// Command names are path-like operational identifiers. Each path segment uses
// the same identifier rules as runtime and worker names. Examples include:
//
//	search/reindex
//	queue/drain
//	snapshots/prune
func ValidateCommandName(name string) error {
	return validatePathName(name, "command name")
}

// ValidateQualifiedWorkerName validates a fully scoped worker name in the form
// runtime/worker.
func ValidateQualifiedWorkerName(name string) error {
	runtimeName, workerName, ok := splitQualifiedWorkerName(name)
	if !ok {
		return fmt.Errorf("qualified worker name must have form runtime/worker")
	}
	if err := ValidateRuntimeName(runtimeName); err != nil {
		return fmt.Errorf("invalid qualified worker runtime: %w", err)
	}
	if err := ValidateWorkerLocalName(workerName); err != nil {
		return fmt.Errorf("invalid qualified worker name: %w", err)
	}
	return nil
}

// validateWorkerReferenceName accepts the two forms callers may use to target a
// worker after registration: a local worker name or a qualified runtime/worker
// name. Registration itself is stricter and only accepts local worker names.
func validateWorkerReferenceName(name string) error {
	if strings.Contains(name, "/") {
		return ValidateQualifiedWorkerName(name)
	}
	return ValidateWorkerLocalName(name)
}

// resolveWorkerName canonicalizes a worker target for runtime map lookups.
// Local names are qualified with the current runtime name. Already-qualified
// names are preserved after validation so external control surfaces can use
// stable runtime/worker references.
func resolveWorkerName(runtimeName, name string) (string, error) {
	if strings.Contains(name, "/") {
		if err := ValidateQualifiedWorkerName(name); err != nil {
			return "", err
		}
		return name, nil
	}
	if err := ValidateWorkerLocalName(name); err != nil {
		return "", err
	}
	return qualifyWorkerName(runtimeName, name), nil
}

// qualifyWorkerName joins already-validated runtime and worker local names. It
// intentionally does no validation so call sites remain explicit about whether
// they accept local names, qualified names, or command paths.
func qualifyWorkerName(runtimeName, workerName string) string {
	return runtimeName + "/" + workerName
}

// splitQualifiedWorkerName recognizes exactly runtime/worker. It rejects
// deeper paths because worker names are flat. Slash has one meaning in worker
// identity.
func splitQualifiedWorkerName(name string) (runtimeName, workerName string, ok bool) {
	before, after, found := strings.Cut(name, "/")
	if !found || before == "" || after == "" || strings.Contains(after, "/") {
		return "", "", false
	}
	return before, after, true
}

// validatePathName validates slash-separated command names. Every segment is a
// normal Workerkit identifier. Slash is only a namespace separator between
// non-empty segments.
func validatePathName(name, field string) error {
	if name == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if strings.TrimSpace(name) != name {
		return fmt.Errorf("%s must not contain leading or trailing whitespace", field)
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return fmt.Errorf("%s must not start or end with '/'", field)
	}
	if strings.Contains(name, "//") {
		return fmt.Errorf("%s must not contain empty path segments", field)
	}
	for _, segment := range strings.Split(name, "/") {
		if err := validateIdentifier(segment, field+" segment"); err != nil {
			return err
		}
	}
	return nil
}

// validateIdentifier validates one flat operational name segment. These names
// are deliberately narrow because they flow through telemetry, logs, config,
// operations APIs, and command routing without escaping or normalization.
func validateIdentifier(name, field string) error {
	if name == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if strings.TrimSpace(name) != name {
		return fmt.Errorf("%s must not contain leading or trailing whitespace", field)
	}

	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_':
		case r >= 'A' && r <= 'Z':
			return fmt.Errorf("%s must use lowercase ASCII letters, digits, '-' or '_' only", field)
		case unicode.IsSpace(r):
			return fmt.Errorf("%s must not contain whitespace", field)
		case r == '/':
			return fmt.Errorf("%s must be flat and must not contain '/'", field)
		case r > 127:
			return fmt.Errorf("%s must use lowercase ASCII letters, digits, '-' or '_' only", field)
		default:
			return fmt.Errorf("%s must use lowercase ASCII letters, digits, '-' or '_' only", field)
		}
	}

	return nil
}
