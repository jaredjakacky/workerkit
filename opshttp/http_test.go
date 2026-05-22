package opshttp_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/jaredjakacky/servekit"
)

func assertHTTPError(t *testing.T, err error, wantStatus int, wantMessage string) {
	t.Helper()

	var httpErr servekit.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %T, want servekit.HTTPError", err)
	}
	if httpErr.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", httpErr.StatusCode, wantStatus)
	}
	if !strings.Contains(httpErr.Message, wantMessage) {
		t.Fatalf("message = %q, want to contain %q", httpErr.Message, wantMessage)
	}
}
