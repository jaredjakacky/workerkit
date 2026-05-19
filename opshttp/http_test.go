package opshttp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jaredjakacky/servekit"
)

func TestRequiredQueryValue(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/admin/worker?name=worker&name=other", nil)

	value, err := requiredQueryValue(req, "name")
	if err != nil {
		t.Fatalf("requiredQueryValue returned error: %v", err)
	}
	if value != "worker" {
		t.Fatalf("value = %q, want worker", value)
	}
}

func TestRequiredQueryValueReturnsBadRequestForMissingValue(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/admin/worker?name=", nil)

	value, err := requiredQueryValue(req, "name")
	if err == nil {
		t.Fatal("requiredQueryValue returned nil error")
	}
	if value != "" {
		t.Fatalf("value = %q, want empty", value)
	}
	assertHTTPError(t, err, http.StatusBadRequest, `missing required query parameter "name"`)
}

func TestDecodeStrictJSON(t *testing.T) {
	t.Parallel()

	var dst struct {
		Name string `json:"name"`
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/test", strings.NewReader(`{"name":"worker"}`))

	if err := decodeStrictJSON(req, &dst, "test request"); err != nil {
		t.Fatalf("decodeStrictJSON returned error: %v", err)
	}
	if dst.Name != "worker" {
		t.Fatalf("Name = %q, want worker", dst.Name)
	}
}

func TestDecodeStrictJSONAllowsTrailingWhitespace(t *testing.T) {
	t.Parallel()

	var dst struct {
		Name string `json:"name"`
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/test", strings.NewReader("{\"name\":\"worker\"}\n\t "))

	if err := decodeStrictJSON(req, &dst, "test request"); err != nil {
		t.Fatalf("decodeStrictJSON returned error: %v", err)
	}
	if dst.Name != "worker" {
		t.Fatalf("Name = %q, want worker", dst.Name)
	}
}

func TestDecodeStrictJSONRejectsInvalidBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "malformed JSON",
			body: `{"name":`,
			want: "invalid test request JSON",
		},
		{
			name: "unknown field",
			body: `{"name":"worker","extra":true}`,
			want: "invalid test request JSON: json: unknown field \"extra\"",
		},
		{
			name: "multiple objects",
			body: `{"name":"worker"} {"name":"other"}`,
			want: "test request must contain exactly one JSON object",
		},
		{
			name: "trailing garbage",
			body: `{"name":"worker"} nope`,
			want: "invalid test request JSON",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var dst struct {
				Name string `json:"name"`
			}
			req := httptest.NewRequest(http.MethodPost, "/admin/test", strings.NewReader(tt.body))

			err := decodeStrictJSON(req, &dst, "test request")
			if err == nil {
				t.Fatal("decodeStrictJSON returned nil error")
			}
			assertHTTPError(t, err, http.StatusBadRequest, tt.want)
		})
	}
}

func TestDecodeStrictJSONRejectsNilBody(t *testing.T) {
	t.Parallel()

	var dst struct {
		Name string `json:"name"`
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/test", nil)
	req.Body = nil

	err := decodeStrictJSON(req, &dst, "test request")
	if err == nil {
		t.Fatal("decodeStrictJSON returned nil error")
	}
	assertHTTPError(t, err, http.StatusBadRequest, "request body must contain JSON")
}

func TestErrorHelpersReturnServekitHTTPErrors(t *testing.T) {
	t.Parallel()

	assertHTTPError(t, badRequestError("bad input"), http.StatusBadRequest, "bad input")
	assertHTTPError(t, notFoundError("worker", "runtime/worker"), http.StatusNotFound, `worker "runtime/worker" not found`)
}

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
