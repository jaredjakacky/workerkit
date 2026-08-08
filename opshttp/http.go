package opshttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	opskit "github.com/jaredjakacky/opskit"
	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func decodeStrictJSON(r *http.Request, dst any, label string) error {
	if r.Body == nil {
		return badRequestError("request body must contain JSON")
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return badRequestError(fmt.Sprintf("invalid %s JSON: %v", label, err))
	}

	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return badRequestError(fmt.Sprintf("%s must contain exactly one JSON object", label))
	} else if !errors.Is(err, io.EOF) {
		return badRequestError(fmt.Sprintf("invalid %s JSON: %v", label, err))
	}

	return nil
}

func badRequestError(message string) error {
	return servekit.Error(http.StatusBadRequest, message, nil)
}

func notFoundError(kind, name string) error {
	return servekit.Error(http.StatusNotFound, fmt.Sprintf("%s %q not found", kind, name), nil)
}

// mappedOperationalError preserves cause discovery while ensuring that both
// the public HTTP message and the mapped error's own Error method use only
// explicitly selected operational text.
func mappedOperationalError(status int, message string, cause error) error {
	safeCause := workerkit.WithOperationalFailure(cause, opskit.Failure{Message: message})
	return servekit.Error(status, message, safeCause)
}
