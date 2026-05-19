package opshttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/jaredjakacky/servekit"
)

func requiredQueryValue(r *http.Request, key string) (string, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return "", badRequestError(fmt.Sprintf("missing required query parameter %q", key))
	}
	return value, nil
}

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
