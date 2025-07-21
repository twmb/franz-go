package mock

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	errCodeSubjectNotFound     = 40401
	errCodeVersionNotFound     = 40402
	errCodeSchemaNotFound      = 40403
	errCodeInvalidSchema       = 42201
	errCodeInvalidVersion      = 42202
	errCodeInvalidCompatLevel  = 42203
	errCodeBackendDataStore    = 50001
	errCodeOperationTimeout    = 50002
	errCodeForwardRequestError = 50003
)

func respondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func writeError(w http.ResponseWriter, status, code int, msg string) {
	respondJSON(w, status, map[string]any{
		"error_code": code,
		"message":    msg,
	})
}

// decodeJSONRequest safely decodes a single JSON object from an HTTP request body.
// It ensures there is no trailing data after the JSON object and rejects unknown fields.
// This addresses the json.NewDecoder issue described in https://github.com/golang/go/issues/36225
func decodeJSONRequest(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		return err
	}

	// Check for trailing data - ensures exactly one JSON object
	if err := dec.Decode(&json.RawMessage{}); err != io.EOF {
		return fmt.Errorf("request body must contain only a single JSON object")
	}

	return nil
}
