package mock

import (
	"encoding/json"
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
