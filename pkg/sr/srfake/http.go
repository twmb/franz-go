package srfake

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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

// decodeJSONRequest decodes the JSON value at the start of a request body into
// v. As in Confluent's registry, unknown fields and anything after the value
// are ignored, and a body that is not valid JSON returns 400 with error code
// 400. Any other error, such as a bad level or mode name, returns the error
// that invalid builds from its message.
func decodeJSONRequest(r io.Reader, v any, invalid func(msg string) *registryError) error {
	err := json.NewDecoder(r).Decode(v)
	if _, ok := errors.AsType[*json.SyntaxError](err); ok || errors.Is(err, io.ErrUnexpectedEOF) {
		return newErr(http.StatusBadRequest, http.StatusBadRequest, "%s", err.Error())
	}
	if err != nil {
		return invalid(err.Error())
	}
	return nil
}
