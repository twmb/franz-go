package mock

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/twmb/franz-go/pkg/sr"
)

// Error constants for different error types
var (
	ErrSubjectNotFound   = errors.New("subject not found")
	ErrVersionNotFound   = errors.New("version not found")
	ErrSchemaNotFound    = errors.New("schema not found")
	ErrInvalidSchema     = errors.New("invalid schema")
	ErrReferenceNotFound = errors.New("reference not found")
)

// RegistryError is a single, unified error type for the mock registry.
type RegistryError struct {
	// The underlying cause of the error. Can be compared with errors.Is.
	Cause error
	// The user-facing message for the HTTP response.
	Message string
	// The HTTP status code to return.
	HTTPStatus int
	// The Schema Registry error code to return.
	SRCode int
}

// Error implements the standard error interface.
func (e *RegistryError) Error() string {
	return e.Message
}

// Unwrap allows errors.Is to work on the underlying cause.
func (e *RegistryError) Unwrap() error {
	return e.Cause
}

// newErr creates a new RegistryError with the given parameters.
func newErr(httpStatus, srCode int, cause error, format string, a ...any) *RegistryError {
	return &RegistryError{
		HTTPStatus: httpStatus,
		SRCode:     srCode,
		Cause:      cause,
		Message:    fmt.Sprintf(format, a...),
	}
}

// Specific error constructors for common error scenarios

func errSubjectNotFound(subject string) *RegistryError {
	return newErr(http.StatusNotFound, errCodeSubjectNotFound, ErrSubjectNotFound, "subject %q not found", subject)
}

func errVersionNotFound(subject string, version int) *RegistryError {
	return newErr(http.StatusNotFound, errCodeVersionNotFound, ErrVersionNotFound, "version %d not found for %q", version, subject)
}

func errSchemaNotFound() *RegistryError {
	return newErr(http.StatusNotFound, errCodeSchemaNotFound, ErrSchemaNotFound, "schema not found")
}

func errSchemaIsReferenced(subject string, version int, by []int) *RegistryError {
	return newErr(http.StatusConflict, errCodeInvalidSchema, ErrInvalidSchema, "Cannot delete schema %s:%d as it is still referenced by schema IDs: %v", subject, version, by)
}

func errInvalidReference(ref sr.SchemaReference) *RegistryError {
	return newErr(http.StatusUnprocessableEntity, errCodeInvalidSchema, ErrReferenceNotFound, "reference %q subject %q version %d not found", ref.Name, ref.Subject, ref.Version)
}

func errInvalidSchema(msg string) *RegistryError {
	return newErr(http.StatusUnprocessableEntity, errCodeInvalidSchema, ErrInvalidSchema, msg)
}

func errInvalidSchemaWithCause(cause error, msg string) *RegistryError {
	return newErr(http.StatusUnprocessableEntity, errCodeInvalidSchema, cause, msg)
}

func errInvalidVersion(msg string) *RegistryError {
	return newErr(http.StatusBadRequest, errCodeInvalidVersion, nil, msg)
}

func errInvalidCompatLevel(msg string) *RegistryError {
	return newErr(http.StatusBadRequest, errCodeInvalidCompatLevel, nil, msg)
}

func errCircularDependency(subject string) *RegistryError {
	return newErr(http.StatusUnprocessableEntity, errCodeInvalidSchema, ErrInvalidSchema, "circular dependency detected: subject %s is referenced in a cycle", subject)
}
