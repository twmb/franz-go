package srfake

import (
	"fmt"
	"net/http"

	"github.com/twmb/franz-go/pkg/sr"
)

// registryError is a single, unified error type for the mock registry.
type registryError struct {
	// The user-facing message for the HTTP response.
	Message string
	// The HTTP status code to return.
	HTTPStatus int
	// The Schema Registry error code to return.
	SRCode int
}

// Error implements the standard error interface.
func (e *registryError) Error() string {
	return e.Message
}

// newErr creates a new registryError with the given parameters.
func newErr(httpStatus, srCode int, format string, a ...any) *registryError {
	return &registryError{
		HTTPStatus: httpStatus,
		SRCode:     srCode,
		Message:    fmt.Sprintf(format, a...),
	}
}

// Specific error constructors for common error scenarios

func errSubjectNotFound(subject string) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSubjectNotFound.Code, "subject %q not found", subject)
}

func errVersionNotFound(subject string, version int) *registryError {
	return newErr(http.StatusNotFound, sr.ErrVersionNotFound.Code, "version %d not found for %q", version, subject)
}

func errSchemaNotFound() *registryError {
	return newErr(http.StatusNotFound, sr.ErrSchemaNotFound.Code, "schema not found")
}

func errSchemaIsReferenced(subject string, version int) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrReferenceExists.Code, "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=%s,version=%d}.", subject, version)
}

func errSubjectSoftDeleted(subject string) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSubjectSoftDeleted.Code, "Subject '%s' was soft deleted.Set permanent=true to delete permanently", subject)
}

func errSubjectNotSoftDeleted(subject string) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSubjectNotSoftDeleted.Code, "Subject '%s' was not deleted first before being permanently deleted", subject)
}

func errVersionSoftDeleted(subject string, version int) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSchemaVersionSoftDeleted.Code, "Subject '%s' Version %d was soft deleted.Set permanent=true to delete permanently", subject, version)
}

func errVersionNotSoftDeleted(subject string, version int) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSchemaVersionNotSoftDeleted.Code, "Subject '%s' Version %d was not deleted first before being permanently deleted", subject, version)
}

func errInvalidReference(ref sr.SchemaReference) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidSchema.Code, "reference %q subject %q version %d not found", ref.Name, ref.Subject, ref.Version)
}

func errInvalidSchema(msg string) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidSchema.Code, "%s", msg)
}

func errInvalidSchemaWithCause(_ error, msg string) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidSchema.Code, "%s", msg)
}

func errInvalidVersion(msg string) *registryError {
	return newErr(http.StatusBadRequest, sr.ErrInvalidVersion.Code, "%s", msg)
}

func errInvalidCompatLevel() *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidCompatibilityLevel.Code, "Invalid compatibility level. Valid values are none, backward, forward, full, backward_transitive, forward_transitive, and full_transitive")
}

func errCompatNotConfigured(subject string) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSubjectLevelCompatibilityNotConfigured.Code, "Subject '%s' does not have subject-level compatibility configured", subject)
}

func errModeNotConfigured(subject string) *registryError {
	return newErr(http.StatusNotFound, sr.ErrSubjectLevelModeNotConfigured.Code, "Subject '%s' does not have subject-level mode configured", subject)
}

// errModeRefuses is the error for an operation that the mode of subject
// refuses. As in Confluent's registry, the global subject reads as null.
func errModeRefuses(subject, why string) *registryError {
	name := subject
	if name == "" {
		name = "null"
	}
	return newErr(http.StatusUnprocessableEntity, sr.ErrOperationNotPermitted.Code, "Subject %s in context %s %s", name, qualifiedContext(subjectContext(subject)), why)
}

func errInvalidMode(msg string) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidMode.Code, "%s", msg)
}

func errCircularDependency(subject string) *registryError {
	return newErr(http.StatusUnprocessableEntity, sr.ErrInvalidSchema.Code, "circular dependency detected: subject %s is referenced in a cycle", subject)
}

func errContextNotEmpty(context string) *registryError {
	return newErr(http.StatusConflict, sr.ErrOperationNotPermitted.Code,
		"context %q is not empty", context)
}
