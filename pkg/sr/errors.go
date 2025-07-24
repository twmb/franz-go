package sr

import "log/slog"

// Error represents a Schema Registry error with its code, name, and description.
type Error struct {
	// Code is the numeric error code returned by the Schema Registry.
	Code int
	// Name is the string representation of the error.
	Name string
	// Description is a human-readable description of what this error means.
	Description string
}

// Error implements the error interface, returning the human-readable description.
func (e *Error) Error() string {
	return e.Description
}

// LogValue implements slog.LogValuer to provide structured logging output.
func (e *Error) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("code", e.Code),
		slog.String("name", e.Name),
		slog.String("description", e.Description),
	)
}

// Schema Registry errors as defined in the official Confluent Schema Registry.
// These allow clients to programmatically check for specific error conditions
// when interacting with the Schema Registry API.
//
// The errors are organized by HTTP status code categories:
//   - 404xx: Not Found errors
//   - 409: Conflict errors
//   - 422xx: Unprocessable Entity errors
//   - 500xx: Internal Server errors
//
// Reference: https://github.com/confluentinc/schema-registry/blob/master/core/src/main/java/io/confluent/kafka/schemaregistry/rest/exceptions/Errors.java
var (
	// ErrUnknown represents an unknown Schema Registry error code.
	// This is returned when the Schema Registry returns an error code that is not
	// recognized by this client version.
	ErrUnknown = &Error{-1, "UNKNOWN_SCHEMA_REGISTRY_ERROR", "Unknown Schema Registry error"}

	// HTTP 404 - Not Found errors

	// ErrSubjectNotFound indicates that the specified subject does not exist.
	ErrSubjectNotFound = &Error{40401, "SUBJECT_NOT_FOUND", "Subject does not exist"}

	// ErrVersionNotFound indicates that the specified version does not exist for the subject.
	ErrVersionNotFound = &Error{40402, "VERSION_NOT_FOUND", "Version does not exist for subject"}

	// ErrSchemaNotFound indicates that the specified schema does not exist.
	ErrSchemaNotFound = &Error{40403, "SCHEMA_NOT_FOUND", "Schema does not exist"}

	// ErrSubjectSoftDeleted indicates that the subject was soft deleted.
	// Set permanent=true to delete permanently.
	ErrSubjectSoftDeleted = &Error{40404, "SUBJECT_SOFT_DELETED", "Subject was soft deleted"}

	// ErrSubjectNotSoftDeleted indicates that the subject was not deleted first
	// before being permanently deleted.
	ErrSubjectNotSoftDeleted = &Error{40405, "SUBJECT_NOT_SOFT_DELETED", "Subject was not deleted first before being permanently deleted"}

	// ErrSchemaVersionSoftDeleted indicates that the specific version of the subject
	// was soft deleted. Set permanent=true to delete permanently.
	ErrSchemaVersionSoftDeleted = &Error{40406, "SCHEMA_VERSION_SOFT_DELETED", "Schema version was soft deleted"}

	// ErrSchemaVersionNotSoftDeleted indicates that the specific version of the subject
	// was not deleted first before being permanently deleted.
	ErrSchemaVersionNotSoftDeleted = &Error{40407, "SCHEMA_VERSION_NOT_SOFT_DELETED", "Schema version was not deleted first before being permanently deleted"}

	// ErrSubjectLevelCompatibilityNotConfigured indicates that the subject does not have
	// subject-level compatibility configured.
	ErrSubjectLevelCompatibilityNotConfigured = &Error{40408, "SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED", "Subject does not have subject-level compatibility configured"}

	// ErrSubjectLevelModeNotConfigured indicates that the subject does not have
	// subject-level mode configured.
	ErrSubjectLevelModeNotConfigured = &Error{40409, "SUBJECT_LEVEL_MODE_NOT_CONFIGURED", "Subject does not have subject-level mode configured"}

	// HTTP 409 - Conflict errors

	// ErrIncompatibleSchema indicates that the schema being registered is incompatible
	// with an earlier schema for the same subject, according to the configured compatibility level.
	ErrIncompatibleSchema = &Error{409, "INCOMPATIBLE_SCHEMA", "Schema is incompatible with an earlier schema"}

	// HTTP 422 - Unprocessable Entity errors

	// ErrInvalidSchema indicates that the provided schema is invalid.
	ErrInvalidSchema = &Error{42201, "INVALID_SCHEMA", "Schema is invalid"}

	// ErrInvalidVersion indicates that the provided version is invalid.
	ErrInvalidVersion = &Error{42202, "INVALID_VERSION", "Version is invalid"}

	// ErrInvalidCompatibilityLevel indicates that the provided compatibility level is invalid.
	ErrInvalidCompatibilityLevel = &Error{42203, "INVALID_COMPATIBILITY_LEVEL", "Compatibility level is invalid"}

	// ErrInvalidMode indicates that the provided mode is invalid.
	ErrInvalidMode = &Error{42204, "INVALID_MODE", "Mode is invalid"}

	// ErrOperationNotPermitted indicates that the requested operation is not permitted.
	ErrOperationNotPermitted = &Error{42205, "OPERATION_NOT_PERMITTED", "Operation is not permitted"}

	// ErrReferenceExists indicates that the schema reference already exists.
	ErrReferenceExists = &Error{42206, "REFERENCE_EXISTS", "Schema reference already exists"}

	// ErrIDDoesNotMatch indicates that the provided ID does not match the expected ID.
	ErrIDDoesNotMatch = &Error{42207, "ID_DOES_NOT_MATCH", "Provided ID does not match expected ID"}

	// ErrInvalidSubject indicates that the provided subject name is invalid.
	ErrInvalidSubject = &Error{42208, "INVALID_SUBJECT", "Subject name is invalid"}

	// ErrSchemaTooLarge indicates that the schema is too large.
	ErrSchemaTooLarge = &Error{42209, "SCHEMA_TOO_LARGE", "Schema is too large"}

	// ErrInvalidRuleset indicates that the provided ruleset is invalid.
	ErrInvalidRuleset = &Error{42210, "INVALID_RULESET", "Ruleset is invalid"}

	// ErrContextNotEmpty indicates that the context is not empty when it should be.
	ErrContextNotEmpty = &Error{42211, "CONTEXT_NOT_EMPTY", "Context is not empty when it should be"}

	// HTTP 500 - Internal Server errors

	// ErrStoreError indicates an error in the backend datastore.
	ErrStoreError = &Error{50001, "STORE_ERROR", "Error in backend datastore"}

	// ErrOperationTimeout indicates that the operation timed out.
	ErrOperationTimeout = &Error{50002, "OPERATION_TIMEOUT", "Operation timed out"}

	// ErrRequestForwardingFailed indicates an error while forwarding the request to the primary.
	ErrRequestForwardingFailed = &Error{50003, "REQUEST_FORWARDING_FAILED", "Error forwarding request to primary"}

	// ErrUnknownLeader indicates that the leader is unknown.
	ErrUnknownLeader = &Error{50004, "UNKNOWN_LEADER", "Leader is unknown"}

	// ErrJSONParseError indicates a JSON parsing error (error code 50005 is used
	// by the RestService to indicate a JSON Parse Error).
	ErrJSONParseError = &Error{50005, "JSON_PARSE_ERROR", "JSON parsing error"}
)

// ErrorForCode returns the Error corresponding to the given error code.
// If the code is unknown, this returns ErrUnknown.
func ErrorForCode(code int) *Error {
	err, exists := codeToError[code]
	if !exists {
		return ErrUnknown
	}
	return err
}

// codeToError maps error codes to their corresponding Error instances.
var codeToError = map[int]*Error{
	ErrSubjectNotFound.Code:                        ErrSubjectNotFound,
	ErrVersionNotFound.Code:                        ErrVersionNotFound,
	ErrSchemaNotFound.Code:                         ErrSchemaNotFound,
	ErrSubjectSoftDeleted.Code:                     ErrSubjectSoftDeleted,
	ErrSubjectNotSoftDeleted.Code:                  ErrSubjectNotSoftDeleted,
	ErrSchemaVersionSoftDeleted.Code:               ErrSchemaVersionSoftDeleted,
	ErrSchemaVersionNotSoftDeleted.Code:            ErrSchemaVersionNotSoftDeleted,
	ErrSubjectLevelCompatibilityNotConfigured.Code: ErrSubjectLevelCompatibilityNotConfigured,
	ErrSubjectLevelModeNotConfigured.Code:          ErrSubjectLevelModeNotConfigured,
	ErrIncompatibleSchema.Code:                     ErrIncompatibleSchema,
	ErrInvalidSchema.Code:                          ErrInvalidSchema,
	ErrInvalidVersion.Code:                         ErrInvalidVersion,
	ErrInvalidCompatibilityLevel.Code:              ErrInvalidCompatibilityLevel,
	ErrInvalidMode.Code:                            ErrInvalidMode,
	ErrOperationNotPermitted.Code:                  ErrOperationNotPermitted,
	ErrReferenceExists.Code:                        ErrReferenceExists,
	ErrIDDoesNotMatch.Code:                         ErrIDDoesNotMatch,
	ErrInvalidSubject.Code:                         ErrInvalidSubject,
	ErrSchemaTooLarge.Code:                         ErrSchemaTooLarge,
	ErrInvalidRuleset.Code:                         ErrInvalidRuleset,
	ErrContextNotEmpty.Code:                        ErrContextNotEmpty,
	ErrStoreError.Code:                             ErrStoreError,
	ErrOperationTimeout.Code:                       ErrOperationTimeout,
	ErrRequestForwardingFailed.Code:                ErrRequestForwardingFailed,
	ErrUnknownLeader.Code:                          ErrUnknownLeader,
	ErrJSONParseError.Code:                         ErrJSONParseError,
}

// IsNotFoundError returns true if the error code indicates a "not found" condition.
func IsNotFoundError(code int) bool {
	return code >= 40401 && code <= 40409
}


// IsInvalidRequestError returns true if the error code indicates an invalid request.
func IsInvalidRequestError(code int) bool {
	return code >= 42201 && code <= 42211
}


// IsServerError returns true if the error code indicates a server-side error.
func IsServerError(code int) bool {
	return code >= 50001 && code <= 50005
}


