package sr

// Schema Registry error codes as defined in the official Confluent Schema Registry.
// These constants allow clients to programmatically check for specific error conditions
// when interacting with the Schema Registry API.
//
// The error codes are organized by HTTP status code categories:
//   - 404xx: Not Found errors
//   - 409: Conflict errors
//   - 422xx: Unprocessable Entity errors
//   - 500xx: Internal Server errors
//
// Reference: https://github.com/confluentinc/schema-registry/blob/master/core/src/main/java/io/confluent/kafka/schemaregistry/rest/exceptions/Errors.java
const (
	// HTTP 404 - Not Found errors
	
	// ErrCodeSubjectNotFound indicates that the specified subject does not exist.
	ErrCodeSubjectNotFound = 40401
	
	// ErrCodeVersionNotFound indicates that the specified version does not exist for the subject.
	ErrCodeVersionNotFound = 40402
	
	// ErrCodeSchemaNotFound indicates that the specified schema does not exist.
	ErrCodeSchemaNotFound = 40403
	
	// ErrCodeSubjectSoftDeleted indicates that the subject was soft deleted.
	// Set permanent=true to delete permanently.
	ErrCodeSubjectSoftDeleted = 40404
	
	// ErrCodeSubjectNotSoftDeleted indicates that the subject was not deleted first
	// before being permanently deleted.
	ErrCodeSubjectNotSoftDeleted = 40405
	
	// ErrCodeSchemaVersionSoftDeleted indicates that the specific version of the subject
	// was soft deleted. Set permanent=true to delete permanently.
	ErrCodeSchemaVersionSoftDeleted = 40406
	
	// ErrCodeSchemaVersionNotSoftDeleted indicates that the specific version of the subject
	// was not deleted first before being permanently deleted.
	ErrCodeSchemaVersionNotSoftDeleted = 40407
	
	// ErrCodeSubjectLevelCompatibilityNotConfigured indicates that the subject does not have
	// subject-level compatibility configured.
	ErrCodeSubjectLevelCompatibilityNotConfigured = 40408
	
	// ErrCodeSubjectLevelModeNotConfigured indicates that the subject does not have
	// subject-level mode configured.
	ErrCodeSubjectLevelModeNotConfigured = 40409

	// HTTP 409 - Conflict errors
	
	// ErrCodeIncompatibleSchema indicates that the schema being registered is incompatible
	// with an earlier schema for the same subject, according to the configured compatibility level.
	ErrCodeIncompatibleSchema = 409

	// HTTP 422 - Unprocessable Entity errors
	
	// ErrCodeInvalidSchema indicates that the provided schema is invalid.
	ErrCodeInvalidSchema = 42201
	
	// ErrCodeInvalidVersion indicates that the provided version is invalid.
	ErrCodeInvalidVersion = 42202
	
	// ErrCodeInvalidCompatibilityLevel indicates that the provided compatibility level is invalid.
	ErrCodeInvalidCompatibilityLevel = 42203
	
	// ErrCodeInvalidMode indicates that the provided mode is invalid.
	ErrCodeInvalidMode = 42204
	
	// ErrCodeOperationNotPermitted indicates that the requested operation is not permitted.
	ErrCodeOperationNotPermitted = 42205
	
	// ErrCodeReferenceExists indicates that the schema reference already exists.
	ErrCodeReferenceExists = 42206
	
	// ErrCodeIDDoesNotMatch indicates that the provided ID does not match the expected ID.
	ErrCodeIDDoesNotMatch = 42207
	
	// ErrCodeInvalidSubject indicates that the provided subject name is invalid.
	ErrCodeInvalidSubject = 42208
	
	// ErrCodeSchemaTooLarge indicates that the schema is too large.
	ErrCodeSchemaTooLarge = 42209
	
	// ErrCodeInvalidRuleset indicates that the provided ruleset is invalid.
	ErrCodeInvalidRuleset = 42210
	
	// ErrCodeContextNotEmpty indicates that the context is not empty when it should be.
	ErrCodeContextNotEmpty = 42211

	// HTTP 500 - Internal Server errors
	
	// ErrCodeStoreError indicates an error in the backend datastore.
	ErrCodeStoreError = 50001
	
	// ErrCodeOperationTimeout indicates that the operation timed out.
	ErrCodeOperationTimeout = 50002
	
	// ErrCodeRequestForwardingFailed indicates an error while forwarding the request to the primary.
	ErrCodeRequestForwardingFailed = 50003
	
	// ErrCodeUnknownLeader indicates that the leader is unknown.
	ErrCodeUnknownLeader = 50004
	
	// ErrCodeJSONParseError indicates a JSON parsing error (error code 50005 is used 
	// by the RestService to indicate a JSON Parse Error).
	ErrCodeJSONParseError = 50005
)

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