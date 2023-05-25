package sr

import "fmt"

// Error is the type returned from the schema registry for errors.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%v: %s", e.Code, e.Message)
}

// ErrorForCode returns the error corresponding to the given error code.
//
// If the code is unknown, this returns ErrUnknownServerError.
func ErrorForCode(code int) *Error {
	err, exists := code2err[code]
	if !exists {
		return ErrUnknownServerError
	}
	return err
}

var (
	ErrUnknownServerError = &Error{Code: -1, Message: "The server experienced an unexpected error when processing the request."}

	ErrSubjectNotFound  = &Error{Code: 40401, Message: "TBD"}
	ErrVersionNotFound  = &Error{Code: 40402, Message: "TBD"}
	ErrSchemaNotFound   = &Error{Code: 40403, Message: "TBD"}
	ErrExporterNotFound = &Error{Code: 40450, Message: "TBD"}

	ErrMissingOrInvalidExporterName   = &Error{Code: 40950, Message: "TBD"}
	ErrMissingOrInvalidExporterConfig = &Error{Code: 40951, Message: "TBD"}
	ErrInvalidExporterSubjects        = &Error{Code: 40952, Message: "TBD"}
	ErrExporterAlreadyExists          = &Error{Code: 40960, Message: "TBD"}
	ErrExporterAlreadyRunning         = &Error{Code: 40961, Message: "TBD"}
	ErrExporterAlreadyStarting        = &Error{Code: 40962, Message: "TBD"}
	ErrExporterNotPaused              = &Error{Code: 40963, Message: "TBD"}
	ErrTooManyExporters               = &Error{Code: 40964, Message: "TBD"}

	ErrInvalidSchema             = &Error{Code: 42201, Message: "TBD"}
	ErrInvalidVersion            = &Error{Code: 42202, Message: "TBD"}
	ErrInvalidCompatibilityLevel = &Error{Code: 42203, Message: "TBD"}
	ErrInvalidMode               = &Error{Code: 42204, Message: "TBD"}

	ErrErrorInBackendDataStore = &Error{Code: 50001, Message: "TBD"}
	ErrOperationTimedOut       = &Error{Code: 50002, Message: "TBD"}
	ErrErrorForwardingRequest  = &Error{Code: 50003, Message: "TBD"}
)

var code2err = map[int]*Error{
	40401: ErrSubjectNotFound,
	40402: ErrVersionNotFound,
	40403: ErrSchemaNotFound,
	40450: ErrExporterNotFound,

	40950: ErrMissingOrInvalidExporterName,
	40951: ErrMissingOrInvalidExporterConfig,
	40952: ErrInvalidExporterSubjects,
	40960: ErrExporterAlreadyExists,
	40961: ErrExporterAlreadyRunning,
	40962: ErrExporterAlreadyStarting,
	40963: ErrExporterNotPaused,
	40964: ErrTooManyExporters,

	42201: ErrInvalidSchema,
	42202: ErrInvalidVersion,
	42203: ErrInvalidCompatibilityLevel,
	42204: ErrInvalidMode,

	50001: ErrErrorInBackendDataStore,
	50002: ErrOperationTimedOut,
	50003: ErrErrorForwardingRequest,
}
