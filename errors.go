package tianasqlite

import (
	"fmt"
	"io"
)

// Error contains a bounded diagnosis. OutcomeUnknown means the server may have
// executed the operation; retrying a write may duplicate its effects.
// False does not imply no side effects: SQLite errors can retain partial writes.
// BATON_INVALID from a complete HTTP rejection means no pipeline operation ran,
// but the session is unusable and its previous transaction cannot be continued.
type Error struct {
	Code           string
	OutcomeUnknown bool
	RequestID      string
	cause          error
}

func (e *Error) Error() string {
	if e == nil {
		return "tiana sqlite: error"
	}
	if e.OutcomeUnknown {
		return "tiana sqlite: " + e.Code + " (outcome unknown; do not replay)"
	}
	return "tiana sqlite: " + e.Code
}
func (e *Error) Unwrap() error             { return e.cause }
func (e Error) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }
func failure(code string, unknown bool, cause error) *Error {
	return &Error{Code: code, OutcomeUnknown: unknown, cause: cause}
}
func sqlFailure(code string) *Error {
	switch code {
	case "SQLITE_ERROR", "SQLITE_UNKNOWN", "SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_CONSTRAINT",
		"SQLITE_READONLY", "SQLITE_MISMATCH", "SQLITE_RANGE", "SQLITE_TOOBIG", "SQLITE_FULL",
		"SQLITE_ABORT", "SQLITE_INTERRUPT", "SQLITE_AUTH", "SQLITE_PERM", "ARGS_INVALID",
		"ARGS_BOTH_POSITIONAL_AND_NAMED", "SQL_NO_STATEMENT", "SQL_MANY_STATEMENTS":
		return failure(code, false, nil)
	case "RESULT_TOO_LARGE", "RESPONSE_TOO_LARGE", "SQLITE_IOERR":
		return failure(code, true, nil)
	default:
		return failure("SQL_ERROR", true, nil)
	}
}
