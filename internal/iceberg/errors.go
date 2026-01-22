package iceberg

import (
	"errors"
	"strings"
)

// PermanentError wraps errors that should not be retried by the caller.
// This is used to signal to the OTel exporter that retry_on_failure should
// not retry this error because it will never succeed (e.g., authentication
// failures, permission errors, validation errors).
type PermanentError struct {
	Err error
}

// Error implements the error interface.
func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "permanent error"
	}
	return e.Err.Error()
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *PermanentError) Unwrap() error {
	return e.Err
}

// NewPermanentError creates a new PermanentError wrapping the given error.
func NewPermanentError(err error) *PermanentError {
	return &PermanentError{Err: err}
}

// IsPermanentError checks if the error is or wraps a PermanentError.
func IsPermanentError(err error) bool {
	var permErr *PermanentError
	return errors.As(err, &permErr)
}

// IsPermanentCatalogError determines if a catalog error is permanent (non-retryable).
// Permanent errors include authentication, permission, validation, and duplicate resource errors.
// These errors will not succeed on retry, so the caller should handle them gracefully
func IsPermanentCatalogError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// Authentication and authorization errors - credentials won't magically become valid
	authErrors := []string{
		"unauthorized",
		"unauthenticated",
		"invalid token",
		"token expired",
		"invalid credentials",
		"authentication failed",
	}

	for _, pattern := range authErrors {
		if strings.Contains(errMsg, pattern) {
			return true // Permanent
		}
	}

	// Permission errors - access rights won't change between retries
	permissionErrors := []string{
		"forbidden",
		"access denied",
		"permission denied",
		"not authorized",
		"insufficient permissions",
	}

	for _, pattern := range permissionErrors {
		if strings.Contains(errMsg, pattern) {
			return true // Permanent
		}
	}

	// Client errors due to malformed requests - the request itself is invalid
	clientErrors := []string{
		"bad request",
		"malformed",
		"invalid argument",
		"invalid parameter",
		"validation failed",
		"schema mismatch",
		"incompatible schema",
		"type mismatch",
		"invalid schema",
	}

	for _, pattern := range clientErrors {
		if strings.Contains(errMsg, pattern) {
			return true // Permanent
		}
	}

	// Resource already exists - retrying won't help
	duplicateErrors := []string{
		"already exists",
		"duplicate",
		"conflict: resource already exists",
	}

	for _, pattern := range duplicateErrors {
		if strings.Contains(errMsg, pattern) {
			return true // Permanent
		}
	}

	// All other errors are considered transient and worth retrying
	return false
}
