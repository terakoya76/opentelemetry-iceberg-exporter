package iceberg

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPermanentError(t *testing.T) {
	t.Run("Error returns underlying error message", func(t *testing.T) {
		underlying := errors.New("authentication failed")
		permErr := NewPermanentError(underlying)

		assert.Equal(t, "authentication failed", permErr.Error())
	})

	t.Run("Error with nil underlying returns default message", func(t *testing.T) {
		permErr := NewPermanentError(nil)

		assert.Equal(t, "permanent error", permErr.Error())
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		underlying := errors.New("underlying error")
		permErr := NewPermanentError(underlying)

		assert.Equal(t, underlying, permErr.Unwrap())
	})

	t.Run("errors.Is works with underlying error", func(t *testing.T) {
		var errAuth = errors.New("authentication failed")
		permErr := NewPermanentError(errAuth)

		assert.True(t, errors.Is(permErr, errAuth))
	})

	t.Run("errors.As works with PermanentError", func(t *testing.T) {
		underlying := errors.New("permission denied")
		permErr := NewPermanentError(underlying)

		var target *PermanentError
		assert.True(t, errors.As(permErr, &target))
		assert.Equal(t, "permission denied", target.Error())
	})
}

func TestIsPermanentError(t *testing.T) {
	t.Run("returns true for PermanentError", func(t *testing.T) {
		err := NewPermanentError(errors.New("test"))

		assert.True(t, IsPermanentError(err))
	})

	t.Run("returns true for wrapped PermanentError", func(t *testing.T) {
		permErr := NewPermanentError(errors.New("auth failed"))
		wrapped := fmt.Errorf("operation failed: %w", permErr)

		assert.True(t, IsPermanentError(wrapped))
	})

	t.Run("returns false for regular error", func(t *testing.T) {
		err := errors.New("transient error")

		assert.False(t, IsPermanentError(err))
	})

	t.Run("returns false for nil error", func(t *testing.T) {
		assert.False(t, IsPermanentError(nil))
	})
}

func TestIsPermanentCatalogError(t *testing.T) {
	t.Run("returns false for nil error", func(t *testing.T) {
		assert.False(t, IsPermanentCatalogError(nil))
	})

	// Authentication errors - should be permanent
	t.Run("authentication errors are permanent", func(t *testing.T) {
		authErrors := []string{
			"HTTP 401: Unauthorized",
			"request failed: unauthenticated",
			"invalid token provided",
			"token expired",
			"invalid credentials",
			"authentication failed for user",
		}

		for _, errMsg := range authErrors {
			err := errors.New(errMsg)
			assert.True(t, IsPermanentCatalogError(err), "expected %q to be permanent", errMsg)
		}
	})

	// Permission errors - should be permanent
	t.Run("permission errors are permanent", func(t *testing.T) {
		permErrors := []string{
			"HTTP 403: Forbidden",
			"access denied to resource",
			"permission denied: cannot write to table",
			"not authorized to perform this action",
			"insufficient permissions for operation",
		}

		for _, errMsg := range permErrors {
			err := errors.New(errMsg)
			assert.True(t, IsPermanentCatalogError(err), "expected %q to be permanent", errMsg)
		}
	})

	// Client errors - should be permanent
	t.Run("client errors are permanent", func(t *testing.T) {
		clientErrors := []string{
			"HTTP 400: Bad Request",
			"malformed request body",
			"invalid argument: table name",
			"invalid parameter value",
			"validation failed: schema mismatch",
			"schema mismatch detected",
			"incompatible schema version",
			"type mismatch in column",
		}

		for _, errMsg := range clientErrors {
			err := errors.New(errMsg)
			assert.True(t, IsPermanentCatalogError(err), "expected %q to be permanent", errMsg)
		}
	})

	// Duplicate errors - should be permanent
	t.Run("duplicate errors are permanent", func(t *testing.T) {
		dupErrors := []string{
			"resource already exists",
			"duplicate key violation",
			"conflict: resource already exists",
		}

		for _, errMsg := range dupErrors {
			err := errors.New(errMsg)
			assert.True(t, IsPermanentCatalogError(err), "expected %q to be permanent", errMsg)
		}
	})

	// Transient errors - should NOT be permanent
	t.Run("transient errors are not permanent", func(t *testing.T) {
		transientErrors := []string{
			"connection reset by peer",
			"network timeout",
			"HTTP 500: Internal Server Error",
			"HTTP 502: Bad Gateway",
			"HTTP 503: Service Unavailable",
			"context deadline exceeded",
			"i/o timeout",
			"optimistic concurrency conflict",
			"transaction conflict, please retry",
		}

		for _, errMsg := range transientErrors {
			err := errors.New(errMsg)
			assert.False(t, IsPermanentCatalogError(err), "expected %q to be transient", errMsg)
		}
	})

	t.Run("wrapped errors are correctly classified", func(t *testing.T) {
		// Permanent error wrapped
		permErr := fmt.Errorf("catalog operation failed: %w", errors.New("HTTP 401: Unauthorized"))
		assert.True(t, IsPermanentCatalogError(permErr))

		// Transient error wrapped
		transErr := fmt.Errorf("catalog operation failed: %w", errors.New("connection reset by peer"))
		assert.False(t, IsPermanentCatalogError(transErr))
	})
}
