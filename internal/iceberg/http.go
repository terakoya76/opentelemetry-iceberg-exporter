package iceberg

import (
	"bytes"
	"io"
	"net/http"

	"go.uber.org/zap"
)

// loggingTransport wraps an http.RoundTripper to log request details
type loggingTransport struct {
	wrapped http.RoundTripper
	logger  *zap.Logger
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Log request details
	hasAuth := req.Header.Get("Authorization") != ""
	authLen := len(req.Header.Get("Authorization"))

	t.logger.Debug("HTTP request",
		zap.String("method", req.Method),
		zap.String("url", req.URL.String()),
		zap.Bool("has_authorization", hasAuth),
		zap.Int("authorization_length", authLen))

	resp, err := t.wrapped.RoundTrip(req)
	if err != nil {
		t.logger.Debug("HTTP request failed",
			zap.String("url", req.URL.String()),
			zap.Error(err))
		return resp, err
	}

	// Log response, with body for error responses
	if resp.StatusCode >= 400 {
		// Read the response body for error logging
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr == nil {
			// Replace the body so it can be read again
			resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			// Truncate body for logging if too long
			bodyStr := string(bodyBytes)
			if len(bodyStr) > 1000 {
				bodyStr = bodyStr[:1000] + "...(truncated)"
			}

			t.logger.Warn("HTTP error response",
				zap.String("method", req.Method),
				zap.String("url", req.URL.String()),
				zap.Int("status_code", resp.StatusCode),
				zap.String("response_body", bodyStr))
		}
	} else {
		t.logger.Debug("HTTP response",
			zap.String("url", req.URL.String()),
			zap.Int("status_code", resp.StatusCode))
	}

	return resp, err
}
