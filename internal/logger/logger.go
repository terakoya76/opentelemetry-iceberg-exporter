package logger

import (
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap"
)

// VerboseLogger wraps a zap.Logger with verbosity-based logging.
// It abstracts the mapping between configtelemetry.Level and actual log output.
//
// Logging behavior by verbosity level:
//   - LevelBasic: Only Error and Warn are output (silent on success)
//   - LevelNormal: Error, Warn, and Info are output
//   - LevelDetailed: Error, Warn, Info, and Debug are output
type VerboseLogger struct {
	logger    *zap.Logger
	verbosity configtelemetry.Level
}

// New creates a new VerboseLogger with the given zap.Logger and verbosity level.
func New(logger *zap.Logger, verbosity configtelemetry.Level) *VerboseLogger {
	return &VerboseLogger{
		logger:    logger,
		verbosity: verbosity,
	}
}

// Info logs a message at Info level if verbosity >= LevelNormal.
func (l *VerboseLogger) Info(msg string, fields ...zap.Field) {
	if l.verbosity >= configtelemetry.LevelNormal {
		l.logger.Info(msg, fields...)
	}
}

// Debug logs a message at Debug level if verbosity >= LevelDetailed.
func (l *VerboseLogger) Debug(msg string, fields ...zap.Field) {
	if l.verbosity >= configtelemetry.LevelDetailed {
		l.logger.Debug(msg, fields...)
	}
}

// Error logs an error message. This is always output regardless of verbosity.
func (l *VerboseLogger) Error(msg string, fields ...zap.Field) {
	l.logger.Error(msg, fields...)
}

// Warn logs a warning message. This is always output regardless of verbosity.
func (l *VerboseLogger) Warn(msg string, fields ...zap.Field) {
	l.logger.Warn(msg, fields...)
}

// GetVerbosity returns the current verbosity level.
func (l *VerboseLogger) GetVerbosity() configtelemetry.Level {
	return l.verbosity
}

// Underlying returns the underlying zap.Logger.
// Use this when you need direct access to the logger (e.g., for third-party libraries).
func (l *VerboseLogger) Underlying() *zap.Logger {
	return l.logger
}

// IsNormal returns true if verbosity allows normal-level logging (Info).
func (l *VerboseLogger) IsNormal() bool {
	return l.verbosity >= configtelemetry.LevelNormal
}

// IsDetailed returns true if verbosity allows detailed-level logging (Debug).
func (l *VerboseLogger) IsDetailed() bool {
	return l.verbosity >= configtelemetry.LevelDetailed
}
