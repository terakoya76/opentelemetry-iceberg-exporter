package logger

import (
	"testing"

	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestVerboseLogger_Info(t *testing.T) {
	tests := []struct {
		name      string
		verbosity configtelemetry.Level
		wantLog   bool
	}{
		{
			name:      "Basic level does not log Info messages",
			verbosity: configtelemetry.LevelBasic,
			wantLog:   false,
		},
		{
			name:      "Normal level logs Info messages",
			verbosity: configtelemetry.LevelNormal,
			wantLog:   true,
		},
		{
			name:      "Detailed level logs Info messages",
			verbosity: configtelemetry.LevelDetailed,
			wantLog:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			zapLogger := zap.New(core)

			vlogger := New(zapLogger, tt.verbosity)
			vlogger.Info("test message")

			if tt.wantLog && logs.Len() == 0 {
				t.Errorf("expected log output but got none")
			}
			if !tt.wantLog && logs.Len() > 0 {
				t.Errorf("expected no log output but got %d logs", logs.Len())
			}
		})
	}
}

func TestVerboseLogger_Debug(t *testing.T) {
	tests := []struct {
		name      string
		verbosity configtelemetry.Level
		wantLog   bool
	}{
		{
			name:      "Basic level does not log Debug messages",
			verbosity: configtelemetry.LevelBasic,
			wantLog:   false,
		},
		{
			name:      "Normal level does not log Debug messages",
			verbosity: configtelemetry.LevelNormal,
			wantLog:   false,
		},
		{
			name:      "Detailed level logs Debug messages",
			verbosity: configtelemetry.LevelDetailed,
			wantLog:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use DebugLevel to capture Debug logs
			core, logs := observer.New(zap.DebugLevel)
			zapLogger := zap.New(core)

			vlogger := New(zapLogger, tt.verbosity)
			vlogger.Debug("test message")

			if tt.wantLog && logs.Len() == 0 {
				t.Errorf("expected log output but got none")
			}
			if !tt.wantLog && logs.Len() > 0 {
				t.Errorf("expected no log output but got %d logs", logs.Len())
			}
		})
	}
}

func TestVerboseLogger_Error(t *testing.T) {
	// Error should always be logged regardless of verbosity
	levels := []configtelemetry.Level{
		configtelemetry.LevelBasic,
		configtelemetry.LevelNormal,
		configtelemetry.LevelDetailed,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			zapLogger := zap.New(core)

			vlogger := New(zapLogger, level)
			vlogger.Error("error message")

			if logs.Len() == 0 {
				t.Errorf("expected Error to always be logged at %s level", level)
			}
		})
	}
}

func TestVerboseLogger_Warn(t *testing.T) {
	// Warn should always be logged regardless of verbosity
	levels := []configtelemetry.Level{
		configtelemetry.LevelBasic,
		configtelemetry.LevelNormal,
		configtelemetry.LevelDetailed,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			zapLogger := zap.New(core)

			vlogger := New(zapLogger, level)
			vlogger.Warn("warn message")

			if logs.Len() == 0 {
				t.Errorf("expected Warn to always be logged at %s level", level)
			}
		})
	}
}

func TestVerboseLogger_IsNormal(t *testing.T) {
	tests := []struct {
		verbosity configtelemetry.Level
		want      bool
	}{
		{configtelemetry.LevelBasic, false},
		{configtelemetry.LevelNormal, true},
		{configtelemetry.LevelDetailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.verbosity.String(), func(t *testing.T) {
			vlogger := New(zap.NewNop(), tt.verbosity)
			if got := vlogger.IsNormal(); got != tt.want {
				t.Errorf("IsNormal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerboseLogger_IsDetailed(t *testing.T) {
	tests := []struct {
		verbosity configtelemetry.Level
		want      bool
	}{
		{configtelemetry.LevelBasic, false},
		{configtelemetry.LevelNormal, false},
		{configtelemetry.LevelDetailed, true},
	}

	for _, tt := range tests {
		t.Run(tt.verbosity.String(), func(t *testing.T) {
			vlogger := New(zap.NewNop(), tt.verbosity)
			if got := vlogger.IsDetailed(); got != tt.want {
				t.Errorf("IsDetailed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerboseLogger_GetVerbosity(t *testing.T) {
	levels := []configtelemetry.Level{
		configtelemetry.LevelBasic,
		configtelemetry.LevelNormal,
		configtelemetry.LevelDetailed,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			vlogger := New(zap.NewNop(), level)
			if got := vlogger.GetVerbosity(); got != level {
				t.Errorf("GetVerbosity() = %v, want %v", got, level)
			}
		})
	}
}

func TestVerboseLogger_Underlying(t *testing.T) {
	zapLogger := zap.NewNop()
	vlogger := New(zapLogger, configtelemetry.LevelNormal)

	if got := vlogger.Underlying(); got != zapLogger {
		t.Error("Underlying() should return the wrapped zap.Logger")
	}
}
