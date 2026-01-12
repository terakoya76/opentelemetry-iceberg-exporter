package icebergexporter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap/zaptest"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

func TestDefaultPathConfig(t *testing.T) {
	cfg := DefaultPathConfig()
	assert.Equal(t, "hourly", cfg.Granularity)
	assert.Equal(t, "UTC", cfg.Timezone)
	assert.False(t, cfg.IncludeServiceName)
}

func TestNewPathGenerator(t *testing.T) {
	t.Run("Valid timezone", func(t *testing.T) {
		cfg := PathConfig{
			Granularity: "hourly",
			Timezone:    "UTC",
		}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)
		assert.NotNil(t, gen)
	})

	t.Run("Invalid timezone defaults to UTC", func(t *testing.T) {
		cfg := PathConfig{
			Granularity: "hourly",
			Timezone:    "Invalid/Zone",
		}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)
		assert.NotNil(t, gen)
		// Should still work with UTC
	})
}

func TestPathGenerator_GeneratePath(t *testing.T) {
	// Fixed timestamp for predictable tests
	ts := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)

	t.Run("Hourly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "hourly", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		// Path should contain table name, year, month, day, hour
		assert.Contains(t, path, "otel_traces/data")
		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.Contains(t, path, "day=15")
		assert.Contains(t, path, "hour=14")
		assert.True(t, strings.HasSuffix(path, ".parquet"))
	})

	t.Run("Daily granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "daily", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_logs",
			Timestamp: ts,
		})

		assert.Contains(t, path, "otel_logs/data")
		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.Contains(t, path, "day=15")
		assert.NotContains(t, path, "hour=")
	})

	t.Run("Monthly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "monthly", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_metrics",
			Timestamp: ts,
		})

		assert.Contains(t, path, "otel_metrics/data")
		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.NotContains(t, path, "day=")
		assert.NotContains(t, path, "hour=")
	})

	t.Run("With service name", func(t *testing.T) {
		cfg := PathConfig{
			Granularity:        "hourly",
			Timezone:           "UTC",
			IncludeServiceName: true,
		}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName:   "otel_traces",
			Timestamp:   ts,
			ServiceName: "my-service",
		})

		assert.Contains(t, path, "service_name=my-service")
	})

	t.Run("Different timezone", func(t *testing.T) {
		cfg := PathConfig{Granularity: "hourly", Timezone: "America/New_York"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		// UTC 14:30 is 10:30 EDT (during summer)
		path := gen.GeneratePath(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Contains(t, path, "hour=10")
	})
}

func TestPathGenerator_ExtractPartitionValues(t *testing.T) {
	ts := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)

	t.Run("Hourly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "hourly", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Equal(t, "15", values.Day)
		assert.Equal(t, "14", values.Hour)
	})

	t.Run("Daily granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "daily", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_logs",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Equal(t, "15", values.Day)
		assert.Empty(t, values.Hour)
	})

	t.Run("Monthly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: "monthly", Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_metrics",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Empty(t, values.Day)
		assert.Empty(t, values.Hour)
	})

}

func TestSanitizePartitionValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Simple name", "my-service", "my-service"},
		{"With underscores", "my_service", "my_service"},
		{"With spaces", "my service", "my_service"},
		{"With special chars", "my@service!name", "my_service_name"},
		{"Multiple special chars", "my@@service!!!name", "my_service_name"},
		{"Leading/trailing special", "@@service@@", "service"},
		{"Empty becomes unknown", "", "unknown"},
		{"Only special chars", "@@@", "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sanitizePartitionValue(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestNewIcebergWriter(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)

	t.Run("With filesystem storage and no catalog", func(t *testing.T) {
		tmpDir := t.TempDir()

		cfg := WriterConfig{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: tmpDir,
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "none",
			},
			Partition: PartitionConfig{
				Granularity: "hourly",
				Timezone:    "UTC",
			},
		}

		writer, err := NewIcebergWriter(ctx, cfg, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		assert.Equal(t, "filesystem", writer.GetStorageType())
		assert.Equal(t, "none", writer.GetCatalogType())
	})

	t.Run("Default granularity and timezone", func(t *testing.T) {
		tmpDir := t.TempDir()

		cfg := WriterConfig{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: tmpDir,
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "none",
			},
			// Empty partition config should use defaults
			Partition: PartitionConfig{},
		}

		writer, err := NewIcebergWriter(ctx, cfg, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		assert.NotNil(t, writer)
	})

	t.Run("Invalid storage type fails", func(t *testing.T) {
		cfg := WriterConfig{
			Storage: iceberg.FileIOConfig{
				Type: "invalid",
			},
		}

		_, err := NewIcebergWriter(ctx, cfg, vlogger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage")
	})
}

func TestIcebergWriter_Write(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	tmpDir := t.TempDir()

	cfg := WriterConfig{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: tmpDir,
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
		Partition: PartitionConfig{
			Granularity: "hourly",
			Timezone:    "UTC",
		},
	}

	writer, err := NewIcebergWriter(ctx, cfg, vlogger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	t.Run("Write traces data", func(t *testing.T) {
		opts := WriteOptions{
			SignalType:  "traces",
			Schema:      nil, // Not used when catalog is "none"
			Data:        []byte("test parquet data"),
			RecordCount: 10,
			Timestamp:   time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
		}

		err := writer.Write(ctx, opts)
		require.NoError(t, err)
	})

	t.Run("Write logs data", func(t *testing.T) {
		opts := WriteOptions{
			SignalType:  "logs",
			Schema:      nil,
			Data:        []byte("test logs data"),
			RecordCount: 5,
			Timestamp:   time.Date(2024, 6, 15, 15, 0, 0, 0, time.UTC),
		}

		err := writer.Write(ctx, opts)
		require.NoError(t, err)
	})

	t.Run("Write metrics data", func(t *testing.T) {
		opts := WriteOptions{
			SignalType:  "metrics",
			Schema:      nil,
			Data:        []byte("test metrics data"),
			RecordCount: 20,
			Timestamp:   time.Date(2024, 6, 15, 16, 0, 0, 0, time.UTC),
		}

		err := writer.Write(ctx, opts)
		require.NoError(t, err)
	})
}

func TestIcebergWriter_Close(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	tmpDir := t.TempDir()

	cfg := WriterConfig{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: tmpDir,
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
	}

	writer, err := NewIcebergWriter(ctx, cfg, vlogger)
	require.NoError(t, err)

	// Close should not error
	err = writer.Close()
	assert.NoError(t, err)

	// Multiple closes should be safe (may or may not error depending on implementation)
	_ = writer.Close()
}

func TestPartitionConfig(t *testing.T) {
	cfg := PartitionConfig{
		Granularity: "daily",
		Timezone:    "Europe/London",
	}

	assert.Equal(t, "daily", cfg.Granularity)
	assert.Equal(t, "Europe/London", cfg.Timezone)
}
