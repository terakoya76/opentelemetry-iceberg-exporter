package icebergexporter

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap/zaptest"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

func TestNewIcebergWriter(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()

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

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
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

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
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

		_, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage")
	})
}

func TestIcebergWriter_Write(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()
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

	writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	t.Run("Write with nil record returns nil", func(t *testing.T) {
		opts := WriteOptions{
			SignalType: "traces",
			Record:     nil,
		}

		err := writer.Write(ctx, opts)
		require.NoError(t, err)
	})
}

func TestIcebergWriter_Close(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()
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

	writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
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

func TestIcebergWriter_EnsureAllTables(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()

	t.Run("With catalog type none - skips table creation", func(t *testing.T) {
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

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		// Should not error even with nil schemas since catalog is "none"
		tables := []TableConfig{
			{SignalType: "traces", Schema: nil},
			{SignalType: "logs", Schema: nil},
		}

		err = writer.EnsureAllTables(ctx, tables)
		assert.NoError(t, err)
	})

	t.Run("With empty tables list", func(t *testing.T) {
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

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		err = writer.EnsureAllTables(ctx, []TableConfig{})
		assert.NoError(t, err)
	})
}

func TestIcebergWriter_GetPartitionField(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()
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

	writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	tests := []struct {
		signalType string
		expected   string
	}{
		{"traces", "start_time_unix_nano"},
		{"logs", "time_unix_nano"},
		{"metrics_gauge", "time_unix_nano"},
		{"metrics_sum", "time_unix_nano"},
		{"metrics_histogram", "time_unix_nano"},
		{"metrics_exponential_histogram", "time_unix_nano"},
		{"metrics_summary", "time_unix_nano"},
		{"unknown", ""},
	}

	for _, tc := range tests {
		t.Run(tc.signalType, func(t *testing.T) {
			result := writer.getPartitionField(tc.signalType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIcebergWriter_GetCompression(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	allocator := memory.NewGoAllocator()
	tmpDir := t.TempDir()

	t.Run("Filesystem compression", func(t *testing.T) {
		cfg := WriterConfig{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath:    tmpDir,
					Compression: "zstd",
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "none",
			},
		}

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		assert.Equal(t, "zstd", writer.getCompression())
	})

	t.Run("Default compression", func(t *testing.T) {
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

		writer, err := NewIcebergWriter(ctx, cfg, allocator, vlogger)
		require.NoError(t, err)
		defer func() { _ = writer.Close() }()

		// Default should be snappy
		assert.Equal(t, "snappy", writer.getCompression())
	})
}
