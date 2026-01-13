package icebergexporter

import (
	"context"
	"testing"
	"time"

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
