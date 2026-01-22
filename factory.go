package icebergexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

const (
	// TypeStr is the type identifier for this exporter.
	TypeStr = "iceberg"

	// DefaultPartitionGranularity is the default partition granularity.
	DefaultPartitionGranularity = "hourly"

	// DefaultTimezone is the default timezone.
	DefaultTimezone = "UTC"

	// DefaultCompression is the default compression algorithm.
	DefaultCompression = "snappy"

	// DefaultTimeout is the default timeout for export operations.
	// This is longer than the OTEL default (5s) because Iceberg catalog operations
	// require: LoadTable (REST) + AddFiles (reads Parquet metadata from storage) + Commit (REST).
	// The AddFiles operation can be slow as it fetches file metadata from S3/R2.
	DefaultTimeout = 60 * time.Second
)

// NewFactory creates a new factory for the Iceberg exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(TypeStr),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelDevelopment),
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutConfig: exporterhelper.TimeoutConfig{
			Timeout: DefaultTimeout,
		},
		QueueConfig:   configoptional.Default(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		Verbosity:     configtelemetry.LevelNormal,
		Storage: iceberg.FileIOConfig{
			Type: "s3",
			S3: iceberg.S3FileIOConfig{
				Compression: DefaultCompression,
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type:      "none", // Default to no catalog
			Namespace: "default",
			Tables: iceberg.TableNamesConfig{
				Traces:  "otel_traces",
				Logs:    "otel_logs",
				Metrics: "otel_metrics",
			},
		},
		Partition: PartitionConfig{
			Granularity: DefaultPartitionGranularity,
			Timezone:    DefaultTimezone,
		},
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)

	exp, err := newIcebergExporter(c, set)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.consumeTraces,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)

	exp, err := newIcebergExporter(c, set)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.consumeMetrics,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)

	exp, err := newIcebergExporter(c, set)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.consumeLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
