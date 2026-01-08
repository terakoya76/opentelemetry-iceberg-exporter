package icebergexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

func TestConfigValidate(t *testing.T) {
	t.Run("Valid config with filesystem storage", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: "/tmp/test",
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
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("Invalid partition granularity", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: "/tmp/test",
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "none",
			},
			Partition: PartitionConfig{
				Granularity: "invalid",
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "granularity")
	})

	t.Run("Invalid storage config", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "r2",
				R2: iceberg.R2FileIOConfig{
					// Missing required account_id
					Bucket: "test-bucket",
				},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
	})

	t.Run("Invalid catalog config", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: "/tmp/test",
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "rest",
				REST: iceberg.RESTCatalogConfig{
					// Missing required URI
				},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rest.uri")
	})

	t.Run("Empty partition granularity is valid (defaults)", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: "/tmp/test",
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "none",
			},
			Partition: PartitionConfig{
				Granularity: "",
			},
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("All valid granularities", func(t *testing.T) {
		for _, gran := range []string{"hourly", "daily", "monthly"} {
			cfg := &Config{
				Storage: iceberg.FileIOConfig{
					Type: "filesystem",
					Filesystem: iceberg.LocalFileIOConfig{
						BasePath: "/tmp/test",
					},
				},
				Catalog: iceberg.CatalogConfig{
					Type: "none", // Required: explicitly disable catalog
				},
				Partition: PartitionConfig{
					Granularity: gran,
				},
			}
			err := cfg.Validate()
			assert.NoError(t, err, "granularity %s should be valid", gran)
		}
	})

	t.Run("Empty catalog type is invalid", func(t *testing.T) {
		cfg := &Config{
			Storage: iceberg.FileIOConfig{
				Type: "filesystem",
				Filesystem: iceberg.LocalFileIOConfig{
					BasePath: "/tmp/test",
				},
			},
			Catalog: iceberg.CatalogConfig{
				Type: "", // Missing catalog type should fail
			},
			Partition: PartitionConfig{
				Granularity: "hourly",
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "catalog.type is required")
	})
}

func TestNewFactory(t *testing.T) {
	factory := NewFactory()

	assert.Equal(t, "iceberg", factory.Type().String())

	// Check default config
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)

	icebergCfg, ok := cfg.(*Config)
	require.True(t, ok)

	assert.Equal(t, "s3", icebergCfg.Storage.Type)
	assert.Equal(t, "none", icebergCfg.Catalog.Type)
	assert.Equal(t, "hourly", icebergCfg.Partition.Granularity)
	assert.Equal(t, "UTC", icebergCfg.Partition.Timezone)
}

func TestNewIcebergExporter(t *testing.T) {
	cfg := &Config{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: t.TempDir(),
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType(TypeStr))

	exp, err := newIcebergExporter(cfg, set)
	require.NoError(t, err)
	require.NotNil(t, exp)

	assert.NotNil(t, exp.tracesConverter)
	assert.NotNil(t, exp.metricsConverter)
	assert.NotNil(t, exp.logsConverter)
}

func TestExtractServiceNameFromTraces(t *testing.T) {
	t.Run("With service name", func(t *testing.T) {
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "my-service")
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()

		serviceName := extractServiceNameFromTraces(traces)
		assert.Equal(t, "my-service", serviceName)
	})

	t.Run("Without service name", func(t *testing.T) {
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()

		serviceName := extractServiceNameFromTraces(traces)
		assert.Equal(t, "", serviceName)
	})

	t.Run("Empty traces", func(t *testing.T) {
		traces := ptrace.NewTraces()

		serviceName := extractServiceNameFromTraces(traces)
		assert.Equal(t, "", serviceName)
	})
}

func TestExtractServiceNameFromMetrics(t *testing.T) {
	t.Run("With service name", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "metrics-service")
		rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()

		serviceName := extractServiceNameFromMetrics(metrics)
		assert.Equal(t, "metrics-service", serviceName)
	})

	t.Run("Without service name", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()

		serviceName := extractServiceNameFromMetrics(metrics)
		assert.Equal(t, "", serviceName)
	})

	t.Run("Empty metrics", func(t *testing.T) {
		metrics := pmetric.NewMetrics()

		serviceName := extractServiceNameFromMetrics(metrics)
		assert.Equal(t, "", serviceName)
	})
}

func TestExtractServiceNameFromLogs(t *testing.T) {
	t.Run("With service name", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "logs-service")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

		serviceName := extractServiceNameFromLogs(logs)
		assert.Equal(t, "logs-service", serviceName)
	})

	t.Run("Without service name", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

		serviceName := extractServiceNameFromLogs(logs)
		assert.Equal(t, "", serviceName)
	})

	t.Run("Empty logs", func(t *testing.T) {
		logs := plog.NewLogs()

		serviceName := extractServiceNameFromLogs(logs)
		assert.Equal(t, "", serviceName)
	})
}

func TestIcebergExporterStartShutdown(t *testing.T) {
	ctx := context.Background()

	cfg := &Config{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: t.TempDir(),
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

	set := exportertest.NewNopSettings(component.MustNewType(TypeStr))

	exp, err := newIcebergExporter(cfg, set)
	require.NoError(t, err)

	// Start the exporter
	err = exp.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	require.NotNil(t, exp.writer)

	// Shutdown the exporter
	err = exp.shutdown(ctx)
	require.NoError(t, err)
}

func TestIcebergExporterConsumeTraces(t *testing.T) {
	ctx := context.Background()

	cfg := &Config{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: t.TempDir(),
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType(TypeStr))

	exp, err := newIcebergExporter(cfg, set)
	require.NoError(t, err)

	err = exp.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() { _ = exp.shutdown(ctx) }()

	t.Run("Empty traces", func(t *testing.T) {
		traces := ptrace.NewTraces()
		err := exp.consumeTraces(ctx, traces)
		assert.NoError(t, err)
	})

	t.Run("Traces with data", func(t *testing.T) {
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "test-service")

		ss := rs.ScopeSpans().AppendEmpty()
		span := ss.Spans().AppendEmpty()
		span.SetName("test-span")
		span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
		span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

		err := exp.consumeTraces(ctx, traces)
		assert.NoError(t, err)
	})
}

func TestIcebergExporterConsumeLogs(t *testing.T) {
	ctx := context.Background()

	cfg := &Config{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: t.TempDir(),
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType(TypeStr))

	exp, err := newIcebergExporter(cfg, set)
	require.NoError(t, err)

	err = exp.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() { _ = exp.shutdown(ctx) }()

	t.Run("Empty logs", func(t *testing.T) {
		logs := plog.NewLogs()
		err := exp.consumeLogs(ctx, logs)
		assert.NoError(t, err)
	})

	t.Run("Logs with data", func(t *testing.T) {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "logs-service")

		sl := rl.ScopeLogs().AppendEmpty()
		log := sl.LogRecords().AppendEmpty()
		log.Body().SetStr("test log message")

		err := exp.consumeLogs(ctx, logs)
		assert.NoError(t, err)
	})
}

func TestIcebergExporterConsumeMetrics(t *testing.T) {
	ctx := context.Background()

	cfg := &Config{
		Storage: iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: t.TempDir(),
			},
		},
		Catalog: iceberg.CatalogConfig{
			Type: "none",
		},
	}

	set := exportertest.NewNopSettings(component.MustNewType(TypeStr))

	exp, err := newIcebergExporter(cfg, set)
	require.NoError(t, err)

	err = exp.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() { _ = exp.shutdown(ctx) }()

	t.Run("Empty metrics", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		err := exp.consumeMetrics(ctx, metrics)
		assert.NoError(t, err)
	})

	t.Run("Metrics with data", func(t *testing.T) {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "metrics-service")

		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("test_metric")
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(42.0)

		err := exp.consumeMetrics(ctx, metrics)
		assert.NoError(t, err)
	})
}

func TestGetCompression(t *testing.T) {
	t.Run("S3 storage", func(t *testing.T) {
		exp := &icebergExporter{
			config: &Config{
				Storage: iceberg.FileIOConfig{
					Type: "s3",
					S3: iceberg.S3FileIOConfig{
						Compression: "zstd",
					},
				},
			},
		}
		assert.Equal(t, "zstd", exp.getCompression())
	})

	t.Run("R2 storage", func(t *testing.T) {
		exp := &icebergExporter{
			config: &Config{
				Storage: iceberg.FileIOConfig{
					Type: "r2",
					R2: iceberg.R2FileIOConfig{
						Compression: "gzip",
					},
				},
			},
		}
		assert.Equal(t, "gzip", exp.getCompression())
	})

	t.Run("Filesystem storage", func(t *testing.T) {
		exp := &icebergExporter{
			config: &Config{
				Storage: iceberg.FileIOConfig{
					Type: "filesystem",
					Filesystem: iceberg.LocalFileIOConfig{
						Compression: "none",
					},
				},
			},
		}
		assert.Equal(t, "none", exp.getCompression())
	})

	t.Run("Unknown storage uses snappy", func(t *testing.T) {
		exp := &icebergExporter{
			config: &Config{
				Storage: iceberg.FileIOConfig{
					Type: "unknown",
				},
			},
		}
		assert.Equal(t, "snappy", exp.getCompression())
	})

	t.Run("Empty type uses S3 default", func(t *testing.T) {
		exp := &icebergExporter{
			config: &Config{
				Storage: iceberg.FileIOConfig{
					Type: "",
					S3: iceberg.S3FileIOConfig{
						Compression: "lz4",
					},
				},
			},
		}
		assert.Equal(t, "lz4", exp.getCompression())
	})
}
