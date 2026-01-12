package icebergexporter

import (
	"context"
	"fmt"
	"time"

	oarrow "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// icebergExporter exports telemetry data to Parquet files with Iceberg catalog support.
type icebergExporter struct {
	config *Config
	logger *logger.VerboseLogger
	writer *IcebergWriter

	// Arrow converters
	tracesConverter  *arrow.TracesConverter
	metricsConverter *arrow.MetricsConverter
	logsConverter    *arrow.LogsConverter

	// Memory allocator for Arrow
	allocator memory.Allocator
}

// newIcebergExporter creates a new Iceberg exporter.
func newIcebergExporter(cfg *Config, set exporter.Settings) (*icebergExporter, error) {
	allocator := memory.NewGoAllocator()
	vlogger := logger.New(set.Logger, cfg.Verbosity)

	return &icebergExporter{
		config:           cfg,
		logger:           vlogger,
		allocator:        allocator,
		tracesConverter:  arrow.NewTracesConverter(allocator),
		metricsConverter: arrow.NewMetricsConverter(allocator),
		logsConverter:    arrow.NewLogsConverter(allocator),
	}, nil
}

// start initializes the exporter.
func (e *icebergExporter) start(ctx context.Context, _ component.Host) error {
	// Build writer configuration
	writerCfg := WriterConfig{
		Storage:   e.config.Storage,
		Catalog:   e.config.Catalog,
		Partition: e.config.Partition,
	}

	// Create the Iceberg writer
	w, err := NewIcebergWriter(ctx, writerCfg, e.logger)
	if err != nil {
		return fmt.Errorf("failed to create Iceberg writer: %w", err)
	}
	e.writer = w

	e.logger.Info("Iceberg exporter started",
		zap.String("storage_type", w.GetStorageType()),
		zap.String("catalog_type", w.GetCatalogType()),
		zap.String("verbosity", e.config.Verbosity.String()),
	)

	return nil
}

// shutdown shuts down the exporter.
func (e *icebergExporter) shutdown(_ context.Context) error {
	if e.writer != nil {
		return e.writer.Close()
	}
	return nil
}

// consumeTraces exports traces to Parquet.
func (e *icebergExporter) consumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if traces.SpanCount() == 0 {
		return nil
	}

	// Convert to Arrow
	record, err := e.tracesConverter.Convert(traces)
	if err != nil {
		return fmt.Errorf("failed to convert traces to Arrow: %w", err)
	}
	defer record.Release()

	// Write to Parquet
	data, err := arrow.WriteParquet(record, arrow.ParquetWriterOptions{
		Compression: e.getCompression(),
	})
	if err != nil {
		return fmt.Errorf("failed to write Parquet: %w", err)
	}

	// Extract service name from first span for partitioning
	serviceName := extractServiceNameFromTraces(traces)

	// Write using IcebergWriter
	opts := WriteOptions{
		SignalType:  "traces",
		Schema:      record.Schema(),
		Data:        data,
		RecordCount: int64(traces.SpanCount()),
		Timestamp:   time.Now(),
		ServiceName: serviceName,
	}

	if err := e.writer.Write(ctx, opts); err != nil {
		return fmt.Errorf("failed to write traces: %w", err)
	}

	e.logTracesExport(traces, data, serviceName)

	return nil
}

// consumeMetrics exports metrics to Parquet with separated tables per metric type.
func (e *icebergExporter) consumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	if metrics.DataPointCount() == 0 {
		return nil
	}

	// Convert to Arrow with separated schemas per metric type
	metricRecords, err := e.metricsConverter.Convert(metrics)
	if err != nil {
		return fmt.Errorf("failed to convert metrics to Arrow: %w", err)
	}
	defer metricRecords.Release()

	// Extract service name from first metric for partitioning
	serviceName := extractServiceNameFromMetrics(metrics)
	timestamp := time.Now()

	var totalBytes int
	var totalRecords int64

	// Write each metric type to its own table
	metricRecords.ForEach(func(metricType arrow.MetricType, record oarrow.RecordBatch) {
		// Write to Parquet
		data, writeErr := arrow.WriteParquet(record, arrow.ParquetWriterOptions{
			Compression: e.getCompression(),
		})
		if writeErr != nil {
			e.logger.Error("failed to write Parquet for metric type",
				zap.String("metric_type", string(metricType)),
				zap.Error(writeErr))
			return
		}

		// Write using IcebergWriter
		opts := WriteOptions{
			SignalType:  metricType.SignalType(),
			Schema:      record.Schema(),
			Data:        data,
			RecordCount: record.NumRows(),
			Timestamp:   timestamp,
			ServiceName: serviceName,
		}

		if writeErr := e.writer.Write(ctx, opts); writeErr != nil {
			e.logger.Error("failed to write metrics",
				zap.String("metric_type", string(metricType)),
				zap.Error(writeErr))
			return
		}

		totalBytes += len(data)
		totalRecords += record.NumRows()
	})

	e.logMetricsExport(metrics, totalBytes, totalRecords, serviceName)

	return nil
}

// consumeLogs exports logs to Parquet.
func (e *icebergExporter) consumeLogs(ctx context.Context, logs plog.Logs) error {
	if logs.LogRecordCount() == 0 {
		return nil
	}

	// Convert to Arrow
	record, err := e.logsConverter.Convert(logs)
	if err != nil {
		return fmt.Errorf("failed to convert logs to Arrow: %w", err)
	}
	defer record.Release()

	// Write to Parquet
	data, err := arrow.WriteParquet(record, arrow.ParquetWriterOptions{
		Compression: e.getCompression(),
	})
	if err != nil {
		return fmt.Errorf("failed to write Parquet: %w", err)
	}

	// Extract service name from first log for partitioning
	serviceName := extractServiceNameFromLogs(logs)

	// Write using IcebergWriter
	opts := WriteOptions{
		SignalType:  "logs",
		Schema:      record.Schema(),
		Data:        data,
		RecordCount: int64(logs.LogRecordCount()),
		Timestamp:   time.Now(),
		ServiceName: serviceName,
	}

	if err := e.writer.Write(ctx, opts); err != nil {
		return fmt.Errorf("failed to write logs: %w", err)
	}

	e.logLogsExport(logs, data, serviceName)

	return nil
}

// getCompression returns the compression setting from storage config.
func (e *icebergExporter) getCompression() string {
	switch e.config.Storage.Type {
	case "s3", "":
		return e.config.Storage.S3.GetCompression()
	case "r2":
		return e.config.Storage.R2.GetCompression()
	case "filesystem":
		return e.config.Storage.Filesystem.GetCompression()
	default:
		return "snappy"
	}
}

// Helper functions

func extractServiceNameFromTraces(traces ptrace.Traces) string {
	rs := traces.ResourceSpans()
	if rs.Len() == 0 {
		return ""
	}
	resource := rs.At(0).Resource()
	if val, ok := resource.Attributes().Get("service.name"); ok {
		return val.AsString()
	}
	return ""
}

func extractServiceNameFromMetrics(metrics pmetric.Metrics) string {
	rm := metrics.ResourceMetrics()
	if rm.Len() == 0 {
		return ""
	}
	resource := rm.At(0).Resource()
	if val, ok := resource.Attributes().Get("service.name"); ok {
		return val.AsString()
	}
	return ""
}

func extractServiceNameFromLogs(logs plog.Logs) string {
	rl := logs.ResourceLogs()
	if rl.Len() == 0 {
		return ""
	}
	resource := rl.At(0).Resource()
	if val, ok := resource.Attributes().Get("service.name"); ok {
		return val.AsString()
	}
	return ""
}

// logTracesExport logs trace export information based on verbosity level.
func (e *icebergExporter) logTracesExport(traces ptrace.Traces, data []byte, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
			zap.Int("bytes", len(data)),
			zap.String("service_name", serviceName),
			zap.Int("resource_spans", traces.ResourceSpans().Len()),
		)
	}
}

// logMetricsExport logs metrics export information based on verbosity level.
func (e *icebergExporter) logMetricsExport(metrics pmetric.Metrics, totalBytes int, totalRecords int64, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
			zap.Int("metric_count", metrics.MetricCount()),
			zap.Int64("records_written", totalRecords),
			zap.Int("bytes", totalBytes),
			zap.String("service_name", serviceName),
			zap.Int("resource_metrics", metrics.ResourceMetrics().Len()),
		)
	}
}

// logLogsExport logs log export information based on verbosity level.
func (e *icebergExporter) logLogsExport(logs plog.Logs, data []byte, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
			zap.Int("bytes", len(data)),
			zap.String("service_name", serviceName),
			zap.Int("resource_logs", logs.ResourceLogs().Len()),
		)
	}
}
