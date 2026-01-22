package icebergexporter

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	iarrow "github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// icebergExporter exports telemetry data to Parquet files with Iceberg catalog support.
type icebergExporter struct {
	config *Config
	logger *logger.VerboseLogger
	writer *IcebergWriter

	// Arrow converters
	tracesConverter  *iarrow.TracesConverter
	metricsConverter *iarrow.MetricsConverter
	logsConverter    *iarrow.LogsConverter

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
		tracesConverter:  iarrow.NewTracesConverter(allocator),
		metricsConverter: iarrow.NewMetricsConverter(allocator),
		logsConverter:    iarrow.NewLogsConverter(allocator),
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
	w, err := NewIcebergWriter(ctx, writerCfg, e.allocator, e.logger)
	if err != nil {
		return fmt.Errorf("failed to create Iceberg writer: %w", err)
	}
	e.writer = w

	// Ensure all tables exist at startup
	tables := []TableConfig{
		{SignalType: "traces", Schema: iarrow.TracesSchema()},
		{SignalType: "logs", Schema: iarrow.LogsSchema()},
		{SignalType: iarrow.MetricTypeGauge.SignalType(), Schema: iarrow.GetMetricSchema(iarrow.MetricTypeGauge)},
		{SignalType: iarrow.MetricTypeSum.SignalType(), Schema: iarrow.GetMetricSchema(iarrow.MetricTypeSum)},
		{SignalType: iarrow.MetricTypeHistogram.SignalType(), Schema: iarrow.GetMetricSchema(iarrow.MetricTypeHistogram)},
		{SignalType: iarrow.MetricTypeExponentialHistogram.SignalType(), Schema: iarrow.GetMetricSchema(iarrow.MetricTypeExponentialHistogram)},
		{SignalType: iarrow.MetricTypeSummary.SignalType(), Schema: iarrow.GetMetricSchema(iarrow.MetricTypeSummary)},
	}
	if err := w.EnsureAllTables(ctx, tables); err != nil {
		// Close writer on failure
		_ = w.Close()
		return fmt.Errorf("failed to ensure tables exist: %w", err)
	}

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

// consumeTraces exports traces to Iceberg.
// The writer internally handles catalog vs FileIO mode.
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

	// Write using unified Write method (handles catalog/FileIO dispatch internally)
	opts := WriteOptions{
		SignalType: "traces",
		Record:     record,
	}

	if err := e.writer.Write(ctx, opts); err != nil {
		return fmt.Errorf("failed to write traces: %w", err)
	}

	e.logTracesExport(traces, int(record.NumRows()))

	return nil
}

// consumeMetrics exports metrics to Iceberg with separated tables per metric type.
// The writer internally handles catalog vs FileIO mode.
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

	var totalRecords int64
	var writeErr error

	// Write each metric type using unified Write method
	metricRecords.ForEach(func(metricType iarrow.MetricType, record arrow.RecordBatch) {
		if writeErr != nil {
			return // Skip remaining on first error
		}

		opts := WriteOptions{
			SignalType: metricType.SignalType(),
			Record:     record,
		}

		if err := e.writer.Write(ctx, opts); err != nil {
			writeErr = fmt.Errorf("failed to write metrics %s: %w", metricType, err)
			return
		}

		totalRecords += record.NumRows()
	})

	if writeErr != nil {
		return writeErr
	}

	e.logMetricsExport(metrics, totalRecords)

	return nil
}

// consumeLogs exports logs to Iceberg.
// The writer internally handles catalog vs FileIO mode.
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

	// Write using unified Write method (handles catalog/FileIO dispatch internally)
	opts := WriteOptions{
		SignalType: "logs",
		Record:     record,
	}

	if err := e.writer.Write(ctx, opts); err != nil {
		return fmt.Errorf("failed to write logs: %w", err)
	}

	e.logLogsExport(logs, int(record.NumRows()))

	return nil
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

// logTracesExport logs trace export information.
func (e *icebergExporter) logTracesExport(traces ptrace.Traces, recordCount int) {
	if e.logger.IsNormal() {
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
			zap.Int("records", recordCount),
		)
	} else if e.logger.IsDetailed() {
		serviceName := extractServiceNameFromTraces(traces)
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
			zap.Int("records", recordCount),
			zap.String("service_name", serviceName),
			zap.Int("resource_spans", traces.ResourceSpans().Len()),
		)
	}
}

// logMetricsExport logs metrics export information.
func (e *icebergExporter) logMetricsExport(metrics pmetric.Metrics, totalRecords int64) {
	if e.logger.IsNormal() {
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
			zap.Int64("records", totalRecords),
		)
	} else if e.logger.IsDetailed() {
		serviceName := extractServiceNameFromMetrics(metrics)
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
			zap.Int("metric_count", metrics.MetricCount()),
			zap.Int64("records", totalRecords),
			zap.String("service_name", serviceName),
			zap.Int("resource_metrics", metrics.ResourceMetrics().Len()),
		)
	}
}

// logLogsExport logs log export information.
func (e *icebergExporter) logLogsExport(logs plog.Logs, recordCount int) {
	if e.logger.IsNormal() {
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
			zap.Int("written", recordCount),
		)
	} else if e.logger.IsDetailed() {
		serviceName := extractServiceNameFromLogs(logs)
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
			zap.Int("written", recordCount),
			zap.String("service_name", serviceName),
			zap.Int("resource_logs", logs.ResourceLogs().Len()),
		)
	}
}
