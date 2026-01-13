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
// Data is split by hour partition to ensure each Parquet file belongs to a single Iceberg partition.
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

	// Extract service name from first span for partitioning
	serviceName := extractServiceNameFromTraces(traces)

	// Split by partition to ensure each file belongs to a single Iceberg partition
	timezone := e.config.Partition.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	partitionedBatches, err := iarrow.SplitByPartition(record, iarrow.FieldTraceStartTimeUnixNano, e.allocator, timezone)
	if err != nil {
		return fmt.Errorf("failed to split traces by partition: %w", err)
	}
	defer func() {
		for _, pb := range partitionedBatches {
			pb.Release()
		}
	}()

	// Write each partitioned batch to its own file
	var totalBytes int
	for _, pb := range partitionedBatches {
		// Write to Parquet
		data, err := iarrow.WriteParquet(pb.Batch, iarrow.ParquetWriterOptions{
			Compression: e.getCompression(),
		})
		if err != nil {
			return fmt.Errorf("failed to write Parquet: %w", err)
		}

		// Write using IcebergWriter with the partition timestamp
		opts := WriteOptions{
			SignalType:  "traces",
			Schema:      pb.Batch.Schema(),
			Data:        data,
			RecordCount: pb.RecordCount,
			Timestamp:   pb.Timestamp, // Use the partition timestamp, not time.Now()
		}

		if err := e.writer.Write(ctx, opts); err != nil {
			return fmt.Errorf("failed to write traces for partition %v: %w", pb.Timestamp, err)
		}

		totalBytes += len(data)
	}

	e.logTracesExportPartitioned(traces, totalBytes, len(partitionedBatches), serviceName)

	return nil
}

// consumeMetrics exports metrics to Parquet with separated tables per metric type.
// Each metric type is split by hour partition to ensure each Parquet file belongs to a single Iceberg partition.
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

	// Get timezone for partition splitting
	timezone := e.config.Partition.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	var totalBytes int
	var totalRecords int64
	var totalPartitions int

	// Write each metric type to its own table, splitting by partition
	metricRecords.ForEach(func(metricType iarrow.MetricType, record arrow.RecordBatch) {
		// Split by partition to ensure each file belongs to a single Iceberg partition
		partitionedBatches, splitErr := iarrow.SplitByPartition(record, iarrow.FieldMetricTimeUnixNano, e.allocator, timezone)
		if splitErr != nil {
			e.logger.Error("failed to split metrics by partition",
				zap.String("metric_type", string(metricType)),
				zap.Error(splitErr))
			return
		}
		defer func() {
			for _, pb := range partitionedBatches {
				pb.Release()
			}
		}()

		// Write each partitioned batch
		for _, pb := range partitionedBatches {
			// Write to Parquet
			data, writeErr := iarrow.WriteParquet(pb.Batch, iarrow.ParquetWriterOptions{
				Compression: e.getCompression(),
			})
			if writeErr != nil {
				e.logger.Error("failed to write Parquet for metric type",
					zap.String("metric_type", string(metricType)),
					zap.Error(writeErr))
				continue
			}

			// Write using IcebergWriter with the partition timestamp
			opts := WriteOptions{
				SignalType:  metricType.SignalType(),
				Schema:      pb.Batch.Schema(),
				Data:        data,
				RecordCount: pb.RecordCount,
				Timestamp:   pb.Timestamp, // Use the partition timestamp, not time.Now()
			}

			if writeErr := e.writer.Write(ctx, opts); writeErr != nil {
				e.logger.Error("failed to write metrics",
					zap.String("metric_type", string(metricType)),
					zap.Time("partition", pb.Timestamp),
					zap.Error(writeErr))
				continue
			}

			totalBytes += len(data)
			totalRecords += pb.RecordCount
		}
		totalPartitions += len(partitionedBatches)
	})

	e.logMetricsExportPartitioned(metrics, totalBytes, totalRecords, totalPartitions, serviceName)

	return nil
}

// consumeLogs exports logs to Parquet.
// Data is split by hour partition to ensure each Parquet file belongs to a single Iceberg partition.
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

	// Extract service name from first log for partitioning
	serviceName := extractServiceNameFromLogs(logs)

	// Split by partition to ensure each file belongs to a single Iceberg partition
	timezone := e.config.Partition.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	partitionedBatches, err := iarrow.SplitByPartition(record, iarrow.FieldLogTimeUnixNano, e.allocator, timezone)
	if err != nil {
		return fmt.Errorf("failed to split logs by partition: %w", err)
	}
	defer func() {
		for _, pb := range partitionedBatches {
			pb.Release()
		}
	}()

	// Write each partitioned batch to its own file
	var totalBytes int
	for _, pb := range partitionedBatches {
		// Write to Parquet
		data, err := iarrow.WriteParquet(pb.Batch, iarrow.ParquetWriterOptions{
			Compression: e.getCompression(),
		})
		if err != nil {
			return fmt.Errorf("failed to write Parquet: %w", err)
		}

		// Write using IcebergWriter with the partition timestamp
		opts := WriteOptions{
			SignalType:  "logs",
			Schema:      pb.Batch.Schema(),
			Data:        data,
			RecordCount: pb.RecordCount,
			Timestamp:   pb.Timestamp, // Use the partition timestamp, not time.Now()
		}

		if err := e.writer.Write(ctx, opts); err != nil {
			return fmt.Errorf("failed to write logs for partition %v: %w", pb.Timestamp, err)
		}

		totalBytes += len(data)
	}

	e.logLogsExportPartitioned(logs, totalBytes, len(partitionedBatches), serviceName)

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

// logTracesExportPartitioned logs trace export information with partition details.
func (e *icebergExporter) logTracesExportPartitioned(traces ptrace.Traces, totalBytes int, partitionCount int, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
			zap.Int("partitions", partitionCount),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Traces exported",
			zap.Int("spans", traces.SpanCount()),
			zap.Int("bytes", totalBytes),
			zap.Int("partitions", partitionCount),
			zap.String("service_name", serviceName),
			zap.Int("resource_spans", traces.ResourceSpans().Len()),
		)
	}
}

// logMetricsExportPartitioned logs metrics export information with partition details.
func (e *icebergExporter) logMetricsExportPartitioned(metrics pmetric.Metrics, totalBytes int, totalRecords int64, totalPartitions int, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
			zap.Int("partitions", totalPartitions),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Metrics exported",
			zap.Int("datapoints", metrics.DataPointCount()),
			zap.Int("metric_count", metrics.MetricCount()),
			zap.Int64("records_written", totalRecords),
			zap.Int("bytes", totalBytes),
			zap.Int("partitions", totalPartitions),
			zap.String("service_name", serviceName),
			zap.Int("resource_metrics", metrics.ResourceMetrics().Len()),
		)
	}
}

// logLogsExportPartitioned logs log export information with partition details.
func (e *icebergExporter) logLogsExportPartitioned(logs plog.Logs, totalBytes int, partitionCount int, serviceName string) {
	if e.logger.IsNormal() {
		// LevelNormal: log basic counts at Info level
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
			zap.Int("partitions", partitionCount),
		)
	} else if e.logger.IsDetailed() {
		// LevelDetailed: log full details at Info level
		e.logger.Info("Logs exported",
			zap.Int("records", logs.LogRecordCount()),
			zap.Int("bytes", totalBytes),
			zap.Int("partitions", partitionCount),
			zap.String("service_name", serviceName),
			zap.Int("resource_logs", logs.ResourceLogs().Len()),
		)
	}
}
