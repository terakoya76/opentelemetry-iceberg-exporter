package icebergexporter

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"go.uber.org/zap"

	iarrow "github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
	iiceberg "github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// IcebergWriter orchestrates writing data to Iceberg tables.
//
// Catalog mode (REST, etc.):
//   - Uses iceberg-go's table.Append() which handles partitioning, parquet writing,
//     and catalog registration atomically.
//   - Permanent errors are logged but not propagated (retrying won't help).
//   - Transient errors are propagated to allow OTel collector retry.
//
// No catalog mode ("none"):
//   - Splits data by partition internally.
//   - Converts to parquet and writes directly to storage.
//   - No catalog registration performed.
//   - All storage errors are propagated.
type IcebergWriter struct {
	fileIO        iiceberg.FileIO
	catalog       iiceberg.Catalog
	pathGenerator *iiceberg.PathGenerator
	tableNames    iiceberg.TableNamesConfig
	namespace     string
	granularity   string
	logger        *logger.VerboseLogger

	// tablesInitialized tracks which tables have been initialized.
	tablesInitialized map[string]bool

	// For FileIO-based writing in "none" catalog mode
	allocator     memory.Allocator
	storageConfig iiceberg.FileIOConfig
	timezone      string
}

// WriterConfig holds the configuration for IcebergWriter.
type WriterConfig struct {
	// Storage configuration
	Storage iiceberg.FileIOConfig

	// Catalog configuration
	Catalog iiceberg.CatalogConfig

	// Partition configuration
	Partition PartitionConfig
}

// PartitionConfig holds partition configuration.
type PartitionConfig struct {
	// Granularity is the partition granularity: "hourly", "daily", "monthly"
	Granularity string `mapstructure:"granularity"`

	// Timezone is the timezone for partition values.
	Timezone string `mapstructure:"timezone"`
}

// NewIcebergWriter creates a new IcebergWriter.
func NewIcebergWriter(ctx context.Context, cfg WriterConfig, allocator memory.Allocator, vlogger *logger.VerboseLogger) (*IcebergWriter, error) {
	// Validate context - this is critical for REST catalog connections
	if ctx == nil {
		return nil, fmt.Errorf("context is required for IcebergWriter initialization")
	}

	// Create FileIO
	fileIO, err := iiceberg.NewFileIO(ctx, cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to create FileIO: %w", err)
	}

	// Create catalog (may be NoCatalog if not configured)
	// Pass storage config to enable AWS config injection for bypassing iceberg-go's
	// unsupported S3 property parsing (e.g., s3.signer.uri from REST catalogs)
	cat, err := iiceberg.NewCatalog(ctx, cfg.Catalog, cfg.Storage, vlogger)
	if err != nil {
		// Close FileIO on error (ignore close error since we're already returning an error)
		_ = fileIO.Close()
		return nil, fmt.Errorf("failed to initialize catalog (type=%q): %w", cfg.Catalog.Type, err)
	}

	// Create path generator
	pathConfig := iiceberg.PathConfig{
		Granularity: cfg.Partition.Granularity,
		Timezone:    cfg.Partition.Timezone,
	}
	if pathConfig.Granularity == "" {
		pathConfig.Granularity = "hourly"
	}
	timezone := cfg.Partition.Timezone
	if timezone == "" {
		timezone = "UTC"
	}

	pathGen, err := iiceberg.NewPathGenerator(pathConfig)
	if err != nil {
		// Close resources on error (ignore close errors since we're already returning an error)
		_ = fileIO.Close()
		_ = cat.Close()
		return nil, fmt.Errorf("failed to create path generator: %w", err)
	}

	namespace := cfg.Catalog.GetNamespace()

	return &IcebergWriter{
		fileIO:            fileIO,
		catalog:           cat,
		pathGenerator:     pathGen,
		tableNames:        cfg.Catalog.Tables,
		namespace:         namespace,
		granularity:       pathConfig.Granularity,
		logger:            vlogger,
		tablesInitialized: make(map[string]bool),
		allocator:         allocator,
		storageConfig:     cfg.Storage,
		timezone:          timezone,
	}, nil
}

// WriteOptions contains options for writing data to Iceberg.
type WriteOptions struct {
	// SignalType is the OTEL signal type: "traces", "logs", "metrics_gauge", etc.
	SignalType string

	// Record is the Arrow record batch to write.
	Record arrow.RecordBatch

	// Properties are optional snapshot properties to include in the commit (catalog mode only).
	Properties iceberg.Properties
}

// TableConfig holds configuration for a single table to be initialized.
type TableConfig struct {
	// SignalType is the OTEL signal type (e.g., "traces", "logs", "metrics_gauge")
	SignalType string

	// Schema is the Arrow schema for the table
	Schema *arrow.Schema
}

// Write writes Arrow records to Iceberg.
// It internally dispatches based on catalog type:
//   - Catalog mode: uses iceberg-go's table.Append() for atomic writes with automatic partitioning
//   - No catalog mode ("none"): splits by partition and writes parquet files directly to storage
//
// Error handling:
//   - Catalog mode permanent errors: logged but not propagated
//   - Catalog mode transient errors: propagated for OTel collector retry
//   - No catalog mode errors: always propagated
func (w *IcebergWriter) Write(ctx context.Context, opts WriteOptions) error {
	if opts.Record == nil || opts.Record.NumRows() == 0 {
		return nil
	}

	if w.catalog.GetCatalogType() == iiceberg.CatalogTypeNone {
		return w.writeFileIO(ctx, opts)
	}
	return w.writeCatalog(ctx, opts)
}

// writeFileIO writes data using FileIO (for catalog "none" mode).
// It splits the data by partition and writes each partition as a separate parquet file.
func (w *IcebergWriter) writeFileIO(ctx context.Context, opts WriteOptions) error {
	tableName := w.tableNames.GetTableName(opts.SignalType)
	partitionField := w.getPartitionField(opts.SignalType)

	// Split by partition if partitioning is configured
	var partitionedBatches []iarrow.PartitionedBatch
	var err error

	if partitionField != "" {
		partitionedBatches, err = iarrow.SplitByPartition(opts.Record, partitionField, w.allocator, w.timezone)
		if err != nil {
			return fmt.Errorf("failed to split by partition: %w", err)
		}
		defer func() {
			for _, pb := range partitionedBatches {
				pb.Release()
			}
		}()
	} else {
		// No partitioning - create a single batch with current time
		partitionedBatches = []iarrow.PartitionedBatch{{
			Batch:       opts.Record,
			Timestamp:   time.Now(),
			RecordCount: opts.Record.NumRows(),
		}}
	}

	// Write each partitioned batch
	var totalBytes int
	compression := w.getCompression()

	for _, pb := range partitionedBatches {
		// Convert to parquet
		data, err := iarrow.WriteParquet(pb.Batch, iarrow.ParquetWriterOptions{
			Compression: compression,
		})
		if err != nil {
			return fmt.Errorf("failed to write parquet: %w", err)
		}

		// Generate path
		pathOpts := iiceberg.PathOptions{
			TableName: tableName,
			Timestamp: pb.Timestamp,
		}
		path := w.pathGenerator.GeneratePath(pathOpts)
		fileURI := w.fileIO.GetURI(path)

		// Write to storage
		if err := w.fileIO.Write(ctx, path, data, iiceberg.DefaultWriteOptions()); err != nil {
			w.logger.Error("parquet upload failed",
				zap.String("failure_type", "upload"),
				zap.String("signal_type", opts.SignalType),
				zap.String("namespace", w.namespace),
				zap.String("table", tableName),
				zap.String("target_path", path),
				zap.String("target_uri", fileURI),
				zap.Int64("record_count", pb.RecordCount),
				zap.Int("data_size_bytes", len(data)),
				zap.Error(err))
			return fmt.Errorf("failed to write to storage: %w", err)
		}

		totalBytes += len(data)

		w.logger.Debug("wrote parquet file to storage",
			zap.String("path", path),
			zap.Int("bytes", len(data)),
			zap.Int64("records", pb.RecordCount))
	}

	w.logger.Debug("completed FileIO write",
		zap.String("signal_type", opts.SignalType),
		zap.String("table", tableName),
		zap.Int("partitions", len(partitionedBatches)),
		zap.Int("total_bytes", totalBytes),
		zap.Int64("total_records", opts.Record.NumRows()))

	return nil
}

// writeCatalog writes data using catalog.AppendRecords() (for catalog-enabled modes).
func (w *IcebergWriter) writeCatalog(ctx context.Context, opts WriteOptions) error {
	tableName := w.tableNames.GetTableName(opts.SignalType)
	recordCount := opts.Record.NumRows()

	w.logger.Debug("writing Arrow records via catalog.AppendRecords",
		zap.String("signal_type", opts.SignalType),
		zap.String("namespace", w.namespace),
		zap.String("table", tableName),
		zap.Int64("records", recordCount))

	if err := w.catalog.AppendRecords(ctx, w.namespace, tableName, opts.Record, opts.Properties); err != nil {
		// Check if this is a permanent error (auth, permission, validation)
		if iiceberg.IsPermanentCatalogError(err) {
			// Log error - data was NOT written (atomic failure)
			w.logger.Error("append records failed permanently",
				zap.String("failure_type", "append_records_permanent"),
				zap.String("signal_type", opts.SignalType),
				zap.String("namespace", w.namespace),
				zap.String("table", tableName),
				zap.Int64("record_count", recordCount),
				zap.Error(err))
			// Return nil - manual intervention required
			return nil
		}

		// Transient error - propagate to OTel collector for retry
		w.logger.Warn("append records failed with transient error, will retry",
			zap.String("failure_type", "append_records_transient"),
			zap.String("signal_type", opts.SignalType),
			zap.String("namespace", w.namespace),
			zap.String("table", tableName),
			zap.Int64("record_count", recordCount),
			zap.Error(err))
		return fmt.Errorf("append records failed (transient): %w", err)
	}

	w.logger.Debug("successfully wrote records via catalog",
		zap.String("table", tableName),
		zap.Int64("records", recordCount))

	return nil
}

// getPartitionField returns the timestamp field name for partitioning based on signal type.
func (w *IcebergWriter) getPartitionField(signalType string) string {
	switch signalType {
	case "traces":
		return iarrow.FieldTraceStartTimeUnixNano
	case "logs":
		return iarrow.FieldLogTimeUnixNano
	case "metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary":
		return iarrow.FieldMetricTimeUnixNano
	default:
		return ""
	}
}

// getCompression returns the compression setting from storage config.
func (w *IcebergWriter) getCompression() string {
	switch w.storageConfig.Type {
	case "s3", "":
		return w.storageConfig.S3.GetCompression()
	case "r2":
		return w.storageConfig.R2.GetCompression()
	case "filesystem":
		return w.storageConfig.Filesystem.GetCompression()
	default:
		return "snappy"
	}
}

// EnsureAllTables ensures all specified tables exist in the catalog at startup.
// This should be called during exporter initialization to ensure tables are ready
// before any data is written.
func (w *IcebergWriter) EnsureAllTables(ctx context.Context, tables []TableConfig) error {
	if w.catalog.GetCatalogType() == iiceberg.CatalogTypeNone {
		return nil
	}

	if err := w.catalog.EnsureNamespace(ctx, w.namespace); err != nil {
		return fmt.Errorf("failed to ensure namespace: %w", err)
	}

	for _, table := range tables {
		tableName := w.tableNames.GetTableName(table.SignalType)
		if w.tablesInitialized[tableName] {
			continue
		}

		partitionSpec := w.createPartitionSpec(table.SignalType)
		if err := w.catalog.EnsureTable(ctx, w.namespace, tableName, table.Schema, partitionSpec); err != nil {
			return fmt.Errorf("failed to ensure table %s: %w", tableName, err)
		}

		w.tablesInitialized[tableName] = true
		w.logger.Debug("ensured table exists",
			zap.String("namespace", w.namespace),
			zap.String("table", tableName),
			zap.String("signal_type", table.SignalType))
	}

	return nil
}

// createPartitionSpec creates a partition spec based on signal type and granularity.
func (w *IcebergWriter) createPartitionSpec(signalType string) iiceberg.PartitionSpec {
	switch signalType {
	case "traces":
		return iiceberg.OTELPartitionSpec(iarrow.FieldTraceStartTimeUnixNano, w.granularity)
	case "logs":
		return iiceberg.OTELPartitionSpec(iarrow.FieldLogTimeUnixNano, w.granularity)
	case "metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary":
		return iiceberg.OTELPartitionSpec(iarrow.FieldMetricTimeUnixNano, w.granularity)
	default:
		return iiceberg.PartitionSpec{} // Unpartitioned
	}
}

// Close closes the writer and releases resources.
func (w *IcebergWriter) Close() error {
	var errs []error

	if err := w.fileIO.Close(); err != nil {
		errs = append(errs, fmt.Errorf("fileIO close: %w", err))
	}

	if err := w.catalog.Close(); err != nil {
		errs = append(errs, fmt.Errorf("catalog close: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}

// GetStorageType returns the storage type.
func (w *IcebergWriter) GetStorageType() string {
	return w.fileIO.GetFileIOType()
}

// GetCatalogType returns the catalog type.
func (w *IcebergWriter) GetCatalogType() string {
	return w.catalog.GetCatalogType()
}
