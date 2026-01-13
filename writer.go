package icebergexporter

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap"

	iarrow "github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// IcebergWriter orchestrates writing Parquet files to storage and registering them with the Iceberg catalog.
// It follows the "best-effort" pattern: data is ALWAYS written to storage first,
// and catalog registration is attempted afterward (failures are logged, not propagated).
type IcebergWriter struct {
	fileIO        iceberg.FileIO
	catalog       iceberg.Catalog
	pathGenerator *iceberg.PathGenerator
	tableNames    iceberg.TableNamesConfig
	namespace     string
	granularity   string
	logger        *logger.VerboseLogger

	// tablesInitialized tracks which tables have been initialized.
	tablesInitialized map[string]bool
}

// WriterConfig holds the configuration for IcebergWriter.
type WriterConfig struct {
	// Storage configuration
	Storage iceberg.FileIOConfig

	// Catalog configuration
	Catalog iceberg.CatalogConfig

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
func NewIcebergWriter(ctx context.Context, cfg WriterConfig, vlogger *logger.VerboseLogger) (*IcebergWriter, error) {
	// Validate context - this is critical for REST catalog connections
	if ctx == nil {
		return nil, fmt.Errorf("context is required for IcebergWriter initialization")
	}

	// Create FileIO
	fileIO, err := iceberg.NewFileIO(ctx, cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to create FileIO: %w", err)
	}

	// Create catalog (may be NoCatalog if not configured)
	// Pass storage config to enable AWS config injection for bypassing iceberg-go's
	// unsupported S3 property parsing (e.g., s3.signer.uri from REST catalogs)
	cat, err := iceberg.NewCatalog(ctx, cfg.Catalog, cfg.Storage, vlogger)
	if err != nil {
		// Close FileIO on error (ignore close error since we're already returning an error)
		_ = fileIO.Close()
		return nil, fmt.Errorf("failed to initialize catalog (type=%q): %w", cfg.Catalog.Type, err)
	}

	// Create path generator
	pathConfig := iceberg.PathConfig{
		Granularity: cfg.Partition.Granularity,
		Timezone:    cfg.Partition.Timezone,
	}
	if pathConfig.Granularity == "" {
		pathConfig.Granularity = "hourly"
	}
	if pathConfig.Timezone == "" {
		pathConfig.Timezone = "UTC"
	}

	pathGen, err := iceberg.NewPathGenerator(pathConfig)
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
	}, nil
}

// WriteOptions contains options for a single write operation.
type WriteOptions struct {
	// SignalType is the OTEL signal type: "traces", "logs", "metrics"
	SignalType string

	// Schema is the Arrow schema for the data (used for table creation)
	Schema *arrow.Schema

	// Data is the Parquet file data
	Data []byte

	// RecordCount is the number of records in the file
	RecordCount int64

	// Timestamp is the timestamp for partitioning
	Timestamp time.Time
}

// Write writes data to storage and registers it with the catalog.
// This method follows the "best-effort" pattern:
// 1. Data is ALWAYS written to storage first (ensures data durability)
// 2. Catalog registration is attempted afterward (failures are logged, not propagated)
//
// Error Logging for Recovery:
// - Upload failures: Log with "parquet upload failed" - data needs to be re-exported
// - Catalog failures: Log with "catalog registration failed" - file exists, needs manual registration
func (w *IcebergWriter) Write(ctx context.Context, opts WriteOptions) error {
	tableName := w.tableNames.GetTableName(opts.SignalType)

	// Step 1: Generate Iceberg-compatible path and compute recovery info upfront
	pathOpts := iceberg.PathOptions{
		TableName: tableName,
		Timestamp: opts.Timestamp,
	}
	path := w.pathGenerator.GeneratePath(pathOpts)
	fileURI := w.fileIO.GetURI(path)
	partitionValues := w.pathGenerator.ExtractPartitionValues(pathOpts)

	// Step 2: ALWAYS write to storage first (data durability is the priority)
	if err := w.fileIO.Write(ctx, path, opts.Data, iceberg.DefaultWriteOptions()); err != nil {
		// Log error with full context for recovery (data needs to be re-exported from source)
		w.logger.Error("parquet upload failed",
			zap.String("failure_type", "upload"),
			zap.String("signal_type", opts.SignalType),
			zap.String("namespace", w.namespace),
			zap.String("table", tableName),
			zap.String("target_path", path),
			zap.String("target_uri", fileURI),
			zap.Time("timestamp", opts.Timestamp),
			zap.Int64("record_count", opts.RecordCount),
			zap.Int("data_size_bytes", len(opts.Data)),
			zap.Error(err))
		return fmt.Errorf("failed to write to storage: %w", err)
	}

	w.logger.Debug("wrote parquet file to storage",
		zap.String("path", path),
		zap.Int("bytes", len(opts.Data)),
		zap.Int64("records", opts.RecordCount))

	// Step 3: Ensure table exists (best-effort, only if catalog is enabled)
	if w.catalog.GetCatalogType() != "none" {
		if err := w.ensureTableExists(ctx, tableName, opts); err != nil {
			// Log error with recovery info - file is stored, but table creation failed
			w.logger.Error("table creation failed (data is safely stored)",
				zap.String("failure_type", "table_creation"),
				zap.String("signal_type", opts.SignalType),
				zap.String("namespace", w.namespace),
				zap.String("table", tableName),
				zap.String("file_uri", fileURI),
				zap.String("file_path", path),
				zap.Int64("file_size_bytes", int64(len(opts.Data))),
				zap.Int64("record_count", opts.RecordCount),
				zap.Any("partition_values", partitionValues),
				zap.Error(err))
			// Don't return error - data is already safely stored
		} else {
			// Step 4: Register file with catalog (best-effort)
			if err := w.registerWithCatalog(ctx, tableName, path, opts); err != nil {
				// Log error with full recovery info for manual re-registration
				w.logger.Error("catalog registration failed (data is safely stored)",
					zap.String("failure_type", "catalog_registration"),
					zap.String("signal_type", opts.SignalType),
					zap.String("namespace", w.namespace),
					zap.String("table", tableName),
					zap.String("file_uri", fileURI),
					zap.String("file_path", path),
					zap.Int64("file_size_bytes", int64(len(opts.Data))),
					zap.Int64("record_count", opts.RecordCount),
					zap.Any("partition_values", partitionValues),
					zap.String("recovery_hint", "Use PyIceberg table.add_files([file_uri]) or Spark CALL catalog.system.add_files()"),
					zap.Error(err))
				// Don't return error - data is already safely stored
			} else {
				w.logger.Debug("registered file with catalog",
					zap.String("table", tableName),
					zap.Int64("records", opts.RecordCount))
			}
		}
	}

	return nil
}

// ensureTableExists ensures the table exists in the catalog.
func (w *IcebergWriter) ensureTableExists(ctx context.Context, tableName string, opts WriteOptions) error {
	// Check if already initialized
	if w.tablesInitialized[tableName] {
		return nil
	}

	// Ensure namespace exists
	if err := w.catalog.EnsureNamespace(ctx, w.namespace); err != nil {
		return fmt.Errorf("failed to ensure namespace: %w", err)
	}

	// Create partition spec based on signal type and granularity
	var partitionSpec iceberg.PartitionSpec
	switch opts.SignalType {
	case "traces":
		partitionSpec = iceberg.OTELPartitionSpec(iarrow.FieldTraceStartTimeUnixNano, w.granularity)
	case "logs":
		partitionSpec = iceberg.OTELPartitionSpec(iarrow.FieldLogTimeUnixNano, w.granularity)
	case "metrics":
		partitionSpec = iceberg.OTELPartitionSpec(iarrow.FieldMetricTimeUnixNano, w.granularity)
	default:
		partitionSpec = iceberg.PartitionSpec{} // Unpartitioned
	}

	// Ensure table exists
	if err := w.catalog.EnsureTable(ctx, w.namespace, tableName, opts.Schema, partitionSpec); err != nil {
		return fmt.Errorf("failed to ensure table: %w", err)
	}

	w.tablesInitialized[tableName] = true
	return nil
}

// registerWithCatalog registers a data file with the catalog.
func (w *IcebergWriter) registerWithCatalog(ctx context.Context, tableName, path string, opts WriteOptions) error {
	fileURI := w.fileIO.GetURI(path)

	appendOpts := []iceberg.AppendOptions{{
		Namespace:     w.namespace,
		Table:         tableName,
		FilePath:      fileURI,
		FileSizeBytes: int64(len(opts.Data)),
		RecordCount:   opts.RecordCount,
		PartitionValues: w.pathGenerator.ExtractPartitionValues(iceberg.PathOptions{
			TableName: tableName,
			Timestamp: opts.Timestamp,
		}),
	}}

	return w.catalog.AppendDataFiles(ctx, appendOpts)
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
