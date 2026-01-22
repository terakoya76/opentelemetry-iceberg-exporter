package recovery

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

const (
	// maxBatchSize is the maximum number of files to batch in a single AppendDataFiles call.
	// Batching improves performance by reducing the number of catalog transactions.
	maxBatchSize = 1000

	// batchSizeDivisor is used to reduce batch size when a batch operation fails.
	// For example, with divisor 10: 1000 -> 100 -> 10 -> 1
	batchSizeDivisor = 10
)

// Reconciler compares storage files against the catalog and recovers orphaned files.
type Reconciler struct {
	scanner       *Scanner
	catalog       iceberg.Catalog
	fileIO        iceberg.FileIO
	namespace     string
	logger        *logger.VerboseLogger
	repartitioner *Repartitioner
}

// ReconcilerConfig holds configuration for the Reconciler.
type ReconcilerConfig struct {
	// Timezone for partition calculation (default: "UTC")
	Timezone string
	// Compression for re-partitioned files (default: "snappy")
	Compression string
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	fileIO iceberg.FileIO,
	catalog iceberg.Catalog,
	namespace string,
	vlogger *logger.VerboseLogger,
	cfg ...ReconcilerConfig,
) (*Reconciler, error) {
	// Apply config with defaults
	config := ReconcilerConfig{
		Timezone:    "UTC",
		Compression: "snappy",
	}
	if len(cfg) > 0 {
		if cfg[0].Timezone != "" {
			config.Timezone = cfg[0].Timezone
		}
		if cfg[0].Compression != "" {
			config.Compression = cfg[0].Compression
		}
	}

	repartitioner, err := NewRepartitioner(fileIO, config.Timezone, config.Compression, vlogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create repartitioner: %w", err)
	}

	return &Reconciler{
		scanner:       NewScanner(fileIO, vlogger),
		catalog:       catalog,
		fileIO:        fileIO,
		namespace:     namespace,
		logger:        vlogger,
		repartitioner: repartitioner,
	}, nil
}

// Recover performs the full recovery process: scan, find orphans, and register.
// Queries the catalog manifest to identify already-registered files,
// then only registers truly orphaned files.
func (r *Reconciler) Recover(ctx context.Context, opts RecoveryOptions) (*RecoveryResult, error) {
	r.logger.Info("starting recovery process",
		zap.String("namespace", r.namespace),
		zap.Strings("tables", opts.Tables),
		zap.Bool("dry_run", opts.DryRun))

	files, err := r.ListFiles(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	r.logger.Info("found files in storage", zap.Int("count", len(files)))

	if len(files) == 0 {
		return &RecoveryResult{TotalFiles: 0}, nil
	}

	// Get already-registered files from the catalog manifest
	tableNames := extractTableNames(files)
	registeredFiles, err := r.GetRegisteredFiles(ctx, r.namespace, tableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to get registered files: %w", err)
	}

	r.logger.Info("retrieved registered files from catalog",
		zap.Int("registered_count", len(registeredFiles)))

	// Filter out already-registered files
	var orphanedFiles []DataFile
	var alreadyRegisteredCount int
	for _, file := range files {
		if _, exists := registeredFiles[file.URI]; exists {
			alreadyRegisteredCount++
		} else {
			orphanedFiles = append(orphanedFiles, file)
		}
	}

	// Files in catalog but not found in storage (e.g., removed by compaction/expire_snapshots)
	catalogOnlyCount := len(registeredFiles) - alreadyRegisteredCount

	r.logger.Info("identified orphaned files",
		zap.Int("total_files", len(files)),
		zap.Int("already_registered", alreadyRegisteredCount),
		zap.Int("orphaned", len(orphanedFiles)),
		zap.Int("catalog_only", catalogOnlyCount))

	if len(orphanedFiles) == 0 {
		return &RecoveryResult{
			TotalFiles:   len(files),
			SkippedCount: alreadyRegisteredCount,
		}, nil
	}

	result, err := r.RecoverFiles(ctx, orphanedFiles, opts)
	if err != nil {
		return nil, err
	}

	result.TotalFiles = len(files)
	result.SkippedCount += alreadyRegisteredCount

	return result, nil
}

// ListFiles scans storage and returns all parquet files found.
func (r *Reconciler) ListFiles(ctx context.Context, opts RecoveryOptions) ([]DataFile, error) {
	var files []DataFile
	var err error

	scanOpts := opts.ScanOptions

	// Scan storage for files
	if len(opts.Tables) > 0 {
		for _, table := range opts.Tables {
			tableFiles, scanErr := r.scanner.ScanTable(ctx, table, scanOpts)
			if scanErr != nil {
				return nil, fmt.Errorf("failed to scan table %s: %w", table, scanErr)
			}
			files = append(files, tableFiles...)
		}
	} else {
		files, err = r.scanner.ScanAll(ctx, scanOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to scan storage: %w", err)
		}
	}

	return files, nil
}

// extractTableNames extracts unique table names from a list of data files.
func extractTableNames(files []DataFile) []string {
	tableSet := make(map[string]struct{})
	for _, f := range files {
		if f.TableName != "" {
			tableSet[f.TableName] = struct{}{}
		}
	}

	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	return tables
}

// GetRegisteredFiles returns a set of file URIs that are already registered with the catalog.
func (r *Reconciler) GetRegisteredFiles(ctx context.Context, namespace string, tableNames []string) (map[string]struct{}, error) {
	registeredFiles := make(map[string]struct{})

	// If catalog is not configured, return empty set
	if r.catalog.GetCatalogType() == "none" {
		r.logger.Debug("catalog is disabled, returning empty registered files set")
		return registeredFiles, nil
	}

	for _, tableName := range tableNames {
		r.logger.Debug("listing registered files for table",
			zap.String("namespace", namespace),
			zap.String("table", tableName))

		files, err := r.catalog.ListDataFiles(ctx, namespace, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to list data files for table %s.%s: %w", namespace, tableName, err)
		}

		for _, filePath := range files {
			registeredFiles[filePath] = struct{}{}
		}

		r.logger.Info("found registered files for table",
			zap.String("namespace", namespace),
			zap.String("table", tableName),
			zap.Int("count", len(files)))
	}

	return registeredFiles, nil
}

// RecoverFiles attempts to register orphaned files with the catalog.
// Files are batched by table (up to maxBatchSize files per batch) to improve performance.
func (r *Reconciler) RecoverFiles(ctx context.Context, files []DataFile, opts RecoveryOptions) (*RecoveryResult, error) {
	result := &RecoveryResult{
		TotalFiles:      len(files),
		RegisteredFiles: make([]DataFile, 0),
		Errors:          make([]FileError, 0),
	}

	if opts.DryRun {
		r.logger.Info("dry-run mode: would attempt to register files",
			zap.Int("file_count", len(files)))
		result.SkippedCount = len(files)
		return result, nil
	}

	if r.catalog.GetCatalogType() == "none" {
		return nil, fmt.Errorf("catalog is not configured (type=none)")
	}

	filesByTable := groupFilesByTable(files)
	for tableName, tableFiles := range filesByTable {
		r.logger.Debug("processing files for table",
			zap.String("table", tableName),
			zap.Int("file_count", len(tableFiles)))

		// Process files in batches
		for i := 0; i < len(tableFiles); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(tableFiles) {
				end = len(tableFiles)
			}
			batch := tableFiles[i:end]

			r.processBatch(ctx, batch, result)
		}
	}

	return result, nil
}

// groupFilesByTable groups files by their TableName.
func groupFilesByTable(files []DataFile) map[string][]DataFile {
	grouped := make(map[string][]DataFile)
	for _, f := range files {
		grouped[f.TableName] = append(grouped[f.TableName], f)
	}
	return grouped
}

// processBatch attempts to register a batch of files (all from the same table) with the catalog.
// On failure, it gradually reduces the batch size before falling back to individual file processing.
func (r *Reconciler) processBatch(ctx context.Context, files []DataFile, result *RecoveryResult) {
	r.processBatchWithSize(ctx, files, maxBatchSize, result)
}

// processBatchWithSize attempts to register files in batches of the specified size.
// On failure, it reduces the batch size by batchSizeDivisor (e.g., 1000 -> 100 -> 10 -> 1).
// When batch size reaches 1, it falls back to individual file processing.
func (r *Reconciler) processBatchWithSize(ctx context.Context, files []DataFile, batchSize int, result *RecoveryResult) {
	if len(files) == 0 {
		return
	}

	// If batch size is 1 or less, process files individually
	if batchSize <= 1 {
		for _, file := range files {
			r.processFileIndividually(ctx, file, result)
		}
		return
	}

	// Process files in batches of the current size
	for i := 0; i < len(files); i += batchSize {
		end := i + batchSize
		if end > len(files) {
			end = len(files)
		}
		batch := files[i:end]

		appendOpts := make([]iceberg.AppendOptions, 0, len(batch))
		for _, file := range batch {
			appendOpts = append(appendOpts, iceberg.AppendOptions{
				Namespace:       r.namespace,
				Table:           file.TableName,
				FilePath:        file.URI,
				FileSizeBytes:   file.Size,
				RecordCount:     file.RecordCount,
				PartitionValues: file.PartitionValues,
			})
		}

		err := r.catalog.AppendDataFiles(ctx, appendOpts)
		if err == nil {
			// All files in the batch succeeded
			result.SuccessCount += len(batch)
			result.RegisteredFiles = append(result.RegisteredFiles, batch...)
			r.logger.Debug("batch registration succeeded",
				zap.String("table", batch[0].TableName),
				zap.Int("file_count", len(batch)),
				zap.Int("batch_size", batchSize))
			continue
		}

		// Batch failed - reduce batch size and retry
		// Use the smaller of batchSize and actual file count as the base for reduction
		// This avoids unnecessary retries when file count is small (e.g., 3 files: 3 -> 1, not 1000 -> 100 -> 10 -> 1)
		effectiveSize := batchSize
		if len(batch) < effectiveSize {
			effectiveSize = len(batch)
		}
		nextBatchSize := effectiveSize / batchSizeDivisor
		if nextBatchSize < 1 {
			nextBatchSize = 1
		}

		r.logger.Debug("batch registration failed, reducing batch size",
			zap.String("table", batch[0].TableName),
			zap.Int("file_count", len(batch)),
			zap.Int("effective_size", effectiveSize),
			zap.Int("next_batch_size", nextBatchSize),
			zap.Error(err))

		// Recursively process the failed batch with smaller batch size
		r.processBatchWithSize(ctx, batch, nextBatchSize, result)
	}
}

// processFileIndividually attempts to register a single file with the catalog.
func (r *Reconciler) processFileIndividually(ctx context.Context, file DataFile, result *RecoveryResult) {
	appendOpts := []iceberg.AppendOptions{{
		Namespace:       r.namespace,
		Table:           file.TableName,
		FilePath:        file.URI,
		FileSizeBytes:   file.Size,
		RecordCount:     file.RecordCount,
		PartitionValues: file.PartitionValues,
	}}

	err := r.catalog.AppendDataFiles(ctx, appendOpts)
	if err == nil {
		result.SuccessCount++
		result.RegisteredFiles = append(result.RegisteredFiles, file)
		return
	}

	if IsCrossPartitionError(err) {
		r.handleCrossPartitionFile(ctx, file, err, result)
		return
	}

	result.FailureCount++
	result.Errors = append(result.Errors, FileError{
		File:  file,
		Error: err.Error(),
	})
}

// handleCrossPartitionFile handles re-partitioning and registration of files that span multiple partitions.
func (r *Reconciler) handleCrossPartitionFile(ctx context.Context, file DataFile, originalErr error, result *RecoveryResult) {
	r.logger.Warn("file spans multiple partitions, attempting re-partition",
		zap.String("file", file.Path),
		zap.Error(originalErr))

	repartitioned, err := r.repartitioner.Repartition(ctx, file)
	if err != nil {
		result.FailureCount++
		result.Errors = append(result.Errors, FileError{
			File:  file,
			Error: fmt.Sprintf("re-partition failed: %v (original error: %v)", err, originalErr),
		})
		return
	}

	if len(repartitioned) == 0 {
		return
	}

	// Collect all append options for atomic batch registration
	appendOpts := make([]iceberg.AppendOptions, 0, len(repartitioned))
	for _, rf := range repartitioned {
		appendOpts = append(appendOpts, iceberg.AppendOptions{
			Namespace:       r.namespace,
			Table:           rf.DataFile.TableName,
			FilePath:        rf.DataFile.URI,
			FileSizeBytes:   rf.DataFile.Size,
			RecordCount:     rf.DataFile.RecordCount,
			PartitionValues: rf.DataFile.PartitionValues,
		})
	}

	// Register all repartitioned files atomically
	if err = r.catalog.AppendDataFiles(ctx, appendOpts); err != nil {
		// Atomic registration failed - all files failed
		result.FailureCount += len(repartitioned)
		for _, rf := range repartitioned {
			result.Errors = append(result.Errors, FileError{
				File:  rf.DataFile,
				Error: fmt.Sprintf("re-partitioned file registration failed: %v", err),
			})
		}
		r.logger.Error("failed to register repartitioned files",
			zap.String("original_file", file.Path),
			zap.Int("file_count", len(repartitioned)),
			zap.Error(err))
		return
	}

	// All files registered successfully
	result.SuccessCount += len(repartitioned)
	for _, rf := range repartitioned {
		result.RegisteredFiles = append(result.RegisteredFiles, rf.DataFile)
	}
	r.logger.Info("successfully registered all re-partitioned files",
		zap.String("original_file", file.Path),
		zap.Int("file_count", len(repartitioned)))

	// Delete original file since all repartitioned files were registered successfully
	if err := r.fileIO.Delete(ctx, file.Path); err != nil {
		// Log error but don't count as failure since data is safely registered
		r.logger.Error("failed to delete original file after successful repartition",
			zap.String("path", file.Path),
			zap.Error(err))
	} else {
		result.DeletedRepartitionedOriginalFiles = append(result.DeletedRepartitionedOriginalFiles, file.Path)
		r.logger.Info("deleted original file after successful repartition",
			zap.String("path", file.Path))
	}
}
