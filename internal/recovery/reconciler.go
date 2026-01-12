package recovery

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// Reconciler compares storage files against the catalog and recovers orphaned files.
type Reconciler struct {
	scanner   *Scanner
	catalog   iceberg.Catalog
	fileIO    iceberg.FileIO
	namespace string
	logger    *logger.VerboseLogger
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	fileIO iceberg.FileIO,
	catalog iceberg.Catalog,
	namespace string,
	vlogger *logger.VerboseLogger,
) *Reconciler {
	return &Reconciler{
		scanner:   NewScanner(fileIO, vlogger),
		catalog:   catalog,
		fileIO:    fileIO,
		namespace: namespace,
		logger:    vlogger,
	}
}

// ListFiles scans storage and returns all parquet files found.
func (r *Reconciler) ListFiles(ctx context.Context, opts RecoveryOptions) ([]DataFile, error) {
	var files []DataFile
	var err error

	scanOpts := opts.ScanOptions

	// Scan storage for files
	if len(opts.Tables) > 0 {
		// Scan specific tables
		for _, table := range opts.Tables {
			tableFiles, scanErr := r.scanner.ScanTable(ctx, table, scanOpts)
			if scanErr != nil {
				return nil, fmt.Errorf("failed to scan table %s: %w", table, scanErr)
			}
			files = append(files, tableFiles...)
		}
	} else {
		// Scan all tables
		files, err = r.scanner.ScanAll(ctx, scanOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to scan storage: %w", err)
		}
	}

	return files, nil
}

// GetRegisteredFiles returns a set of file URIs that are already registered with the catalog.
// This is used to filter out already-registered files before attempting recovery.
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

		r.logger.Debug("found registered files for table",
			zap.String("namespace", namespace),
			zap.String("table", tableName),
			zap.Int("count", len(files)))
	}

	return registeredFiles, nil
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

// RecoverFiles attempts to register orphaned files with the catalog.
// Files should be pre-filtered to exclude already-registered files.
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

	// Check if catalog is enabled
	if r.catalog.GetCatalogType() == "none" {
		return nil, fmt.Errorf("catalog is not configured (type=none)")
	}

	for _, file := range files {
		r.logger.Debug("registering file",
			zap.String("file", file.Path),
			zap.String("table", file.TableName),
			zap.String("uri", file.URI))

		appendOpts := iceberg.AppendOptions{
			Namespace:       r.namespace,
			Table:           file.TableName,
			FilePath:        file.URI,
			FileSizeBytes:   file.Size,
			RecordCount:     file.RecordCount,
			PartitionValues: file.PartitionValues,
		}

		if err := r.catalog.AppendDataFile(ctx, appendOpts); err != nil {
			r.logger.Error("failed to register file",
				zap.String("file", file.Path),
				zap.Error(err))
			result.FailureCount++
			result.Errors = append(result.Errors, FileError{
				File:  file,
				Error: err.Error(),
			})
		} else {
			r.logger.Info("successfully registered file",
				zap.String("file", file.Path),
				zap.String("table", file.TableName))
			result.SuccessCount++
			result.RegisteredFiles = append(result.RegisteredFiles, file)
		}
	}

	return result, nil
}

// Recover performs the full recovery process: scan, find orphans, and register.
// Queries the catalog manifest to identify already-registered files,
// then only registers truly orphaned files.
func (r *Reconciler) Recover(ctx context.Context, opts RecoveryOptions) (*RecoveryResult, error) {
	r.logger.Info("starting recovery process",
		zap.String("namespace", r.namespace),
		zap.Strings("tables", opts.Tables),
		zap.Bool("dry_run", opts.DryRun))

	// Find files in storage
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

	r.logger.Info("identified orphaned files",
		zap.Int("total_files", len(files)),
		zap.Int("already_registered", alreadyRegisteredCount),
		zap.Int("orphaned", len(orphanedFiles)))

	if len(orphanedFiles) == 0 {
		return &RecoveryResult{
			TotalFiles:   len(files),
			SkippedCount: alreadyRegisteredCount,
		}, nil
	}

	// Recover only the orphaned files
	result, err := r.RecoverFiles(ctx, orphanedFiles, opts)
	if err != nil {
		return nil, err
	}

	result.TotalFiles = len(files)
	result.SkippedCount += alreadyRegisteredCount

	return result, nil
}
