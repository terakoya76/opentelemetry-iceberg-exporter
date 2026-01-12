package recovery

import (
	"time"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

// DataFile represents a parquet file in storage.
// This unified type is used throughout the scanning and recovery lifecycle.
type DataFile struct {
	// Path is the relative path to the file in storage.
	Path string

	// URI is the full URI to the file (e.g., s3://bucket/path).
	URI string

	// Size is the file size in bytes.
	Size int64

	// LastModified is the time the file was last modified.
	// Used during scanning for time-based filtering.
	LastModified time.Time

	// RecordCount is the number of records in the parquet file.
	// May be 0 if metadata couldn't be read.
	RecordCount int64

	// TableName is the Iceberg table name this file belongs to.
	TableName string

	// PartitionValues are the partition values extracted from the file path.
	PartitionValues iceberg.PartitionValues
}

// RecoveryResult contains the results of a recovery operation.
type RecoveryResult struct {
	// TotalFiles is the total number of orphaned files found.
	TotalFiles int

	// SuccessCount is the number of files successfully registered.
	SuccessCount int

	// FailureCount is the number of files that failed to register.
	FailureCount int

	// SkippedCount is the number of files skipped (e.g., already registered).
	SkippedCount int

	// Errors contains details about files that failed to register.
	Errors []FileError

	// RegisteredFiles contains details about successfully registered files.
	RegisteredFiles []DataFile
}

// FileError represents an error that occurred while processing a file.
type FileError struct {
	// File is the file that caused the error.
	File DataFile

	// Error is the error message.
	Error string
}

// ScanOptions contains options for scanning operations.
type ScanOptions struct {
	// Tables is the list of table names to scan. If empty, all tables are scanned.
	Tables []string

	// After filters to files with partition time >= After (inclusive).
	// If nil, no lower bound is applied.
	After *time.Time

	// Before filters to files with partition time < Before (exclusive).
	// If nil, no upper bound is applied.
	Before *time.Time
}

// RecoveryOptions contains options for the recovery operation.
type RecoveryOptions struct {
	// Namespace is the Iceberg namespace to recover files for.
	Namespace string

	// DryRun if true, only reports what would be recovered without making changes.
	DryRun bool

	// ScanOptions embeds scanning options including table filters and time filters.
	ScanOptions
}
