package recovery

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// Scanner scans storage for parquet files.
type Scanner struct {
	fileIO iceberg.FileIO
	logger *logger.VerboseLogger
}

// NewScanner creates a new Scanner.
func NewScanner(fileIO iceberg.FileIO, vlogger *logger.VerboseLogger) *Scanner {
	return &Scanner{
		fileIO: fileIO,
		logger: vlogger,
	}
}

// ScanAll scans for all parquet files under the exporter prefix.
// Optional ScanOptions can be provided to filter by time.
func (s *Scanner) ScanAll(ctx context.Context, opts ...ScanOptions) ([]DataFile, error) {
	return s.ScanPrefix(ctx, constants.ExporterName, opts...)
}

// ScanTable scans for parquet files for a specific table.
// Optional ScanOptions can be provided to filter by time.
func (s *Scanner) ScanTable(ctx context.Context, tableName string, opts ...ScanOptions) ([]DataFile, error) {
	prefix := fmt.Sprintf("%s/%s/data", constants.ExporterName, tableName)
	return s.ScanPrefix(ctx, prefix, opts...)
}

// ScanPrefix scans for parquet files under the specified prefix.
// Optional ScanOptions can be provided to filter by time.
func (s *Scanner) ScanPrefix(ctx context.Context, prefix string, opts ...ScanOptions) ([]DataFile, error) {
	s.logger.Debug("scanning storage for parquet files",
		zap.String("prefix", prefix))

	// Extract options (use first if provided, otherwise use default)
	var scanOpts ScanOptions
	if len(opts) > 0 {
		scanOpts = opts[0]
	}

	files, err := s.fileIO.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	result := make([]DataFile, 0, len(files))

	for _, f := range files {
		// Only process .parquet files
		if !strings.HasSuffix(f.Path, ".parquet") {
			continue
		}

		df := DataFile{
			Path:         f.Path,
			URI:          s.fileIO.GetURI(f.Path),
			Size:         f.Size,
			LastModified: f.LastModified,
		}

		// Parse table name and partition values from path
		tableName, partitionValues := parseFilePath(f.Path)
		df.TableName = tableName
		df.PartitionValues = partitionValues

		// Apply time filter
		if !matchesTimeFilter(df, scanOpts) {
			continue
		}

		result = append(result, df)
	}

	s.logger.Info("storage scan complete",
		zap.Int("total_files", len(result)),
		zap.Int("tables_found", countUniqueTables(result)))

	return result, nil
}

// countUniqueTables counts the number of unique table names in the file list.
func countUniqueTables(files []DataFile) int {
	tables := make(map[string]struct{})
	for _, f := range files {
		if f.TableName != "" {
			tables[f.TableName] = struct{}{}
		}
	}
	return len(tables)
}

// parseFilePath parses an exporter file path to extract table name and partition values.
// Expected format: {ExporterName}/{table}/data/{partition_path}/{filename}.parquet
// Partition path format: year=YYYY/month=MM/day=DD/hour=HH
func parseFilePath(filePath string) (tableName string, partitionValues iceberg.PartitionValues) {
	// Split path into components
	parts := strings.Split(filePath, "/")

	// Expected: {ExporterName}/{table}/data/...
	if len(parts) < 4 {
		return "", partitionValues
	}

	// Check for exporter prefix
	if parts[0] != constants.ExporterName {
		return "", partitionValues
	}

	tableName = parts[1]

	// Check for data directory
	if parts[2] != "data" {
		return tableName, partitionValues
	}

	// Parse partition values from remaining path components (excluding filename)
	// Format: key=value
	partitionRegex := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)=(.+)$`)

	for i := 3; i < len(parts)-1; i++ {
		matches := partitionRegex.FindStringSubmatch(parts[i])
		if len(matches) == 3 {
			key := matches[1]
			value := matches[2]
			switch key {
			case "year":
				partitionValues.Year = value
			case "month":
				partitionValues.Month = value
			case "day":
				partitionValues.Day = value
			case "hour":
				partitionValues.Hour = value
			}
		}
	}

	return tableName, partitionValues
}

// partitionToTime converts partition values to a time.Time.
// Expected partition fields: Year, Month, Day, Hour.
// Returns the time and true if at least Year partition exists.
// Returns zero time and false if no valid Year partition exists.
func partitionToTime(partitions iceberg.PartitionValues) (time.Time, bool) {
	if partitions.Year == "" {
		return time.Time{}, false
	}

	year, err := strconv.Atoi(partitions.Year)
	if err != nil {
		return time.Time{}, false
	}

	// Default to January 1st, 00:00:00 UTC
	month := 1
	day := 1
	hour := 0

	if partitions.Month != "" {
		if m, err := strconv.Atoi(partitions.Month); err == nil && m >= 1 && m <= 12 {
			month = m
		}
	}

	if partitions.Day != "" {
		if d, err := strconv.Atoi(partitions.Day); err == nil && d >= 1 && d <= 31 {
			day = d
		}
	}

	if partitions.Hour != "" {
		if h, err := strconv.Atoi(partitions.Hour); err == nil && h >= 0 && h <= 23 {
			hour = h
		}
	}

	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), true
}

// matchesTimeFilter checks if a file matches the time filter criteria.
// Filters by partition time (year/month/day/hour).
// Returns true if the file should be included in results.
func matchesTimeFilter(file DataFile, opts ScanOptions) bool {
	// If no time filters specified, include all files
	if opts.After == nil && opts.Before == nil {
		return true
	}

	// Get partition time
	fileTime, hasTime := partitionToTime(file.PartitionValues)
	if !hasTime {
		// If partition time cannot be determined, include the file
		// This is conservative behavior - don't filter out files we can't classify
		return true
	}

	// Apply time filters
	// After: include if fileTime >= After (inclusive)
	if opts.After != nil && fileTime.Before(*opts.After) {
		return false
	}

	// Before: include if fileTime < Before (exclusive)
	if opts.Before != nil && !fileTime.Before(*opts.Before) {
		return false
	}

	return true
}
