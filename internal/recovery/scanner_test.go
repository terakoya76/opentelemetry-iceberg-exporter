package recovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap/zaptest"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

func TestScanner_ScanPrefix(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create FileIO
	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}

	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Write some test files
	testFiles := []struct {
		path string
		data []byte
	}{
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=14/00000-0-test1234.parquet", constants.ExporterName),
			data: []byte("test parquet data 1"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=15/00000-0-test5678.parquet", constants.ExporterName),
			data: []byte("test parquet data 2"),
		},
		{
			path: fmt.Sprintf("%s/otel_logs/data/year=2024/month=06/day=15/00000-0-logs1234.parquet", constants.ExporterName),
			data: []byte("test logs data"),
		},
	}

	for _, tf := range testFiles {
		err := fileIO.Write(ctx, tf.path, tf.data, iceberg.DefaultWriteOptions())
		require.NoError(t, err)
	}

	// Create scanner
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	scanner := NewScanner(fileIO, vlogger)

	t.Run("Scan all", func(t *testing.T) {
		files, err := scanner.ScanAll(ctx)
		require.NoError(t, err)

		assert.Len(t, files, 3)
		// Verify files are from expected tables
		tableCount := make(map[string]int)
		for _, f := range files {
			tableCount[f.TableName]++
		}
		assert.Equal(t, 2, tableCount["otel_traces"])
		assert.Equal(t, 1, tableCount["otel_logs"])
	})

	t.Run("Scan specific table", func(t *testing.T) {
		files, err := scanner.ScanTable(ctx, "otel_traces")
		require.NoError(t, err)

		assert.Len(t, files, 2)
		for _, f := range files {
			assert.Equal(t, "otel_traces", f.TableName)
		}
	})

	t.Run("Scan non-existent table", func(t *testing.T) {
		files, err := scanner.ScanTable(ctx, "non_existent")
		require.NoError(t, err)

		assert.Len(t, files, 0)
	})
}

func TestScanner_ScanAll(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create FileIO
	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}

	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Write test files
	testFiles := []struct {
		path string
		data []byte
	}{
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=10/00000-0-test1.parquet", constants.ExporterName),
			data: []byte("test data 1"),
		},
		{
			path: fmt.Sprintf("%s/otel_logs/data/year=2024/month=06/day=15/hour=14/00000-0-test2.parquet", constants.ExporterName),
			data: []byte("test data 2"),
		},
	}

	for _, tf := range testFiles {
		err := fileIO.Write(ctx, tf.path, tf.data, iceberg.DefaultWriteOptions())
		require.NoError(t, err)
	}

	// Create scanner
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	scanner := NewScanner(fileIO, vlogger)

	t.Run("ScanAll with After filter", func(t *testing.T) {
		after := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		files, err := scanner.ScanAll(ctx, ScanOptions{After: &after})
		require.NoError(t, err)
		// Should include only hour=14 file
		assert.Len(t, files, 1)
		assert.Equal(t, "otel_logs", files[0].TableName)
	})
}

func TestScanner_ScanTable(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create FileIO
	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}

	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Write test files with different partition times
	testFiles := []struct {
		path string
		data []byte
	}{
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=10/00000-0-test1.parquet", constants.ExporterName),
			data: []byte("test data 1"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=14/00000-0-test2.parquet", constants.ExporterName),
			data: []byte("test data 2"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=18/00000-0-test3.parquet", constants.ExporterName),
			data: []byte("test data 3"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=16/hour=10/00000-0-test4.parquet", constants.ExporterName),
			data: []byte("test data 4"),
		},
	}

	for _, tf := range testFiles {
		err := fileIO.Write(ctx, tf.path, tf.data, iceberg.DefaultWriteOptions())
		require.NoError(t, err)
	}

	// Create scanner
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	scanner := NewScanner(fileIO, vlogger)

	t.Run("No time filter - all files", func(t *testing.T) {
		files, err := scanner.ScanTable(ctx, "otel_traces")
		require.NoError(t, err)
		assert.Len(t, files, 4)
	})

	t.Run("After filter only", func(t *testing.T) {
		after := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
		files, err := scanner.ScanTable(ctx, "otel_traces", ScanOptions{After: &after})
		require.NoError(t, err)
		// Should include: hour=14, hour=18, day=16/hour=10
		assert.Len(t, files, 3)
	})

	t.Run("Before filter only", func(t *testing.T) {
		before := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
		files, err := scanner.ScanTable(ctx, "otel_traces", ScanOptions{Before: &before})
		require.NoError(t, err)
		// Should include: hour=10 only (before is exclusive)
		assert.Len(t, files, 1)
	})

	t.Run("After and Before filter - range", func(t *testing.T) {
		after := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		before := time.Date(2024, 6, 15, 16, 0, 0, 0, time.UTC)
		files, err := scanner.ScanTable(ctx, "otel_traces", ScanOptions{After: &after, Before: &before})
		require.NoError(t, err)
		// Should include: hour=14 only
		assert.Len(t, files, 1)
	})

	t.Run("After filter - day boundary", func(t *testing.T) {
		after := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
		files, err := scanner.ScanTable(ctx, "otel_traces", ScanOptions{After: &after})
		require.NoError(t, err)
		// Should include: day=16/hour=10 only
		assert.Len(t, files, 1)
	})

	t.Run("Empty range - no results", func(t *testing.T) {
		after := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
		before := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
		files, err := scanner.ScanTable(ctx, "otel_traces", ScanOptions{After: &after, Before: &before})
		require.NoError(t, err)
		// Should include nothing (before is exclusive, after is inclusive)
		assert.Len(t, files, 0)
	})
}

func TestParseFilePath(t *testing.T) {
	tests := []struct {
		name               string
		filePath           string
		expectedTableName  string
		expectedPartitions iceberg.PartitionValues
	}{
		{
			name:              "Full hourly path",
			filePath:          fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=14/00000-0-abc12345.parquet", constants.ExporterName),
			expectedTableName: "otel_traces",
			expectedPartitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "14",
			},
		},
		{
			name:              "Daily path",
			filePath:          fmt.Sprintf("%s/otel_logs/data/year=2024/month=06/day=15/00000-0-abc12345.parquet", constants.ExporterName),
			expectedTableName: "otel_logs",
			expectedPartitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
			},
		},
		{
			name:              "Monthly path",
			filePath:          fmt.Sprintf("%s/otel_metrics/data/year=2024/month=06/00000-0-abc12345.parquet", constants.ExporterName),
			expectedTableName: "otel_metrics",
			expectedPartitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
			},
		},
		{
			name:              "With extra partition (ignored)",
			filePath:          fmt.Sprintf("%s/otel_traces/data/service_name=my-service/year=2024/month=06/day=15/hour=14/00000-0-abc12345.parquet", constants.ExporterName),
			expectedTableName: "otel_traces",
			expectedPartitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "14",
			},
		},
		{
			name:               "Invalid - no exporter prefix",
			filePath:           "other/otel_traces/data/year=2024/file.parquet",
			expectedTableName:  "",
			expectedPartitions: iceberg.PartitionValues{},
		},
		{
			name:               "Invalid - too short",
			filePath:           fmt.Sprintf("%s/table", constants.ExporterName),
			expectedTableName:  "",
			expectedPartitions: iceberg.PartitionValues{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tableName, partitionValues := parseFilePath(tc.filePath)
			assert.Equal(t, tc.expectedTableName, tableName)
			assert.Equal(t, tc.expectedPartitions, partitionValues)
		})
	}
}

func TestPartitionToTime(t *testing.T) {
	tests := []struct {
		name         string
		partitions   iceberg.PartitionValues
		expectedTime time.Time
		expectOk     bool
	}{
		{
			name: "Full partitions (year/month/day/hour)",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "14",
			},
			expectedTime: time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name: "Year/month/day only",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
			},
			expectedTime: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name: "Year/month only",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
			},
			expectedTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name: "Year only",
			partitions: iceberg.PartitionValues{
				Year: "2024",
			},
			expectedTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name:       "Empty partitions",
			partitions: iceberg.PartitionValues{},
			expectOk:   false,
		},
		{
			name: "No year partition",
			partitions: iceberg.PartitionValues{
				Month: "06",
				Day:   "15",
			},
			expectOk: false,
		},
		{
			name: "Invalid year",
			partitions: iceberg.PartitionValues{
				Year: "invalid",
			},
			expectOk: false,
		},
		{
			name: "Invalid month (out of range)",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "13",
			},
			// Invalid month is ignored, defaults to January
			expectedTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name: "Invalid day (out of range)",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "32",
			},
			// Invalid day is ignored, defaults to 1
			expectedTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
		{
			name: "Invalid hour (out of range)",
			partitions: iceberg.PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "25",
			},
			// Invalid hour is ignored, defaults to 0
			expectedTime: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			expectOk:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := partitionToTime(tc.partitions)
			assert.Equal(t, tc.expectOk, ok)
			if tc.expectOk {
				assert.Equal(t, tc.expectedTime, result)
			}
		})
	}
}

func TestMatchesTimeFilter(t *testing.T) {
	refTime := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
	beforeRef := time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC)
	afterRef := time.Date(2024, 6, 15, 15, 0, 0, 0, time.UTC)

	fileWithPartitions := DataFile{
		Path: "test.parquet",
		PartitionValues: iceberg.PartitionValues{
			Year:  "2024",
			Month: "06",
			Day:   "15",
			Hour:  "14",
		},
		LastModified: refTime,
	}

	fileWithoutPartitions := DataFile{
		Path:            "test.parquet",
		PartitionValues: iceberg.PartitionValues{},
		LastModified:    refTime,
	}

	tests := []struct {
		name     string
		file     DataFile
		opts     ScanOptions
		expected bool
	}{
		{
			name:     "No filters - include all",
			file:     fileWithPartitions,
			opts:     ScanOptions{},
			expected: true,
		},
		{
			name:     "After filter - file equals after (inclusive)",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &refTime},
			expected: true,
		},
		{
			name:     "After filter - file after the filter",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &beforeRef},
			expected: true,
		},
		{
			name:     "After filter - file before the filter",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &afterRef},
			expected: false,
		},
		{
			name:     "Before filter - file before the filter",
			file:     fileWithPartitions,
			opts:     ScanOptions{Before: &afterRef},
			expected: true,
		},
		{
			name:     "Before filter - file equals before (exclusive)",
			file:     fileWithPartitions,
			opts:     ScanOptions{Before: &refTime},
			expected: false,
		},
		{
			name:     "Before filter - file after the filter",
			file:     fileWithPartitions,
			opts:     ScanOptions{Before: &beforeRef},
			expected: false,
		},
		{
			name:     "Both filters - file in range",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &beforeRef, Before: &afterRef},
			expected: true,
		},
		{
			name:     "Both filters - file at start of range (inclusive)",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &refTime, Before: &afterRef},
			expected: true,
		},
		{
			name:     "Both filters - file at end of range (exclusive)",
			file:     fileWithPartitions,
			opts:     ScanOptions{After: &beforeRef, Before: &refTime},
			expected: false,
		},
		{
			name:     "No partitions - include (conservative)",
			file:     fileWithoutPartitions,
			opts:     ScanOptions{After: &refTime},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchesTimeFilter(tc.file, tc.opts)
			assert.Equal(t, tc.expected, result)
		})
	}
}
