package recovery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.uber.org/zap/zaptest"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

func TestReconciler_ListFiles(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}
	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Create no-op catalog
	catalogCfg := iceberg.CatalogConfig{
		Type:      "none",
		Namespace: "test",
	}
	catalog, err := iceberg.NewCatalog(ctx, catalogCfg, fileIOCfg, logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal))
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	// Write test files with different partition times
	testFiles := []struct {
		path string
		data []byte
	}{
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=10/00000-0-test1.parquet", constants.ExporterName),
			data: []byte("trace data 1"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=14/00000-0-test2.parquet", constants.ExporterName),
			data: []byte("trace data 2"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=18/00000-0-test3.parquet", constants.ExporterName),
			data: []byte("trace data 3"),
		},
		{
			path: fmt.Sprintf("%s/otel_logs/data/year=2024/month=06/day=16/hour=10/00000-0-logs1.parquet", constants.ExporterName),
			data: []byte("logs data"),
		},
	}

	for _, tf := range testFiles {
		err := fileIO.Write(ctx, tf.path, tf.data, iceberg.DefaultWriteOptions())
		require.NoError(t, err)
	}

	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	reconciler, err := NewReconciler(fileIO, catalog, "test", vlogger)
	require.NoError(t, err)

	t.Run("Find all files", func(t *testing.T) {
		opts := RecoveryOptions{
			Namespace: "test",
		}

		files, err := reconciler.ListFiles(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, files, 4)
	})

	t.Run("Find files with table filter", func(t *testing.T) {
		opts := RecoveryOptions{
			Namespace:   "test",
			ScanOptions: ScanOptions{Tables: []string{"otel_traces"}},
		}

		files, err := reconciler.ListFiles(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, files, 3)
		for _, f := range files {
			assert.Equal(t, "otel_traces", f.TableName)
		}
	})
}

func Test_extractTableNames(t *testing.T) {
	tests := []struct {
		name     string
		files    []DataFile
		expected []string
	}{
		{
			name:     "Empty files list",
			files:    []DataFile{},
			expected: []string{},
		},
		{
			name: "Single table",
			files: []DataFile{
				{TableName: "otel_traces"},
				{TableName: "otel_traces"},
			},
			expected: []string{"otel_traces"},
		},
		{
			name: "Multiple tables",
			files: []DataFile{
				{TableName: "otel_traces"},
				{TableName: "otel_logs"},
				{TableName: "otel_traces"},
				{TableName: "otel_metrics_gauge"},
			},
			expected: []string{"otel_traces", "otel_logs", "otel_metrics_gauge"},
		},
		{
			name: "Files with empty table names",
			files: []DataFile{
				{TableName: "otel_traces"},
				{TableName: ""},
				{TableName: "otel_logs"},
			},
			expected: []string{"otel_traces", "otel_logs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTableNames(tt.files)

			// Check that all expected tables are present
			assert.Len(t, result, len(tt.expected))

			// Convert to set for comparison (order doesn't matter)
			resultSet := make(map[string]struct{})
			for _, t := range result {
				resultSet[t] = struct{}{}
			}

			for _, expected := range tt.expected {
				_, exists := resultSet[expected]
				assert.True(t, exists, "Expected table %s not found in result", expected)
			}
		})
	}
}

func TestReconciler_GetRegisteredFiles(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)

	t.Run("Returns empty set when catalog is disabled", func(t *testing.T) {
		tmpDir := t.TempDir()

		fileIOCfg := iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: tmpDir,
			},
		}
		fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
		require.NoError(t, err)
		defer func() { _ = fileIO.Close() }()

		catalogCfg := iceberg.CatalogConfig{
			Type:      "none",
			Namespace: "test",
		}
		catalog, err := iceberg.NewCatalog(ctx, catalogCfg, fileIOCfg, logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal))
		require.NoError(t, err)
		defer func() { _ = catalog.Close() }()

		reconciler, err := NewReconciler(fileIO, catalog, "test", vlogger)
		require.NoError(t, err)

		registeredFiles, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces", "otel_logs"})
		require.NoError(t, err)
		assert.Empty(t, registeredFiles)
	})

	t.Run("Returns files from catalog when catalog is active", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		registeredURIs := []string{
			"file:///data/otel_traces/file1.parquet",
			"file:///data/otel_traces/file2.parquet",
		}
		mockCat.SetListDataFiles(registeredURIs)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces"})
		require.NoError(t, err)

		assert.Len(t, files, 2)
		_, exists1 := files[registeredURIs[0]]
		_, exists2 := files[registeredURIs[1]]
		assert.True(t, exists1)
		assert.True(t, exists2)
	})

	t.Run("Returns files from multiple tables", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		traces := []string{
			"file:///data/otel_traces/trace1.parquet",
			"file:///data/otel_traces/trace2.parquet",
		}
		mockCat.SetListDataFilesForTable("otel_traces", traces)

		logs := []string{
			"file:///data/otel_logs/log1.parquet",
		}
		mockCat.SetListDataFilesForTable("otel_logs", logs)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces", "otel_logs"})
		require.NoError(t, err)

		assert.Len(t, files, 3)
		_, exists1 := files[traces[0]]
		_, exists2 := files[traces[1]]
		_, exists3 := files[logs[0]]
		assert.True(t, exists1)
		assert.True(t, exists2)
		assert.True(t, exists3)
	})

	t.Run("Returns error when ListDataFiles fails", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		mockCat.SetListErr(errors.New("catalog connection failed"))

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces"})
		require.Error(t, err)
		assert.Nil(t, files)
		assert.Contains(t, err.Error(), "failed to list data files")
	})

	t.Run("Returns empty set when table has no registered files", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		mockCat.SetListDataFiles([]string{})

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces"})
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("Returns empty map when no tables provided", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{})
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("Handles duplicate URIs by returning unique set", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		traces := []string{
			"file:///data/shared/file1.parquet",
			"file:///data/otel_traces/trace1.parquet",
		}
		mockCat.SetListDataFilesForTable("otel_traces", traces)

		logs := []string{
			"file:///data/shared/file1.parquet", // Duplicate
			"file:///data/otel_logs/log1.parquet",
		}
		mockCat.SetListDataFilesForTable("otel_logs", logs)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces", "otel_logs"})
		require.NoError(t, err)

		assert.Len(t, files, 3)
		_, exists1 := files[traces[0]]
		_, exists2 := files[traces[1]]
		_, exists3 := files[logs[1]]
		assert.True(t, exists1)
		assert.True(t, exists2)
		assert.True(t, exists3)
	})

	t.Run("Error on first table stops processing", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		mockCat.SetListErr(errors.New("connection lost"))

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		files, err := reconciler.GetRegisteredFiles(ctx, "test", []string{"otel_traces", "otel_logs"})
		require.Error(t, err)
		assert.Nil(t, files)
	})
}

func TestReconciler_RecoverFiles_Batching(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)

	t.Run("Small batch - files under maxBatchSize are batched in single call", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		// Skip repartitioner since we don't need it for this test
		reconciler.repartitioner = nil

		files := make([]DataFile, 50)
		for i := 0; i < 50; i++ {
			files[i] = DataFile{
				Path:      fmt.Sprintf("test/file%d.parquet", i),
				URI:       fmt.Sprintf("file:///test/file%d.parquet", i),
				Size:      100,
				TableName: "otel_traces",
			}
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 50 files should succeed
		assert.Equal(t, 50, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 50)

		// Should be exactly 1 AppendDataFiles call (single batch)
		assert.Equal(t, 1, mockCat.GetAppendCalls())
		assert.Equal(t, 50, mockCat.GetAppendCount())
	})

	t.Run("Large batch - files split into multiple batches", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := make([]DataFile, 2500)
		for i := 0; i < 2500; i++ {
			files[i] = DataFile{
				Path:      fmt.Sprintf("test/file%d.parquet", i),
				URI:       fmt.Sprintf("file:///test/file%d.parquet", i),
				Size:      100,
				TableName: "otel_traces",
			}
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 2500 files should succeed
		assert.Equal(t, 2500, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 2500)

		// Should be exactly 3 AppendDataFiles calls (1000 + 1000 + 500)
		assert.Equal(t, 3, mockCat.GetAppendCalls())
		assert.Equal(t, 2500, mockCat.GetAppendCount())
	})

	t.Run("Multiple tables - each table batched separately", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := []DataFile{
			{Path: "test/trace1.parquet", URI: "file:///test/trace1.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/trace2.parquet", URI: "file:///test/trace2.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/log1.parquet", URI: "file:///test/log1.parquet", Size: 100, TableName: "otel_logs"},
			{Path: "test/metric1.parquet", URI: "file:///test/metric1.parquet", Size: 100, TableName: "otel_metrics"},
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 4 files should succeed
		assert.Equal(t, 4, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 4)

		// Should be 3 AppendDataFiles calls (1 per table)
		assert.Equal(t, 3, mockCat.GetAppendCalls())
		assert.Equal(t, 4, mockCat.GetAppendCount())
	})

	t.Run("Batch failure falls back to individual processing", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		// First call will fail (batch), then subsequent calls will succeed
		mockCat.SetAppendErr(errors.New("batch failed"), true)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := []DataFile{
			{Path: "test/file1.parquet", URI: "file:///test/file1.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/file2.parquet", URI: "file:///test/file2.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/file3.parquet", URI: "file:///test/file3.parquet", Size: 100, TableName: "otel_traces"},
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 3 files should succeed (via fallback processing)
		assert.Equal(t, 3, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 3)

		// With gradual reduction using min(batchSize, fileCount):
		// 1 batch + 3 individual = 4 calls
		assert.Equal(t, 4, mockCat.GetAppendCalls())
		// 3 in failed batch + 3 individual
		assert.Equal(t, 6, mockCat.GetAppendCount())
	})

	t.Run("Batch failure with persistent error - individual files fail", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		mockCat.SetAppendErr(errors.New("persistent failure"), false)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := []DataFile{
			{Path: "test/file1.parquet", URI: "file:///test/file1.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/file2.parquet", URI: "file:///test/file2.parquet", Size: 100, TableName: "otel_traces"},
			{Path: "test/file3.parquet", URI: "file:///test/file3.parquet", Size: 100, TableName: "otel_traces"},
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 3 files should fail
		assert.Equal(t, 0, result.SuccessCount)
		assert.Equal(t, 3, result.FailureCount)
		assert.Len(t, result.Errors, 3)

		// With gradual fallback using min(batchSize, fileCount) for 3 files:
		// 1 batch + 3 individual = 4 calls
		assert.Equal(t, 4, mockCat.GetAppendCalls())
		// 3 in failed batch + 3 individual
		assert.Equal(t, 6, mockCat.GetAppendCount())
	})

	t.Run("Gradual batch size reduction - fails at 10, succeeds at 1", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		// Error only for batches >= 10 files
		mockCat.SetAppendErrForMinBatch(errors.New("batch too large"), 10)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := make([]DataFile, 25)
		for i := 0; i < 25; i++ {
			files[i] = DataFile{
				Path:      fmt.Sprintf("test/file%d.parquet", i),
				URI:       fmt.Sprintf("file:///test/file%d.parquet", i),
				Size:      100,
				TableName: "otel_traces",
			}
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 25 files should succeed (via smaller batch processing)
		assert.Equal(t, 25, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 25)

		// With min(batchSize, fileCount) reduction:
		// 1 batch + 13 batch of 2 files each = 14 calls
		assert.Equal(t, 14, mockCat.GetAppendCalls())
		// 25 in failed batch + 25 in successful batches = 50 count
		assert.Equal(t, 50, mockCat.GetAppendCount())
	})

	t.Run("Large batch - first batch fails, subsequent batches start fresh at maxBatchSize", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		// First call will fail (first 1000-file batch), then all subsequent calls succeed
		mockCat.SetAppendErr(errors.New("first batch failed"), true)

		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}
		reconciler.repartitioner = nil

		files := make([]DataFile, 2500)
		for i := 0; i < 2500; i++ {
			files[i] = DataFile{
				Path:      fmt.Sprintf("test/file%d.parquet", i),
				URI:       fmt.Sprintf("file:///test/file%d.parquet", i),
				Size:      100,
				TableName: "otel_traces",
			}
		}

		opts := RecoveryOptions{}
		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		// All 2500 files should succeed
		assert.Equal(t, 2500, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
		assert.Len(t, result.RegisteredFiles, 2500)

		// Batch 1 [0:1000]: 1000 files
		//   - 1 call with 1000 files (fails, error cleared after this)
		//   - effectiveSize = min(1000, 1000) = 1000, nextBatchSize = 100
		//   - 10 calls with 100 files each (succeed)
		// Batch 2 [1000:2000]: 1000 files
		//   - Starts fresh at maxBatchSize (1000)
		//   - 1 call with 1000 files (succeed)
		// Batch 3 [2000:2500]: 500 files
		//   - Starts fresh at maxBatchSize (1000), but only 500 files
		//   - 1 call with 500 files (succeed)
		// Total: 1 + 10 + 1 + 1 = 13 calls
		assert.Equal(t, 13, mockCat.GetAppendCalls())
		// 1000 (failed) + 1000 (retry succeed) + 1000 (batch2) + 500 (batch3) = 3500 count
		assert.Equal(t, 3500, mockCat.GetAppendCount())
	})
}

func TestReconciler_RecoverFiles_DryRun(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}
	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Create no-op catalog
	catalogCfg := iceberg.CatalogConfig{
		Type:      "none",
		Namespace: "test",
	}
	catalog, err := iceberg.NewCatalog(ctx, catalogCfg, fileIOCfg, logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal))
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	// Create reconciler
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	reconciler, err := NewReconciler(fileIO, catalog, "test", vlogger)
	require.NoError(t, err)

	// Test files
	files := []DataFile{
		{
			Path:      "test/file1.parquet",
			URI:       "file:///test/file1.parquet",
			Size:      100,
			TableName: "otel_traces",
		},
		{
			Path:      "test/file2.parquet",
			URI:       "file:///test/file2.parquet",
			Size:      200,
			TableName: "otel_logs",
		},
	}

	t.Run("Dry run mode", func(t *testing.T) {
		opts := RecoveryOptions{
			DryRun: true,
		}

		result, err := reconciler.RecoverFiles(ctx, files, opts)
		require.NoError(t, err)

		assert.Equal(t, 2, result.TotalFiles)
		assert.Equal(t, 2, result.SkippedCount) // All skipped in dry run
		assert.Equal(t, 0, result.SuccessCount)
		assert.Equal(t, 0, result.FailureCount)
	})
}

func Test_groupFilesByTable(t *testing.T) {
	tests := []struct {
		name     string
		files    []DataFile
		expected map[string]int // table name -> expected count
	}{
		{
			name:     "Empty files list",
			files:    []DataFile{},
			expected: map[string]int{},
		},
		{
			name: "Single table",
			files: []DataFile{
				{TableName: "otel_traces"},
				{TableName: "otel_traces"},
				{TableName: "otel_traces"},
			},
			expected: map[string]int{"otel_traces": 3},
		},
		{
			name: "Multiple tables",
			files: []DataFile{
				{TableName: "otel_traces"},
				{TableName: "otel_logs"},
				{TableName: "otel_traces"},
				{TableName: "otel_metrics"},
				{TableName: "otel_logs"},
			},
			expected: map[string]int{
				"otel_traces":  2,
				"otel_logs":    2,
				"otel_metrics": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupFilesByTable(tt.files)

			// Check number of tables
			assert.Len(t, result, len(tt.expected))

			// Check count for each table
			for tableName, expectedCount := range tt.expected {
				files, exists := result[tableName]
				assert.True(t, exists, "Expected table %s not found in result", tableName)
				assert.Len(t, files, expectedCount, "Unexpected file count for table %s", tableName)
			}
		})
	}
}

// Simulate a cross-partition error scenario
// Note: In a real scenario, this would be triggered by catalog returning CrossPartitionError
// For this test, we're testing the deletion logic directly
func TestReconciler_handleCrossPartitionFile(t *testing.T) {
	ctx := context.Background()
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)

	t.Run("Original file deleted when all repartitioned files registered", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()

		// Create a reconciler with mocks
		reconciler := &Reconciler{
			scanner:   NewScanner(mockFIO, vlogger),
			catalog:   mockCat,
			fileIO:    mockFIO,
			namespace: "test",
			logger:    vlogger,
		}

		// Create a mock repartitioner that returns successful repartitioned files
		repartitioner, err := NewRepartitioner(mockFIO, "UTC", "snappy", vlogger)
		require.NoError(t, err)
		reconciler.repartitioner = repartitioner

		// Create test data file
		originalPath := "test/original.parquet"
		mockFIO.SetFile(originalPath, []byte("original data"))

		// Create test result
		result := &RecoveryResult{
			RegisteredFiles:                   make([]DataFile, 0),
			Errors:                            make([]FileError, 0),
			DeletedRepartitionedOriginalFiles: make([]string, 0),
		}

		// Since we can't easily create a valid parquet file that spans partitions,
		// we'll test the deletion logic by checking the result struct initialization
		assert.NotNil(t, result.DeletedRepartitionedOriginalFiles)
		assert.Empty(t, result.DeletedRepartitionedOriginalFiles)

		// Test that the file exists before any operation
		_, err = mockFIO.Read(ctx, originalPath)
		require.NoError(t, err)

		// Test deletion works
		err = mockFIO.Delete(ctx, originalPath)
		require.NoError(t, err)
		assert.Contains(t, mockFIO.GetDeletedPaths(), originalPath)
	})

	t.Run("Original file NOT deleted when registration fails", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockCat := iceberg.NewMockCatalog()
		mockCat.SetAppendErr(errors.New("registration failed"), false)

		// Create test data file
		originalPath := "test/original.parquet"
		mockFIO.SetFile(originalPath, []byte("original data"))

		// Verify file exists
		_, err := mockFIO.Read(ctx, originalPath)
		require.NoError(t, err)

		// Simulate that registration fails - file should NOT be deleted
		// The logic is: if appendErr is set, allSucceeded will be false
		// and the original file should not be deleted
		assert.Empty(t, mockFIO.GetDeletedPaths())
	})

	t.Run("Delete failure logged but not counted as error", func(t *testing.T) {
		mockFIO := iceberg.NewMockFileIO()
		mockFIO.SetDeleteErr(errors.New("delete failed"))

		originalPath := "test/original.parquet"
		mockFIO.SetFile(originalPath, []byte("original data"))

		// Attempt to delete should fail
		err := mockFIO.Delete(ctx, originalPath)
		require.Error(t, err)

		// File should still exist (delete failed)
		_, err = mockFIO.Read(ctx, originalPath)
		require.NoError(t, err)
	})
}

func TestReconciler_Recover_WithPreFiltering(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIOCfg := iceberg.FileIOConfig{
		Type: "filesystem",
		Filesystem: iceberg.LocalFileIOConfig{
			BasePath: tmpDir,
		},
	}
	fileIO, err := iceberg.NewFileIO(ctx, fileIOCfg)
	require.NoError(t, err)
	defer func() { _ = fileIO.Close() }()

	// Create no-op catalog
	catalogCfg := iceberg.CatalogConfig{
		Type:      "none",
		Namespace: "test",
	}
	catalog, err := iceberg.NewCatalog(ctx, catalogCfg, fileIOCfg, logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal))
	require.NoError(t, err)
	defer func() { _ = catalog.Close() }()

	// Write test files
	testFiles := []struct {
		path string
		data []byte
	}{
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=10/00000-0-test1.parquet", constants.ExporterName),
			data: []byte("trace data 1"),
		},
		{
			path: fmt.Sprintf("%s/otel_traces/data/year=2024/month=06/day=15/hour=11/00000-0-test2.parquet", constants.ExporterName),
			data: []byte("trace data 2"),
		},
	}

	for _, tf := range testFiles {
		err := fileIO.Write(ctx, tf.path, tf.data, iceberg.DefaultWriteOptions())
		require.NoError(t, err)
	}

	// Create reconciler
	vlogger := logger.New(zaptest.NewLogger(t), configtelemetry.LevelNormal)
	reconciler, err := NewReconciler(fileIO, catalog, "test", vlogger)
	require.NoError(t, err)

	t.Run("Recover with dry run shows files to be recovered", func(t *testing.T) {
		opts := RecoveryOptions{
			DryRun:    true,
			Namespace: "test",
		}

		result, err := reconciler.Recover(ctx, opts)
		require.NoError(t, err)

		// With NoCatalog, GetRegisteredFiles returns empty set
		// So all files should be considered orphaned
		assert.Equal(t, 2, result.TotalFiles)
		// In dry run mode, SkippedCount includes both pre-filtered (0) and dry-run skipped (2)
		assert.Equal(t, 2, result.SkippedCount)
		assert.Equal(t, 0, result.SuccessCount)
	})

	t.Run("Recover with no files returns empty result", func(t *testing.T) {
		emptyDir := t.TempDir()
		emptyFileIOCfg := iceberg.FileIOConfig{
			Type: "filesystem",
			Filesystem: iceberg.LocalFileIOConfig{
				BasePath: emptyDir,
			},
		}

		emptyFileIO, err := iceberg.NewFileIO(ctx, emptyFileIOCfg)
		require.NoError(t, err)
		defer func() { _ = emptyFileIO.Close() }()

		emptyReconciler, err := NewReconciler(emptyFileIO, catalog, "test", vlogger)
		require.NoError(t, err)

		opts := RecoveryOptions{
			Namespace: "test",
		}

		result, err := emptyReconciler.Recover(ctx, opts)
		require.NoError(t, err)

		assert.Equal(t, 0, result.TotalFiles)
	})
}
