package arrow

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestGenerateGoldenFiles regenerates all golden parquet files.
// This test is skipped in short mode (CI) and must be run explicitly:
//
//	go test ./internal/arrow/... -run TestGenerateGoldenFiles -v
//
// Run this after changing:
// - Schema definitions
// - Converter logic
// - Test data generators
func TestGenerateGoldenFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping golden file generation in short mode")
	}

	t.Run("logs", generateLogsGoldenFiles)
	t.Run("traces", generateTracesGoldenFiles)
	t.Run("metrics", generateMetricsGoldenFiles)
}

func generateLogsGoldenFiles(t *testing.T) {
	testCases := []struct {
		name     string
		createFn func() plog.Logs
	}{
		{"basic", createDeterministicLogs},
		{"empty", createEmptyLogs},
		{"nullable", createLogsWithNullableFields},
		{"complex_body", createLogsWithComplexBody},
		{"multi_resource", createLogsWithMultipleResources},
	}

	allocator := memory.NewGoAllocator()
	converter := NewLogsConverter(allocator)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goldenPath := filepath.Join(testdataDir, "logs_"+tc.name+".parquet")

			logs := tc.createFn()
			record, err := converter.Convert(logs)
			require.NoError(t, err)
			defer record.Release()

			writeGoldenParquet(t, goldenPath, record)
			t.Logf("Generated: %s", goldenPath)
		})
	}
}

func generateTracesGoldenFiles(t *testing.T) {
	testCases := []struct {
		name     string
		createFn func() ptrace.Traces
	}{
		{"basic", createDeterministicTraces},
		{"empty", createEmptyTraces},
		{"nullable", createTracesWithNullableFields},
		{"all_span_kinds", createTracesWithAllSpanKinds},
		{"status_codes", createTracesWithStatusCodes},
		{"multi_events", createTracesWithMultipleEvents},
		{"multi_links", createTracesWithMultipleLinks},
	}

	allocator := memory.NewGoAllocator()
	converter := NewTracesConverter(allocator)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			goldenPath := filepath.Join(testdataDir, "traces_"+tc.name+".parquet")

			traces := tc.createFn()
			record, err := converter.Convert(traces)
			require.NoError(t, err)
			defer record.Release()

			writeGoldenParquet(t, goldenPath, record)
			t.Logf("Generated: %s", goldenPath)
		})
	}
}

func generateMetricsGoldenFiles(t *testing.T) {
	testCases := []struct {
		name     string
		createFn func() pmetric.Metrics
	}{
		{"basic", createDeterministicMetrics},
		{"empty", createEmptyMetrics},
		{"nullable", createMetricsWithNullableFields},
		{"gauge", createMetricsWithGauge},
		{"sum", createMetricsWithSum},
		{"histogram", createMetricsWithHistogram},
		{"summary", createMetricsWithSummary},
		{"exp_histogram", createMetricsWithExponentialHistogram},
		{"mixed", createMetricsWithMixed},
	}

	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := tc.createFn()
			records, err := converter.Convert(metrics)
			require.NoError(t, err)
			defer records.Release()

			// Write each metric type to its own golden file
			records.ForEach(func(metricType MetricType, record arrow.RecordBatch) {
				goldenPath := filepath.Join(testdataDir, "metrics_"+tc.name+"_"+string(metricType)+".parquet")
				writeGoldenParquet(t, goldenPath, record)
				t.Logf("Generated: %s", goldenPath)
			})
		})
	}
}

// writeGoldenParquet writes an Arrow record to a Parquet file
func writeGoldenParquet(t *testing.T, path string, record arrow.RecordBatch) {
	t.Helper()

	data, err := WriteParquet(record, ParquetWriterOptions{Compression: "snappy"})
	require.NoError(t, err, "Failed to write parquet")

	err = os.MkdirAll(filepath.Dir(path), 0755)
	require.NoError(t, err, "Failed to create directory")

	err = os.WriteFile(path, data, 0644)
	require.NoError(t, err, "Failed to write golden file: %s", path)
}
