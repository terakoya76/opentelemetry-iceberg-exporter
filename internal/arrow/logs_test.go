package arrow

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestGolden_Logs(t *testing.T) {
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

			// Generate current record from test data
			logs := tc.createFn()
			record, err := converter.Convert(logs)
			require.NoError(t, err)
			defer record.Release()

			// Read expected output from golden file
			expected := readParquetAsTable(t, goldenPath)
			defer expected.Release()

			// Compare generated record with golden file
			compareRecordWithTable(t, record, expected)
		})
	}
}

func TestRoundTrip_Logs(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewLogsConverter(allocator)

	original := createDeterministicLogs()
	record, err := converter.Convert(original)
	require.NoError(t, err)
	defer record.Release()

	// Write to Parquet buffer
	data, err := WriteParquet(record, ParquetWriterOptions{Compression: "snappy"})
	require.NoError(t, err)

	// Read back from Parquet
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, allocator)
	require.NoError(t, err)

	table, err := arrowReader.ReadTable(context.Background())
	require.NoError(t, err)
	defer table.Release()

	// Verify row count matches
	assert.Equal(t, record.NumRows(), table.NumRows())
	assert.Equal(t, record.NumCols(), table.NumCols())
}

func TestNewLogsConverter(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewLogsConverter(allocator)

	assert.NotNil(t, converter)
	assert.NotNil(t, converter.Schema())
	assert.Equal(t, LogsSchema().NumFields(), converter.Schema().NumFields())
}

func TestLogsConverter_Schema(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewLogsConverter(allocator)

	schema := converter.Schema()
	assert.NotNil(t, schema)

	// Verify expected fields exist
	expectedFields := []string{
		FieldLogTimeUnixNano,
		FieldLogSeverityNumber,
		FieldLogSeverityText,
		FieldLogBody,
		FieldLogAttributes,
		FieldLogDroppedAttributesCount,
		FieldLogFlags,
		FieldLogTraceId,
		FieldLogSpanId,
		FieldLogObservedTimeUnixNano,
		FieldLogEventName,
		FieldServiceName,
		FieldResourceAttributes,
		FieldResourceDroppedAttributesCount,
		FieldScopeName,
		FieldScopeVersion,
		FieldScopeAttributes,
		FieldScopeDroppedAttributesCount,
	}

	assert.Equal(t, len(expectedFields), schema.NumFields())
	for i, name := range expectedFields {
		assert.Equal(t, name, schema.Field(i).Name, "Field at index %d should be %s", i, name)
	}
}

func TestLogValueToJSON(t *testing.T) {
	testCases := []struct {
		name     string
		setup    func() pcommon.Value
		expected string
	}{
		{
			name: "string value",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetStr("hello")
				return v
			},
			expected: "hello",
		},
		{
			name: "int value",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetInt(42)
				return v
			},
			expected: "42",
		},
		{
			name: "double value",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetDouble(3.14)
				return v
			},
			expected: "3.140000",
		},
		{
			name: "bool true",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetBool(true)
				return v
			},
			expected: "true",
		},
		{
			name: "bool false",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetBool(false)
				return v
			},
			expected: "false",
		},
		{
			name: "empty value",
			setup: func() pcommon.Value {
				return pcommon.NewValueEmpty()
			},
			expected: "",
		},
		{
			name: "map value",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				m := v.SetEmptyMap()
				m.PutStr("key", "value")
				m.PutInt("num", 123)
				return v
			},
			expected: `{"key":"value","num":123}`,
		},
		{
			name: "slice value",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				s := v.SetEmptySlice()
				s.AppendEmpty().SetStr("a")
				s.AppendEmpty().SetStr("b")
				return v
			},
			expected: `["a","b"]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.setup()
			result := logValueToJSON(v)
			if tc.name == "map value" {
				// For maps, parse and compare since key order may vary
				var resultMap, expectedMap map[string]interface{}
				err := json.Unmarshal([]byte(result), &resultMap)
				require.NoError(t, err)
				err = json.Unmarshal([]byte(tc.expected), &expectedMap)
				require.NoError(t, err)
				assert.Equal(t, expectedMap, resultMap)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
