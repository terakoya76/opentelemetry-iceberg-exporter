package arrow

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestGolden_Traces(t *testing.T) {
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

			// Generate current record from test data
			traces := tc.createFn()
			record, err := converter.Convert(traces)
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

func TestRoundTrip_Traces(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewTracesConverter(allocator)

	original := createDeterministicTraces()
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

func TestNewTracesConverter(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewTracesConverter(allocator)

	assert.NotNil(t, converter)
	assert.NotNil(t, converter.Schema())
	assert.Equal(t, TracesSchema().NumFields(), converter.Schema().NumFields())
}

func TestTracesConverter_Schema(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewTracesConverter(allocator)

	schema := converter.Schema()
	assert.NotNil(t, schema)

	// Verify expected fields exist in order
	expectedFields := []string{
		FieldTraceTraceId,
		FieldTraceSpanId,
		FieldTraceTraceState,
		FieldTraceParentSpanId,
		FieldTraceSpanFlags,
		FieldTraceSpanName,
		FieldTraceSpanKind,
		FieldTraceStartTimeUnixNano,
		FieldTraceEndTimeUnixNano,
		FieldTraceSpanAttributes,
		FieldTraceDroppedAttributesCount,
		FieldTraceEventsTimeUnixNano,
		FieldTraceEventsName,
		FieldTraceEventsAttributes,
		FieldTraceEventsDroppedAttributesCount,
		FieldTraceDroppedEventsCount,
		FieldTraceLinksTraceId,
		FieldTraceLinksSpanId,
		FieldTraceLinksTraceState,
		FieldTraceLinksAttributes,
		FieldTraceLinksDroppedAttributesCount,
		FieldTraceLinksFlags,
		FieldTraceDroppedLinksCount,
		FieldTraceStatusCode,
		FieldTraceStatusMessage,
		FieldTraceDuration,
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

func TestAttributesToJSON(t *testing.T) {
	testCases := []struct {
		name     string
		setup    func() pcommon.Map
		contains []string
	}{
		{
			name: "empty map",
			setup: func() pcommon.Map {
				return pcommon.NewMap()
			},
			contains: []string{"{}"},
		},
		{
			name: "string attribute",
			setup: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("key", "value")
				return m
			},
			contains: []string{`"key":"value"`},
		},
		{
			name: "int attribute",
			setup: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutInt("count", 42)
				return m
			},
			contains: []string{`"count":42`},
		},
		{
			name: "bool attribute",
			setup: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutBool("enabled", true)
				return m
			},
			contains: []string{`"enabled":true`},
		},
		{
			name: "double attribute",
			setup: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutDouble("score", 3.14)
				return m
			},
			contains: []string{`"score":3.14`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup()
			result := attributesToJSON(m)
			for _, s := range tc.contains {
				assert.Contains(t, result, s)
			}
		})
	}
}

func TestTraceIDToString(t *testing.T) {
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", traceIDToString(testTraceID))
	assert.Equal(t, "00000000000000000000000000000000", traceIDToString(zeroTraceID))
}

func TestSpanIDToString(t *testing.T) {
	assert.Equal(t, "1112131415161718", spanIDToString(testSpanID))
	assert.Equal(t, "0000000000000000", spanIDToString(zeroSpanID))
}

func TestSpanKindToString(t *testing.T) {
	testCases := []struct {
		kind     ptrace.SpanKind
		expected string
	}{
		{ptrace.SpanKindUnspecified, "UNSPECIFIED"},
		{ptrace.SpanKindInternal, "INTERNAL"},
		{ptrace.SpanKindServer, "SERVER"},
		{ptrace.SpanKindClient, "CLIENT"},
		{ptrace.SpanKindProducer, "PRODUCER"},
		{ptrace.SpanKindConsumer, "CONSUMER"},
		{ptrace.SpanKind(99), "UNKNOWN"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, spanKindToString(tc.kind))
		})
	}
}

func TestStatusCodeToString(t *testing.T) {
	testCases := []struct {
		code     ptrace.StatusCode
		expected string
	}{
		{ptrace.StatusCodeUnset, ""},
		{ptrace.StatusCodeOk, "OK"},
		{ptrace.StatusCodeError, "ERROR"},
		{ptrace.StatusCode(99), ""},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, statusCodeToString(tc.code))
		})
	}
}

func TestTimestampToPartition(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)

	testCases := []struct {
		name     string
		format   string
		timezone string
		expected string
	}{
		{
			name:     "hourly UTC",
			format:   "2006/01/02/15",
			timezone: "UTC",
			expected: "2024/01/15/10",
		},
		{
			name:     "daily UTC",
			format:   "2006/01/02",
			timezone: "UTC",
			expected: "2024/01/15",
		},
		{
			name:     "with timezone offset",
			format:   "2006/01/02/15",
			timezone: "America/New_York",
			expected: "2024/01/15/05", // UTC-5
		},
		{
			name:     "invalid timezone defaults to UTC",
			format:   "2006/01/02/15",
			timezone: "Invalid/Timezone",
			expected: "2024/01/15/10",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := TimestampToPartition(ts, tc.format, tc.timezone)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestValueToInterface(t *testing.T) {
	testCases := []struct {
		name     string
		setup    func() pcommon.Value
		expected interface{}
	}{
		{
			name: "string",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetStr("hello")
				return v
			},
			expected: "hello",
		},
		{
			name: "int",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetInt(42)
				return v
			},
			expected: int64(42),
		},
		{
			name: "double",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetDouble(3.14)
				return v
			},
			expected: 3.14,
		},
		{
			name: "bool",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				v.SetBool(true)
				return v
			},
			expected: true,
		},
		{
			name: "empty",
			setup: func() pcommon.Value {
				return pcommon.NewValueEmpty()
			},
			expected: nil,
		},
		{
			name: "slice",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				s := v.SetEmptySlice()
				s.AppendEmpty().SetStr("a")
				s.AppendEmpty().SetInt(1)
				return v
			},
			expected: []interface{}{"a", int64(1)},
		},
		{
			name: "map",
			setup: func() pcommon.Value {
				v := pcommon.NewValueEmpty()
				m := v.SetEmptyMap()
				m.PutStr("key", "value")
				return v
			},
			expected: map[string]interface{}{"key": "value"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.setup()
			result := valueToInterface(v)
			assert.Equal(t, tc.expected, result)
		})
	}
}
