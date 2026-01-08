package arrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracesSchema(t *testing.T) {
	schema := TracesSchema()

	require.NotNil(t, schema)

	// Verify schema version metadata
	metadata := schema.Metadata()
	idx := metadata.FindKey("iceberg_exporter.traces_schema_version")
	assert.GreaterOrEqual(t, idx, 0, "Schema should have version metadata")
	if idx >= 0 {
		assert.Equal(t, "1.0.0", metadata.Values()[idx])
	}

	// Expected fields in order (must match proto field order)
	expectedFields := []struct {
		name     string
		nullable bool
		isList   bool
	}{
		{FieldTraceTraceId, false, false},
		{FieldTraceSpanId, false, false},
		{FieldTraceTraceState, true, false},
		{FieldTraceParentSpanId, true, false},
		{FieldTraceSpanFlags, false, false},
		{FieldTraceSpanName, false, false},
		{FieldTraceSpanKind, false, false},
		{FieldTraceStartTimeUnixNano, false, false},
		{FieldTraceEndTimeUnixNano, false, false},
		{FieldTraceSpanAttributes, false, false},
		{FieldTraceDroppedAttributesCount, false, false},
		{FieldTraceEventsTimeUnixNano, false, true},
		{FieldTraceEventsName, false, true},
		{FieldTraceEventsAttributes, false, true},
		{FieldTraceEventsDroppedAttributesCount, false, true},
		{FieldTraceDroppedEventsCount, false, false},
		{FieldTraceLinksTraceId, false, true},
		{FieldTraceLinksSpanId, false, true},
		{FieldTraceLinksTraceState, false, true},
		{FieldTraceLinksAttributes, false, true},
		{FieldTraceLinksDroppedAttributesCount, false, true},
		{FieldTraceLinksFlags, false, true},
		{FieldTraceDroppedLinksCount, false, false},
		{FieldTraceStatusCode, true, false},
		{FieldTraceStatusMessage, true, false},
		{FieldTraceDuration, false, false},
		{FieldServiceName, true, false},
		{FieldResourceAttributes, false, false},
		{FieldResourceDroppedAttributesCount, false, false},
		{FieldScopeName, true, false},
		{FieldScopeVersion, true, false},
		{FieldScopeAttributes, false, false},
		{FieldScopeDroppedAttributesCount, false, false},
	}

	assert.Equal(t, len(expectedFields), schema.NumFields(), "Schema should have %d fields", len(expectedFields))

	for i, expected := range expectedFields {
		field := schema.Field(i)
		assert.Equal(t, expected.name, field.Name, "Field %d should be %s", i, expected.name)
		assert.Equal(t, expected.nullable, field.Nullable, "Field %s nullable should be %v", expected.name, expected.nullable)

		if expected.isList {
			_, ok := field.Type.(*arrow.ListType)
			assert.True(t, ok, "Field %s should be a list type", expected.name)
		}

		// Verify Parquet field_id metadata
		fieldIdIdx := field.Metadata.FindKey("PARQUET:field_id")
		assert.GreaterOrEqual(t, fieldIdIdx, 0, "Field %s should have PARQUET:field_id metadata", expected.name)
	}
}

func TestLogsSchema(t *testing.T) {
	schema := LogsSchema()

	require.NotNil(t, schema)

	// Verify schema version metadata
	metadata := schema.Metadata()
	idx := metadata.FindKey("iceberg_exporter.logs_schema_version")
	assert.GreaterOrEqual(t, idx, 0, "Schema should have version metadata")
	if idx >= 0 {
		assert.Equal(t, "1.0.0", metadata.Values()[idx])
	}

	// Expected fields in order
	expectedFields := []struct {
		name     string
		nullable bool
	}{
		{FieldLogTimeUnixNano, false},
		{FieldLogSeverityNumber, false},
		{FieldLogSeverityText, true},
		{FieldLogBody, false},
		{FieldLogAttributes, false},
		{FieldLogDroppedAttributesCount, false},
		{FieldLogFlags, false},
		{FieldLogTraceId, true},
		{FieldLogSpanId, true},
		{FieldLogObservedTimeUnixNano, true},
		{FieldLogEventName, true},
		{FieldServiceName, true},
		{FieldResourceAttributes, false},
		{FieldResourceDroppedAttributesCount, false},
		{FieldScopeName, true},
		{FieldScopeVersion, true},
		{FieldScopeAttributes, false},
		{FieldScopeDroppedAttributesCount, false},
	}

	assert.Equal(t, len(expectedFields), schema.NumFields(), "Schema should have %d fields", len(expectedFields))

	for i, expected := range expectedFields {
		field := schema.Field(i)
		assert.Equal(t, expected.name, field.Name, "Field %d should be %s", i, expected.name)
		assert.Equal(t, expected.nullable, field.Nullable, "Field %s nullable should be %v", expected.name, expected.nullable)

		// Verify Parquet field_id metadata
		fieldIdIdx := field.Metadata.FindKey("PARQUET:field_id")
		assert.GreaterOrEqual(t, fieldIdIdx, 0, "Field %s should have PARQUET:field_id metadata", expected.name)
	}
}

func TestMetricSchemas(t *testing.T) {
	testCases := []struct {
		name         string
		schema       *arrow.Schema
		metadataKey  string
		expectedCols int
	}{
		{"Gauge", MetricsGaugeSchema(), "iceberg_exporter.metrics_gauge_schema_version", 23},
		{"Sum", MetricsSumSchema(), "iceberg_exporter.metrics_sum_schema_version", 25},
		{"Histogram", MetricsHistogramSchema(), "iceberg_exporter.metrics_histogram_schema_version", 28},
		{"ExponentialHistogram", MetricsExponentialHistogramSchema(), "iceberg_exporter.metrics_exponential_histogram_schema_version", 33},
		{"Summary", MetricsSummarySchema(), "iceberg_exporter.metrics_summary_schema_version", 19},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.schema)
			assert.Equal(t, tc.expectedCols, tc.schema.NumFields())

			// Verify schema version metadata
			metadata := tc.schema.Metadata()
			idx := metadata.FindKey(tc.metadataKey)
			assert.GreaterOrEqual(t, idx, 0, "Schema should have version metadata")
			if idx >= 0 {
				assert.Equal(t, "1.0.0", metadata.Values()[idx])
			}

			// Common fields should exist in all metric schemas
			commonFields := []string{
				FieldMetricTimeUnixNano,
				FieldServiceName,
				FieldResourceAttributes,
				FieldScopeName,
				FieldMetricName,
				FieldMetricAttributes,
				FieldMetricFlags,
			}

			for _, fieldName := range commonFields {
				indices := tc.schema.FieldIndices(fieldName)
				require.NotEmpty(t, indices, "Field %s should exist in %s schema", fieldName, tc.name)

				field := tc.schema.Field(indices[0])
				// Verify Parquet field_id metadata
				fieldIdIdx := field.Metadata.FindKey("PARQUET:field_id")
				assert.GreaterOrEqual(t, fieldIdIdx, 0, "Field %s should have PARQUET:field_id metadata", fieldName)
			}
		})
	}
}

func TestBuildSchema(t *testing.T) {
	t.Run("basic fields", func(t *testing.T) {
		defs := []FieldDef{
			{"field1", arrow.BinaryTypes.String, false, 1},
			{"field2", arrow.PrimitiveTypes.Int64, true, 2},
			{"field3", arrow.ListOf(arrow.BinaryTypes.String), true, 3},
		}

		metadata := arrow.NewMetadata(
			[]string{"test_version"},
			[]string{"1.0.0"},
		)
		schema := buildSchema(defs, &metadata)

		require.NotNil(t, schema)
		assert.Equal(t, 3, schema.NumFields())

		// Verify schema-level metadata
		schemaMetadata := schema.Metadata()
		idx := schemaMetadata.FindKey("test_version")
		assert.GreaterOrEqual(t, idx, 0)
		assert.Equal(t, "1.0.0", schemaMetadata.Values()[idx])

		// Verify each field
		for i, def := range defs {
			field := schema.Field(i)
			assert.Equal(t, def.Name, field.Name)
			assert.Equal(t, def.Type, field.Type)
			assert.Equal(t, def.Nullable, field.Nullable)

			// Verify field ID metadata
			fieldIDIdx := field.Metadata.FindKey("PARQUET:field_id")
			require.GreaterOrEqual(t, fieldIDIdx, 0, "Field %s should have PARQUET:field_id metadata", def.Name)
			assert.Equal(t, itoa(def.FieldID), field.Metadata.Values()[fieldIDIdx])
		}
	})

	t.Run("with nil metadata", func(t *testing.T) {
		defs := []FieldDef{
			{"test_field", arrow.BinaryTypes.String, false, 1},
		}

		schema := buildSchema(defs, nil)

		require.NotNil(t, schema)
		assert.Equal(t, 1, schema.NumFields())

		// Verify field is created correctly even without schema-level metadata
		field := schema.Field(0)
		assert.Equal(t, "test_field", field.Name)

		// Field should still have its field ID metadata
		fieldIDIdx := field.Metadata.FindKey("PARQUET:field_id")
		require.GreaterOrEqual(t, fieldIDIdx, 0)
		assert.Equal(t, "1", field.Metadata.Values()[fieldIDIdx])
	})

	t.Run("empty definitions", func(t *testing.T) {
		defs := []FieldDef{}

		schema := buildSchema(defs, nil)

		require.NotNil(t, schema)
		assert.Equal(t, 0, schema.NumFields())
	})

	t.Run("field IDs are unique", func(t *testing.T) {
		testCases := []struct {
			name   string
			schema *arrow.Schema
		}{
			{"TracesSchema", TracesSchema()},
			{"LogsSchema", LogsSchema()},
			{"MetricsGaugeSchema", MetricsGaugeSchema()},
			{"MetricsSumSchema", MetricsSumSchema()},
			{"MetricsHistogramSchema", MetricsHistogramSchema()},
			{"MetricsExponentialHistogramSchema", MetricsExponentialHistogramSchema()},
			{"MetricsSummarySchema", MetricsSummarySchema()},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fieldIDs := make(map[string]bool)

				for i := 0; i < tc.schema.NumFields(); i++ {
					field := tc.schema.Field(i)
					idx := field.Metadata.FindKey("PARQUET:field_id")
					require.GreaterOrEqual(t, idx, 0, "Field %s should have PARQUET:field_id", field.Name)

					fieldID := field.Metadata.Values()[idx]
					assert.False(t, fieldIDs[fieldID], "Field ID %s is duplicated in %s (field: %s)", fieldID, tc.name, field.Name)
					fieldIDs[fieldID] = true
				}
			})
		}
	})

	t.Run("field IDs are sequential", func(t *testing.T) {
		testCases := []struct {
			name   string
			schema *arrow.Schema
		}{
			{"TracesSchema", TracesSchema()},
			{"LogsSchema", LogsSchema()},
			{"MetricsGaugeSchema", MetricsGaugeSchema()},
			{"MetricsSumSchema", MetricsSumSchema()},
			{"MetricsHistogramSchema", MetricsHistogramSchema()},
			{"MetricsExponentialHistogramSchema", MetricsExponentialHistogramSchema()},
			{"MetricsSummarySchema", MetricsSummarySchema()},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				for i := 0; i < tc.schema.NumFields(); i++ {
					field := tc.schema.Field(i)
					idx := field.Metadata.FindKey("PARQUET:field_id")
					require.GreaterOrEqual(t, idx, 0)

					fieldID := field.Metadata.Values()[idx]
					expectedID := itoa(i + 1) // Field IDs are 1-based
					assert.Equal(t, expectedID, fieldID, "Field %s at index %d should have field_id %s, got %s", field.Name, i, expectedID, fieldID)
				}
			})
		}
	})
}

func TestItoa(t *testing.T) {
	testCases := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{123, "123"},
		{1000, "1000"},
		{-1, "-1"},
		{-42, "-42"},
		{-1000, "-1000"},
		{2147483647, "2147483647"},   // Max int32
		{-2147483648, "-2147483648"}, // Min int32
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, itoa(tc.input))
		})
	}
}

func TestTracesSchema_FieldTypes(t *testing.T) {
	schema := TracesSchema()

	// Verify specific field types
	timestampType := arrow.FixedWidthTypes.Timestamp_us

	// TraceId and SpanId should be strings
	traceIdField := schema.Field(schema.FieldIndices(FieldTraceTraceId)[0])
	assert.Equal(t, arrow.BinaryTypes.String, traceIdField.Type)

	spanIdField := schema.Field(schema.FieldIndices(FieldTraceSpanId)[0])
	assert.Equal(t, arrow.BinaryTypes.String, spanIdField.Type)

	// Timestamps should use microsecond precision
	startTimeField := schema.Field(schema.FieldIndices(FieldTraceStartTimeUnixNano)[0])
	assert.Equal(t, timestampType, startTimeField.Type)

	endTimeField := schema.Field(schema.FieldIndices(FieldTraceEndTimeUnixNano)[0])
	assert.Equal(t, timestampType, endTimeField.Type)

	// Duration should be int64
	durationField := schema.Field(schema.FieldIndices(FieldTraceDuration)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, durationField.Type)

	// Flags should be int64 (converted from uint32 for Iceberg compatibility)
	flagsField := schema.Field(schema.FieldIndices(FieldTraceSpanFlags)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, flagsField.Type)

	// Events time should be list of timestamps
	eventsTimeField := schema.Field(schema.FieldIndices(FieldTraceEventsTimeUnixNano)[0])
	listType, ok := eventsTimeField.Type.(*arrow.ListType)
	require.True(t, ok)
	assert.Equal(t, timestampType, listType.Elem())
}

func TestLogsSchema_FieldTypes(t *testing.T) {
	schema := LogsSchema()

	timestampType := arrow.FixedWidthTypes.Timestamp_us

	// TimeUnixNano should use microsecond precision
	timeField := schema.Field(schema.FieldIndices(FieldLogTimeUnixNano)[0])
	assert.Equal(t, timestampType, timeField.Type)

	// SeverityNumber should be int32
	severityField := schema.Field(schema.FieldIndices(FieldLogSeverityNumber)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int32, severityField.Type)

	// Body should be string (JSON encoded)
	bodyField := schema.Field(schema.FieldIndices(FieldLogBody)[0])
	assert.Equal(t, arrow.BinaryTypes.String, bodyField.Type)

	// Flags should be int64 (converted from uint32 for Iceberg compatibility)
	flagsField := schema.Field(schema.FieldIndices(FieldLogFlags)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, flagsField.Type)
}

func TestMetricsGaugeSchema_FieldTypes(t *testing.T) {
	schema := MetricsGaugeSchema()
	timestampType := arrow.FixedWidthTypes.Timestamp_us

	// TimeUnixNano should use microsecond precision
	timeField := schema.Field(schema.FieldIndices(FieldMetricTimeUnixNano)[0])
	assert.Equal(t, timestampType, timeField.Type)

	// AsDouble should be float64
	asDoubleField := schema.Field(schema.FieldIndices(FieldMetricAsDouble)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Float64, asDoubleField.Type)

	// AsInt should be int64
	asIntField := schema.Field(schema.FieldIndices(FieldMetricAsInt)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, asIntField.Type)
}

func TestMetricsHistogramSchema_FieldTypes(t *testing.T) {
	schema := MetricsHistogramSchema()
	timestampType := arrow.FixedWidthTypes.Timestamp_us

	// TimeUnixNano should use microsecond precision
	timeField := schema.Field(schema.FieldIndices(FieldMetricTimeUnixNano)[0])
	assert.Equal(t, timestampType, timeField.Type)

	// Count should be int64 (converted from uint64 for Iceberg compatibility)
	countField := schema.Field(schema.FieldIndices(FieldMetricCount)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, countField.Type)

	// BucketCounts should be list of int64 (converted from uint64 for Iceberg compatibility)
	bucketCountsField := schema.Field(schema.FieldIndices(FieldMetricBucketCounts)[0])
	listType, ok := bucketCountsField.Type.(*arrow.ListType)
	require.True(t, ok)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, listType.Elem())

	// ExplicitBounds should be list of float64
	explicitBoundsField := schema.Field(schema.FieldIndices(FieldMetricExplicitBounds)[0])
	listType, ok = explicitBoundsField.Type.(*arrow.ListType)
	require.True(t, ok)
	assert.Equal(t, arrow.PrimitiveTypes.Float64, listType.Elem())
}

func TestMetricsSumSchema_FieldTypes(t *testing.T) {
	schema := MetricsSumSchema()

	// IsMonotonic should be boolean
	isMonotonicField := schema.Field(schema.FieldIndices(FieldMetricIsMonotonic)[0])
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, isMonotonicField.Type)

	// AggregationTemporality should be string
	aggField := schema.Field(schema.FieldIndices(FieldMetricAggregationTemporality)[0])
	assert.Equal(t, arrow.BinaryTypes.String, aggField.Type)
}

func TestMetricsExponentialHistogramSchema_FieldTypes(t *testing.T) {
	schema := MetricsExponentialHistogramSchema()

	// Scale should be int32
	scaleField := schema.Field(schema.FieldIndices(FieldMetricScale)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int32, scaleField.Type)

	// ZeroCount should be int64 (converted from uint64 for Iceberg compatibility)
	zeroCountField := schema.Field(schema.FieldIndices(FieldMetricZeroCount)[0])
	assert.Equal(t, arrow.PrimitiveTypes.Int64, zeroCountField.Type)

	// PositiveBuckets should be list of int64 (converted from uint64 for Iceberg compatibility)
	posBucketsField := schema.Field(schema.FieldIndices(FieldMetricPositiveBuckets)[0])
	listType, ok := posBucketsField.Type.(*arrow.ListType)
	require.True(t, ok)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, listType.Elem())
}
