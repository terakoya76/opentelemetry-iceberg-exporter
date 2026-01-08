package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// Field names for OTLP data (OTel proto compatible).
// Comments indicate the source proto definition.
const (
	/**
	 * Common Scope fields (shared across traces, logs, metrics)
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/common/v1/common.proto
	 */

	// InstrumentationScope.name
	FieldScopeName = "scope_name"
	// InstrumentationScope.version
	FieldScopeVersion = "scope_version"
	// InstrumentationScope.attributes
	FieldScopeAttributes = "scope_attributes"
	// InstrumentationScope.dropped_attributes_count
	FieldScopeDroppedAttributesCount = "scope_dropped_attributes_count"

	/**
	 * Common Resource fields (shared across traces, logs, metrics)
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/resource/v1/resource.proto
	 */

	// Resource.attributes["service.name"] (extracted)
	FieldServiceName = "service_name"
	// Resource.attributes
	FieldResourceAttributes = "resource_attributes"
	// Resource.dropped_attributes_count
	FieldResourceDroppedAttributesCount = "resource_dropped_attributes_count"

	/**
	 * Trace-specific fields
	 * Field order matches proto definition order
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/trace/v1/trace.proto
	 */

	// Span.trace_id (field 1)
	FieldTraceTraceId = "trace_id"
	// Span.span_id (field 2)
	FieldTraceSpanId = "span_id"
	// Span.trace_state (field 3)
	FieldTraceTraceState = "trace_state"
	// Span.parent_span_id (field 4)
	FieldTraceParentSpanId = "parent_span_id"
	// Span.flags (field 5)
	FieldTraceSpanFlags = "span_flags"
	// Span.name (field 6)
	FieldTraceSpanName = "span_name"
	// Span.kind (field 7)
	FieldTraceSpanKind = "span_kind"
	// Span.start_time_unix_nano (field 8)
	FieldTraceStartTimeUnixNano = "start_time_unix_nano"
	// Span.end_time_unix_nano (field 9)
	FieldTraceEndTimeUnixNano = "end_time_unix_nano"
	// Span.attributes (field 10)
	FieldTraceSpanAttributes = "span_attributes"
	// Span.dropped_attributes_count (field 11)
	FieldTraceDroppedAttributesCount = "dropped_attributes_count"
	// Span.Event.time_unix_nano (field 12 - events)
	FieldTraceEventsTimeUnixNano = "events_time_unix_nano"
	// Span.Event.name (field 12 - events)
	FieldTraceEventsName = "events_name"
	// Span.Event.attributes (field 12 - events)
	FieldTraceEventsAttributes = "events_attributes"
	// Span.Event.dropped_attributes_count (field 12 - events)
	FieldTraceEventsDroppedAttributesCount = "events_dropped_attributes_count"
	// Span.dropped_events_count (field 13)
	FieldTraceDroppedEventsCount = "dropped_events_count"
	// Span.Link.trace_id (field 14 - links)
	FieldTraceLinksTraceId = "links_trace_id"
	// Span.Link.span_id (field 14 - links)
	FieldTraceLinksSpanId = "links_span_id"
	// Span.Link.trace_state (field 14 - links)
	FieldTraceLinksTraceState = "links_trace_state"
	// Span.Link.attributes (field 14 - links)
	FieldTraceLinksAttributes = "links_attributes"
	// Span.Link.dropped_attributes_count (field 14 - links)
	FieldTraceLinksDroppedAttributesCount = "links_dropped_attributes_count"
	// Span.Link.flags (field 14 - links)
	FieldTraceLinksFlags = "links_flags"
	// Span.dropped_links_count (field 15)
	FieldTraceDroppedLinksCount = "dropped_links_count"
	// Span.Status.code (field 16 - status)
	FieldTraceStatusCode = "status_code"
	// Span.Status.message (field 16 - status)
	FieldTraceStatusMessage = "status_message"
	// Calculated: Span.end_time_unix_nano - Span.start_time_unix_nano (not in proto)
	FieldTraceDuration = "duration"

	/**
	 * Log-specific fields
	 * Field order matches proto definition order
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/logs/v1/logs.proto
	 */

	// LogRecord.time_unix_nano (field 1)
	FieldLogTimeUnixNano = "time_unix_nano"
	// LogRecord.severity_number (field 2)
	FieldLogSeverityNumber = "severity_number"
	// LogRecord.severity_text (field 3)
	FieldLogSeverityText = "severity_text"
	// LogRecord.body (field 5)
	FieldLogBody = "body"
	// LogRecord.attributes (field 6)
	FieldLogAttributes = "log_attributes"
	// LogRecord.dropped_attributes_count (field 7)
	FieldLogDroppedAttributesCount = "dropped_attributes_count"
	// LogRecord.flags (field 8)
	FieldLogFlags = "log_flags"
	// LogRecord.trace_id (field 9)
	FieldLogTraceId = "trace_id"
	// LogRecord.span_id (field 10)
	FieldLogSpanId = "span_id"
	// LogRecord.observed_time_unix_nano (field 11)
	FieldLogObservedTimeUnixNano = "observed_time_unix_nano"
	// LogRecord.event_name (field 12)
	FieldLogEventName = "event_name"

	/**
	 * Metric-specific fields
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/metrics/v1/metrics.proto
	 */

	// NumberDataPoint.time_unix_nano / HistogramDataPoint.time_unix_nano / etc.
	FieldMetricTimeUnixNano = "time_unix_nano"
	// Metric.name
	FieldMetricName = "metric_name"
	// Metric.description
	FieldMetricDescription = "metric_description"
	// Metric.unit
	FieldMetricUnit = "metric_unit"
	// Derived from Metric type (gauge/sum/histogram/summary/exponential_histogram)
	FieldMetricType = "metric_type"
	// Metric.metadata
	FieldMetricMetadata = "metric_metadata"
	// NumberDataPoint.attributes / HistogramDataPoint.attributes / etc.
	FieldMetricAttributes = "attributes"
	// NumberDataPoint.as_double
	FieldMetricAsDouble = "as_double"
	// NumberDataPoint.as_int
	FieldMetricAsInt = "as_int"
	// HistogramDataPoint.count / SummaryDataPoint.count / ExponentialHistogramDataPoint.count
	FieldMetricCount = "count"
	// HistogramDataPoint.sum / SummaryDataPoint.sum / ExponentialHistogramDataPoint.sum
	FieldMetricSum = "sum"
	// HistogramDataPoint.min / ExponentialHistogramDataPoint.min
	FieldMetricMin = "min"
	// HistogramDataPoint.max / ExponentialHistogramDataPoint.max
	FieldMetricMax = "max"
	// HistogramDataPoint.bucket_counts
	FieldMetricBucketCounts = "bucket_counts"
	// HistogramDataPoint.explicit_bounds
	FieldMetricExplicitBounds = "explicit_bounds"
	// SummaryDataPoint.ValueAtQuantile.quantile
	FieldMetricQuantileValuesQuantile = "quantile_values.quantile"
	// SummaryDataPoint.ValueAtQuantile.value
	FieldMetricQuantileValuesValue = "quantile_values.value"
	// NumberDataPoint.flags / HistogramDataPoint.flags / etc.
	FieldMetricFlags = "flags"
	// NumberDataPoint.start_time_unix_nano / HistogramDataPoint.start_time_unix_nano / etc.
	FieldMetricStartTimeUnixNano = "start_time_unix_nano"
	// Exemplar.time_unix_nano (field 2)
	FieldMetricExemplarsTimeUnixNano = "exemplars_time_unix_nano"
	// Exemplar.as_double (field 3)
	FieldMetricExemplarsAsDouble = "exemplars_as_double"
	// Exemplar.span_id (field 4)
	FieldMetricExemplarsSpanId = "exemplars_span_id"
	// Exemplar.trace_id (field 5)
	FieldMetricExemplarsTraceId = "exemplars_trace_id"
	// Exemplar.as_int (field 6)
	FieldMetricExemplarsAsInt = "exemplars_as_int"
	// Exemplar.filtered_attributes (field 7)
	FieldMetricExemplarsFilteredAttributes = "exemplars_filtered_attributes"
	// Sum.is_monotonic
	FieldMetricIsMonotonic = "is_monotonic"
	// Sum.aggregation_temporality / Histogram.aggregation_temporality / ExponentialHistogram.aggregation_temporality
	FieldMetricAggregationTemporality = "aggregation_temporality"

	/**
	 * ExponentialHistogram-specific fields (Metric category)
	 *
	 * https://github.com/open-telemetry/opentelemetry-proto/blob/v1.9.0/opentelemetry/proto/metrics/v1/metrics.proto
	 */

	// ExponentialHistogramDataPoint.scale
	FieldMetricScale = "scale"
	// ExponentialHistogramDataPoint.zero_count
	FieldMetricZeroCount = "zero_count"
	// ExponentialHistogramDataPoint.zero_threshold
	FieldMetricZeroThreshold = "zero_threshold"
	// ExponentialHistogramDataPoint.positive.offset
	FieldMetricPositiveOffset = "positive_offset"
	// ExponentialHistogramDataPoint.positive.bucket_counts
	FieldMetricPositiveBuckets = "positive_buckets"
	// ExponentialHistogramDataPoint.negative.offset
	FieldMetricNegativeOffset = "negative_offset"
	// ExponentialHistogramDataPoint.negative.bucket_counts
	FieldMetricNegativeBuckets = "negative_buckets"
)

// =============================================================================
// Schema Building Infrastructure
// =============================================================================

// FieldDef defines a field with all attributes needed for schema generation.
// This enables defining fields once and generating schemas with or without field IDs.
type FieldDef struct {
	Name     string
	Type     arrow.DataType
	Nullable bool
	FieldID  int
}

// convertTypeForIceberg converts Arrow types to Iceberg-compatible types.
// Iceberg does not support unsigned integers, so we convert:
// - Uint8, Uint16 → Int32 (safe, values fit within signed 32-bit range)
// - Uint32 → Int64 (safe, values fit within signed 64-bit range)
// - Uint64 → Int64 (may overflow for values > 2^63-1, but this is standard practice)
//
// This function recursively handles nested types (List, Map, Struct).
func convertTypeForIceberg(dt arrow.DataType) arrow.DataType {
	switch dt.ID() {
	// Unsigned integer types → signed integer types
	case arrow.UINT8, arrow.UINT16:
		return arrow.PrimitiveTypes.Int32
	case arrow.UINT32, arrow.UINT64:
		return arrow.PrimitiveTypes.Int64

	// Handle nested types recursively
	case arrow.LIST:
		listType := dt.(*arrow.ListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		if convertedElem == listType.Elem() {
			return dt // No change needed
		}
		return arrow.ListOf(convertedElem)

	case arrow.LARGE_LIST:
		listType := dt.(*arrow.LargeListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		if convertedElem == listType.Elem() {
			return dt
		}
		return arrow.LargeListOf(convertedElem)

	case arrow.FIXED_SIZE_LIST:
		listType := dt.(*arrow.FixedSizeListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		if convertedElem == listType.Elem() {
			return dt
		}
		return arrow.FixedSizeListOf(listType.Len(), convertedElem)

	case arrow.MAP:
		mapType := dt.(*arrow.MapType)
		convertedKey := convertTypeForIceberg(mapType.KeyType())
		convertedValue := convertTypeForIceberg(mapType.ItemType())
		if convertedKey == mapType.KeyType() && convertedValue == mapType.ItemType() {
			return dt
		}
		return arrow.MapOf(convertedKey, convertedValue)

	case arrow.STRUCT:
		structType := dt.(*arrow.StructType)
		fields := make([]arrow.Field, len(structType.Fields()))
		changed := false
		for i, f := range structType.Fields() {
			convertedType := convertTypeForIceberg(f.Type)
			if convertedType != f.Type {
				changed = true
			}
			fields[i] = arrow.Field{
				Name:     f.Name,
				Type:     convertedType,
				Nullable: f.Nullable,
				Metadata: f.Metadata,
			}
		}
		if !changed {
			return dt
		}
		return arrow.StructOf(fields...)

	default:
		// All other types pass through unchanged
		return dt
	}
}

// buildSchema converts field definitions to an Arrow schema.
func buildSchema(defs []FieldDef, metadata *arrow.Metadata) *arrow.Schema {
	fields := make([]arrow.Field, len(defs))
	for i, def := range defs {
		fields[i] = arrow.Field{
			Name:     def.Name,
			Type:     convertTypeForIceberg(def.Type),
			Nullable: def.Nullable,
			Metadata: arrow.NewMetadata(
				[]string{"PARQUET:field_id"},
				[]string{itoa(def.FieldID)},
			),
		}
	}
	return arrow.NewSchema(fields, metadata)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	negative := i < 0
	if negative {
		i = -i
	}
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		n--
		buf[n] = '-'
	}
	return string(buf[n:])
}

// =============================================================================
// Traces Schema
// =============================================================================

// TracesSchema returns the Arrow schema for OTLP traces WITH field IDs.
func TracesSchema() *arrow.Schema {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	// the field definitions for OTLP traces.
	tracesFieldDefs := []FieldDef{
		// Span fields in proto order (IDs 1-16)
		{FieldTraceTraceId, arrow.BinaryTypes.String, false, 1},
		{FieldTraceSpanId, arrow.BinaryTypes.String, false, 2},
		{FieldTraceTraceState, arrow.BinaryTypes.String, true, 3},
		{FieldTraceParentSpanId, arrow.BinaryTypes.String, true, 4},
		{FieldTraceSpanFlags, arrow.PrimitiveTypes.Uint32, false, 5},
		{FieldTraceSpanName, arrow.BinaryTypes.String, false, 6},
		{FieldTraceSpanKind, arrow.BinaryTypes.String, false, 7},
		{FieldTraceStartTimeUnixNano, ts, false, 8},
		{FieldTraceEndTimeUnixNano, ts, false, 9},
		{FieldTraceSpanAttributes, arrow.BinaryTypes.String, false, 10}, // JSON encoded
		{FieldTraceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 11},

		// Span.events - as lists
		{FieldTraceEventsTimeUnixNano, arrow.ListOf(ts), false, 12},
		{FieldTraceEventsName, arrow.ListOf(arrow.BinaryTypes.String), false, 13},
		{FieldTraceEventsAttributes, arrow.ListOf(arrow.BinaryTypes.String), false, 14}, // JSON encoded
		{FieldTraceEventsDroppedAttributesCount, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false, 15},
		{FieldTraceDroppedEventsCount, arrow.PrimitiveTypes.Uint32, false, 16},

		// Span.links - as lists
		{FieldTraceLinksTraceId, arrow.ListOf(arrow.BinaryTypes.String), false, 17},
		{FieldTraceLinksSpanId, arrow.ListOf(arrow.BinaryTypes.String), false, 18},
		{FieldTraceLinksTraceState, arrow.ListOf(arrow.BinaryTypes.String), false, 19},
		{FieldTraceLinksAttributes, arrow.ListOf(arrow.BinaryTypes.String), false, 20}, // JSON encoded
		{FieldTraceLinksDroppedAttributesCount, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false, 21},
		{FieldTraceLinksFlags, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false, 22},
		{FieldTraceDroppedLinksCount, arrow.PrimitiveTypes.Uint32, false, 23},

		// Span.status
		{FieldTraceStatusCode, arrow.BinaryTypes.String, true, 24},
		{FieldTraceStatusMessage, arrow.BinaryTypes.String, true, 25},

		// Calculated field (not in proto)
		{FieldTraceDuration, arrow.PrimitiveTypes.Int64, false, 26},

		// Resource fields (from ResourceSpans wrapper)
		{FieldServiceName, arrow.BinaryTypes.String, true, 27},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false, 28}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 29},

		// Scope fields (from ScopeSpans wrapper)
		{FieldScopeName, arrow.BinaryTypes.String, true, 30},
		{FieldScopeVersion, arrow.BinaryTypes.String, true, 31},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false, 32}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 33},
	}

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.traces_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(tracesFieldDefs, &metadata)
}

// =============================================================================
// Logs Schema
// =============================================================================

// LogsSchema returns the Arrow schema for OTLP logs WITH field IDs.
func LogsSchema() *arrow.Schema {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	// the field definitions for OTLP logs.
	logsFieldDefs := []FieldDef{
		// LogRecord fields in proto order
		{FieldLogTimeUnixNano, ts, false, 1},
		{FieldLogSeverityNumber, arrow.PrimitiveTypes.Int32, false, 2},
		{FieldLogSeverityText, arrow.BinaryTypes.String, true, 3},
		{FieldLogBody, arrow.BinaryTypes.String, false, 4},       // JSON encoded
		{FieldLogAttributes, arrow.BinaryTypes.String, false, 5}, // JSON encoded
		{FieldLogDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 6},
		{FieldLogFlags, arrow.PrimitiveTypes.Uint32, false, 7},
		{FieldLogTraceId, arrow.BinaryTypes.String, true, 8},
		{FieldLogSpanId, arrow.BinaryTypes.String, true, 9},
		{FieldLogObservedTimeUnixNano, ts, true, 10},
		{FieldLogEventName, arrow.BinaryTypes.String, true, 11},

		// Resource fields (from ResourceLogs wrapper)
		{FieldServiceName, arrow.BinaryTypes.String, true, 12},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false, 13}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 14},

		// Scope fields (from ScopeLogs wrapper)
		{FieldScopeName, arrow.BinaryTypes.String, true, 15},
		{FieldScopeVersion, arrow.BinaryTypes.String, true, 16},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false, 17}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, 18},
	}
	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.logs_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(logsFieldDefs, &metadata)
}

// =============================================================================
// Metrics Schema
// =============================================================================

// commonMetricFields returns the common fields shared by all metric types.
// These fields are always present regardless of metric type.
func commonMetricFields(startID int) []FieldDef {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	return []FieldDef{
		// Metric timestamp (required)
		{FieldMetricTimeUnixNano, ts, false, startID},

		// Resource fields
		{FieldServiceName, arrow.BinaryTypes.String, true, startID + 1},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false, startID + 2}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, startID + 3},

		// Scope fields
		{FieldScopeName, arrow.BinaryTypes.String, true, startID + 4},
		{FieldScopeVersion, arrow.BinaryTypes.String, true, startID + 5},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false, startID + 6}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false, startID + 7},

		// Metric identity
		{FieldMetricName, arrow.BinaryTypes.String, false, startID + 8},
		{FieldMetricDescription, arrow.BinaryTypes.String, true, startID + 9},
		{FieldMetricUnit, arrow.BinaryTypes.String, true, startID + 10},
		{FieldMetricMetadata, arrow.BinaryTypes.String, true, startID + 11}, // JSON encoded

		// Data point common fields
		{FieldMetricAttributes, arrow.BinaryTypes.String, false, startID + 12}, // JSON encoded
		{FieldMetricStartTimeUnixNano, ts, true, startID + 13},
		{FieldMetricFlags, arrow.PrimitiveTypes.Uint32, false, startID + 14},
	}
}

// exemplarFields returns the exemplar fields (shared by gauge, sum, histogram, exp_histogram).
func exemplarFields(startID int) []FieldDef {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	return []FieldDef{
		{FieldMetricExemplarsTimeUnixNano, arrow.ListOf(ts), true, startID},
		{FieldMetricExemplarsAsDouble, arrow.ListOf(arrow.PrimitiveTypes.Float64), true, startID + 1},
		{FieldMetricExemplarsSpanId, arrow.ListOf(arrow.BinaryTypes.String), true, startID + 2},
		{FieldMetricExemplarsTraceId, arrow.ListOf(arrow.BinaryTypes.String), true, startID + 3},
		{FieldMetricExemplarsAsInt, arrow.ListOf(arrow.PrimitiveTypes.Int64), true, startID + 4},
		{FieldMetricExemplarsFilteredAttributes, arrow.ListOf(arrow.BinaryTypes.String), true, startID + 5}, // JSON encoded
	}
}

// GaugeSchema returns the Arrow schema for OTLP gauge metrics WITH field IDs.
// Gauge metrics represent a single numerical value that can arbitrarily go up and down.
// Fields: common (15) + value (2) + exemplars (6) = 23 fields
func MetricsGaugeSchema() *arrow.Schema {
	fields := commonMetricFields(1)

	// Gauge-specific fields: value (one of as_double or as_int)
	gaugeFields := []FieldDef{
		{FieldMetricAsDouble, arrow.PrimitiveTypes.Float64, true, 16},
		{FieldMetricAsInt, arrow.PrimitiveTypes.Int64, true, 17},
	}
	fields = append(fields, gaugeFields...)

	// Exemplars
	fields = append(fields, exemplarFields(18)...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_gauge_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// SumSchema returns the Arrow schema for OTLP sum (counter) metrics WITH field IDs.
// Sum metrics represent a cumulative or delta sum of values.
// Fields: common (15) + value (2) + sum-specific (2) + exemplars (6) = 25 fields
func MetricsSumSchema() *arrow.Schema {
	fields := commonMetricFields(1)

	// Sum-specific fields: value + aggregation semantics
	sumFields := []FieldDef{
		{FieldMetricAsDouble, arrow.PrimitiveTypes.Float64, true, 16},
		{FieldMetricAsInt, arrow.PrimitiveTypes.Int64, true, 17},
		{FieldMetricIsMonotonic, arrow.FixedWidthTypes.Boolean, false, 18},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false, 19},
	}
	fields = append(fields, sumFields...)

	// Exemplars
	fields = append(fields, exemplarFields(20)...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_sum_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// HistogramSchema returns the Arrow schema for OTLP histogram metrics WITH field IDs.
// Histogram metrics represent a distribution of values with explicit bucket boundaries.
// Fields: common (15) + histogram-specific (7, includes agg_temporality) + exemplars (6) = 28 fields
func MetricsHistogramSchema() *arrow.Schema {
	fields := commonMetricFields(1)

	// Histogram-specific fields
	histogramFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false, 16},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, true, 17},
		{FieldMetricMin, arrow.PrimitiveTypes.Float64, true, 18},
		{FieldMetricMax, arrow.PrimitiveTypes.Float64, true, 19},
		{FieldMetricBucketCounts, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false, 20},
		{FieldMetricExplicitBounds, arrow.ListOf(arrow.PrimitiveTypes.Float64), false, 21},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false, 22},
	}
	fields = append(fields, histogramFields...)

	fields = append(fields, exemplarFields(23)...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_histogram_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// ExponentialHistogramSchema returns the Arrow schema for OTLP exponential histogram metrics WITH field IDs.
// Exponential histograms use a logarithmic scale for bucket boundaries.
// Fields: common (15) + exp-histogram-specific (11) + aggregation (1) + exemplars (6) = 33 fields
func MetricsExponentialHistogramSchema() *arrow.Schema {
	fields := commonMetricFields(1)

	// ExponentialHistogram-specific fields
	expHistogramFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false, 16},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, true, 17},
		{FieldMetricMin, arrow.PrimitiveTypes.Float64, true, 18},
		{FieldMetricMax, arrow.PrimitiveTypes.Float64, true, 19},
		{FieldMetricScale, arrow.PrimitiveTypes.Int32, false, 20},
		{FieldMetricZeroCount, arrow.PrimitiveTypes.Uint64, false, 21},
		{FieldMetricZeroThreshold, arrow.PrimitiveTypes.Float64, false, 22},
		{FieldMetricPositiveOffset, arrow.PrimitiveTypes.Int32, false, 23},
		{FieldMetricPositiveBuckets, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false, 24},
		{FieldMetricNegativeOffset, arrow.PrimitiveTypes.Int32, false, 25},
		{FieldMetricNegativeBuckets, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false, 26},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false, 27},
	}
	fields = append(fields, expHistogramFields...)

	fields = append(fields, exemplarFields(28)...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_exponential_histogram_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// SummarySchema returns the Arrow schema for OTLP summary metrics WITH field IDs.
// Summary metrics represent pre-calculated quantiles (legacy metric type).
// Fields: common (15) + summary-specific (4) = 19 fields
func MetricsSummarySchema() *arrow.Schema {
	fields := commonMetricFields(1)

	// Summary-specific fields
	summaryFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false, 16},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, false, 17},
		{FieldMetricQuantileValuesQuantile, arrow.ListOf(arrow.PrimitiveTypes.Float64), false, 18},
		{FieldMetricQuantileValuesValue, arrow.ListOf(arrow.PrimitiveTypes.Float64), false, 19},
	}
	fields = append(fields, summaryFields...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_summary_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// GetMetricSchema returns the appropriate schema for the given metric type.
func GetMetricSchema(metricType MetricType) *arrow.Schema {
	switch metricType {
	case MetricTypeGauge:
		return MetricsGaugeSchema()
	case MetricTypeSum:
		return MetricsSumSchema()
	case MetricTypeHistogram:
		return MetricsHistogramSchema()
	case MetricTypeExponentialHistogram:
		return MetricsExponentialHistogramSchema()
	case MetricTypeSummary:
		return MetricsSummarySchema()
	default:
		return nil
	}
}
