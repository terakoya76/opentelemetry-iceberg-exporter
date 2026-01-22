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
type FieldDef struct {
	Name     string
	Type     arrow.DataType
	Nullable bool
}

// convertTypeForIceberg converts Arrow types to Iceberg-compatible types.
// This function does NOT assign field IDs - iceberg-go will use the table's
// name mapping to assign correct field IDs based on field names.
//
// Type conversions (Iceberg does not support unsigned integers):
// - Uint8, Uint16 → Int32 (safe, values fit within signed 32-bit range)
// - Uint32 → Int64 (safe, values fit within signed 64-bit range)
// - Uint64 → Int64 (may overflow for values > 2^63-1, but this is standard practice)
func convertTypeForIceberg(dt arrow.DataType) arrow.DataType {
	switch dt.ID() {
	// Unsigned integer types → signed integer types
	case arrow.UINT8, arrow.UINT16:
		return arrow.PrimitiveTypes.Int32
	case arrow.UINT32, arrow.UINT64:
		return arrow.PrimitiveTypes.Int64

	// Handle list types - recursively convert element type
	case arrow.LIST:
		listType := dt.(*arrow.ListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		return arrow.ListOfField(arrow.Field{
			Name:     "element",
			Type:     convertedElem,
			Nullable: true,
		})

	case arrow.LARGE_LIST:
		listType := dt.(*arrow.LargeListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		return arrow.LargeListOfField(arrow.Field{
			Name:     "element",
			Type:     convertedElem,
			Nullable: true,
		})

	case arrow.FIXED_SIZE_LIST:
		listType := dt.(*arrow.FixedSizeListType)
		convertedElem := convertTypeForIceberg(listType.Elem())
		return arrow.FixedSizeListOfField(listType.Len(), arrow.Field{
			Name:     "element",
			Type:     convertedElem,
			Nullable: true,
		})

	case arrow.MAP:
		mapType := dt.(*arrow.MapType)
		convertedKey := convertTypeForIceberg(mapType.KeyType())
		convertedValue := convertTypeForIceberg(mapType.ItemType())
		return arrow.MapOf(convertedKey, convertedValue)

	case arrow.STRUCT:
		structType := dt.(*arrow.StructType)
		fields := make([]arrow.Field, len(structType.Fields()))
		for i, f := range structType.Fields() {
			convertedType := convertTypeForIceberg(f.Type)
			fields[i] = arrow.Field{
				Name:     f.Name,
				Type:     convertedType,
				Nullable: f.Nullable,
			}
		}
		return arrow.StructOf(fields...)

	default:
		// All other types pass through unchanged
		return dt
	}
}

// buildSchema converts field definitions to an Arrow schema.
// This function does NOT assign field IDs to the Arrow schema.
// iceberg-go will use the table's name mapping to assign correct field IDs
// based on field names when writing to the table.
//
// This approach ensures compatibility with existing Iceberg tables regardless
// of their field ID assignments, matching the behavior of addDataFiles.
func buildSchema(defs []FieldDef, metadata *arrow.Metadata) *arrow.Schema {
	fields := make([]arrow.Field, len(defs))
	for i, def := range defs {
		convertedType := convertTypeForIceberg(def.Type)
		fields[i] = arrow.Field{
			Name:     def.Name,
			Type:     convertedType,
			Nullable: def.Nullable,
		}
	}
	return arrow.NewSchema(fields, metadata)
}

// =============================================================================
// Traces Schema
// =============================================================================

// TracesSchema returns the Arrow schema for OTLP traces with auto-assigned field IDs.
func TracesSchema() *arrow.Schema {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	// the field definitions for OTLP traces.
	tracesFieldDefs := []FieldDef{
		// Span fields in proto order
		{FieldTraceTraceId, arrow.BinaryTypes.String, false},
		{FieldTraceSpanId, arrow.BinaryTypes.String, false},
		{FieldTraceTraceState, arrow.BinaryTypes.String, true},
		{FieldTraceParentSpanId, arrow.BinaryTypes.String, true},
		{FieldTraceSpanFlags, arrow.PrimitiveTypes.Uint32, false},
		{FieldTraceSpanName, arrow.BinaryTypes.String, false},
		{FieldTraceSpanKind, arrow.BinaryTypes.String, false},
		{FieldTraceStartTimeUnixNano, ts, false},
		{FieldTraceEndTimeUnixNano, ts, false},
		{FieldTraceSpanAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldTraceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},

		// Span.events - as lists
		{FieldTraceEventsTimeUnixNano, arrow.ListOf(ts), false},
		{FieldTraceEventsName, arrow.ListOf(arrow.BinaryTypes.String), false},
		{FieldTraceEventsAttributes, arrow.ListOf(arrow.BinaryTypes.String), false}, // JSON encoded
		{FieldTraceEventsDroppedAttributesCount, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false},
		{FieldTraceDroppedEventsCount, arrow.PrimitiveTypes.Uint32, false},

		// Span.links - as lists
		{FieldTraceLinksTraceId, arrow.ListOf(arrow.BinaryTypes.String), false},
		{FieldTraceLinksSpanId, arrow.ListOf(arrow.BinaryTypes.String), false},
		{FieldTraceLinksTraceState, arrow.ListOf(arrow.BinaryTypes.String), false},
		{FieldTraceLinksAttributes, arrow.ListOf(arrow.BinaryTypes.String), false}, // JSON encoded
		{FieldTraceLinksDroppedAttributesCount, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false},
		{FieldTraceLinksFlags, arrow.ListOf(arrow.PrimitiveTypes.Uint32), false},
		{FieldTraceDroppedLinksCount, arrow.PrimitiveTypes.Uint32, false},

		// Span.status
		{FieldTraceStatusCode, arrow.BinaryTypes.String, true},
		{FieldTraceStatusMessage, arrow.BinaryTypes.String, true},

		// Calculated field (not in proto)
		{FieldTraceDuration, arrow.PrimitiveTypes.Int64, false},

		// Resource fields (from ResourceSpans wrapper)
		{FieldServiceName, arrow.BinaryTypes.String, true},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},

		// Scope fields (from ScopeSpans wrapper)
		{FieldScopeName, arrow.BinaryTypes.String, true},
		{FieldScopeVersion, arrow.BinaryTypes.String, true},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},
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

// LogsSchema returns the Arrow schema for OTLP logs with auto-assigned field IDs.
func LogsSchema() *arrow.Schema {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	// the field definitions for OTLP logs.
	logsFieldDefs := []FieldDef{
		// LogRecord fields in proto order
		{FieldLogTimeUnixNano, ts, false},
		{FieldLogSeverityNumber, arrow.PrimitiveTypes.Int32, false},
		{FieldLogSeverityText, arrow.BinaryTypes.String, true},
		{FieldLogBody, arrow.BinaryTypes.String, false},       // JSON encoded
		{FieldLogAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldLogDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},
		{FieldLogFlags, arrow.PrimitiveTypes.Uint32, false},
		{FieldLogTraceId, arrow.BinaryTypes.String, true},
		{FieldLogSpanId, arrow.BinaryTypes.String, true},
		{FieldLogObservedTimeUnixNano, ts, true},
		{FieldLogEventName, arrow.BinaryTypes.String, true},

		// Resource fields (from ResourceLogs wrapper)
		{FieldServiceName, arrow.BinaryTypes.String, true},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},

		// Scope fields (from ScopeLogs wrapper)
		{FieldScopeName, arrow.BinaryTypes.String, true},
		{FieldScopeVersion, arrow.BinaryTypes.String, true},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},
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
func commonMetricFields() []FieldDef {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	return []FieldDef{
		// Metric timestamp (required)
		{FieldMetricTimeUnixNano, ts, false},

		// Resource fields
		{FieldServiceName, arrow.BinaryTypes.String, true},
		{FieldResourceAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldResourceDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},

		// Scope fields
		{FieldScopeName, arrow.BinaryTypes.String, true},
		{FieldScopeVersion, arrow.BinaryTypes.String, true},
		{FieldScopeAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldScopeDroppedAttributesCount, arrow.PrimitiveTypes.Uint32, false},

		// Metric identity
		{FieldMetricName, arrow.BinaryTypes.String, false},
		{FieldMetricDescription, arrow.BinaryTypes.String, true},
		{FieldMetricUnit, arrow.BinaryTypes.String, true},
		{FieldMetricMetadata, arrow.BinaryTypes.String, true}, // JSON encoded

		// Data point common fields
		{FieldMetricAttributes, arrow.BinaryTypes.String, false}, // JSON encoded
		{FieldMetricStartTimeUnixNano, ts, true},
		{FieldMetricFlags, arrow.PrimitiveTypes.Uint32, false},
	}
}

// exemplarFields returns the exemplar fields (shared by gauge, sum, histogram, exp_histogram).
func exemplarFields() []FieldDef {
	// Use microsecond precision for Iceberg v1/v2 compatibility
	ts := arrow.FixedWidthTypes.Timestamp_us

	return []FieldDef{
		{FieldMetricExemplarsTimeUnixNano, arrow.ListOf(ts), true},
		{FieldMetricExemplarsAsDouble, arrow.ListOf(arrow.PrimitiveTypes.Float64), true},
		{FieldMetricExemplarsSpanId, arrow.ListOf(arrow.BinaryTypes.String), true},
		{FieldMetricExemplarsTraceId, arrow.ListOf(arrow.BinaryTypes.String), true},
		{FieldMetricExemplarsAsInt, arrow.ListOf(arrow.PrimitiveTypes.Int64), true},
		{FieldMetricExemplarsFilteredAttributes, arrow.ListOf(arrow.BinaryTypes.String), true}, // JSON encoded
	}
}

// MetricsGaugeSchema returns the Arrow schema for OTLP gauge metrics with auto-assigned field IDs.
// Gauge metrics represent a single numerical value that can arbitrarily go up and down.
func MetricsGaugeSchema() *arrow.Schema {
	fields := commonMetricFields()

	// Gauge-specific fields: value (one of as_double or as_int)
	gaugeFields := []FieldDef{
		{FieldMetricAsDouble, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricAsInt, arrow.PrimitiveTypes.Int64, true},
	}
	fields = append(fields, gaugeFields...)

	// Exemplars
	fields = append(fields, exemplarFields()...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_gauge_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// MetricsSumSchema returns the Arrow schema for OTLP sum (counter) metrics with auto-assigned field IDs.
// Sum metrics represent a cumulative or delta sum of values.
func MetricsSumSchema() *arrow.Schema {
	fields := commonMetricFields()

	// Sum-specific fields: value + aggregation semantics
	sumFields := []FieldDef{
		{FieldMetricAsDouble, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricAsInt, arrow.PrimitiveTypes.Int64, true},
		{FieldMetricIsMonotonic, arrow.FixedWidthTypes.Boolean, false},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false},
	}
	fields = append(fields, sumFields...)

	// Exemplars
	fields = append(fields, exemplarFields()...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_sum_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// MetricsHistogramSchema returns the Arrow schema for OTLP histogram metrics with auto-assigned field IDs.
// Histogram metrics represent a distribution of values with explicit bucket boundaries.
func MetricsHistogramSchema() *arrow.Schema {
	fields := commonMetricFields()

	// Histogram-specific fields
	histogramFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricMin, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricMax, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricBucketCounts, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false},
		{FieldMetricExplicitBounds, arrow.ListOf(arrow.PrimitiveTypes.Float64), false},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false},
	}
	fields = append(fields, histogramFields...)

	fields = append(fields, exemplarFields()...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_histogram_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// MetricsExponentialHistogramSchema returns the Arrow schema for OTLP exponential histogram metrics with auto-assigned field IDs.
// Exponential histograms use a logarithmic scale for bucket boundaries.
func MetricsExponentialHistogramSchema() *arrow.Schema {
	fields := commonMetricFields()

	// ExponentialHistogram-specific fields
	expHistogramFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricMin, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricMax, arrow.PrimitiveTypes.Float64, true},
		{FieldMetricScale, arrow.PrimitiveTypes.Int32, false},
		{FieldMetricZeroCount, arrow.PrimitiveTypes.Uint64, false},
		{FieldMetricZeroThreshold, arrow.PrimitiveTypes.Float64, false},
		{FieldMetricPositiveOffset, arrow.PrimitiveTypes.Int32, false},
		{FieldMetricPositiveBuckets, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false},
		{FieldMetricNegativeOffset, arrow.PrimitiveTypes.Int32, false},
		{FieldMetricNegativeBuckets, arrow.ListOf(arrow.PrimitiveTypes.Uint64), false},
		{FieldMetricAggregationTemporality, arrow.BinaryTypes.String, false},
	}
	fields = append(fields, expHistogramFields...)

	fields = append(fields, exemplarFields()...)

	metadata := arrow.NewMetadata(
		[]string{"iceberg_exporter.metrics_exponential_histogram_schema_version"},
		[]string{"1.0.0"},
	)
	return buildSchema(fields, &metadata)
}

// MetricsSummarySchema returns the Arrow schema for OTLP summary metrics with auto-assigned field IDs.
// Summary metrics represent pre-calculated quantiles (legacy metric type).
func MetricsSummarySchema() *arrow.Schema {
	fields := commonMetricFields()

	// Summary-specific fields
	summaryFields := []FieldDef{
		{FieldMetricCount, arrow.PrimitiveTypes.Uint64, false},
		{FieldMetricSum, arrow.PrimitiveTypes.Float64, false},
		{FieldMetricQuantileValuesQuantile, arrow.ListOf(arrow.PrimitiveTypes.Float64), false},
		{FieldMetricQuantileValuesValue, arrow.ListOf(arrow.PrimitiveTypes.Float64), false},
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
