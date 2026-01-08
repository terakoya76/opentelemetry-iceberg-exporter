package arrow

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricType represents the type of OTLP metric.
type MetricType string

const (
	// MetricTypeGauge represents a gauge metric.
	MetricTypeGauge MetricType = "gauge"
	// MetricTypeSum represents a sum (counter) metric.
	MetricTypeSum MetricType = "sum"
	// MetricTypeHistogram represents a histogram metric.
	MetricTypeHistogram MetricType = "histogram"
	// MetricTypeExponentialHistogram represents an exponential histogram metric.
	MetricTypeExponentialHistogram MetricType = "exponential_histogram"
	// MetricTypeSummary represents a summary metric.
	MetricTypeSummary MetricType = "summary"
)

// AllMetricTypes returns all supported metric types in order.
func AllMetricTypes() []MetricType {
	return []MetricType{
		MetricTypeGauge,
		MetricTypeSum,
		MetricTypeHistogram,
		MetricTypeExponentialHistogram,
		MetricTypeSummary,
	}
}

// String returns the string representation of the metric type.
func (mt MetricType) String() string {
	return string(mt)
}

// SignalType returns the signal type string used for table naming.
// Format: "metrics_{type}" (e.g., "metrics_gauge", "metrics_histogram")
func (mt MetricType) SignalType() string {
	return "metrics_" + string(mt)
}

// TableSuffix returns the suffix used for default table names.
func (mt MetricType) TableSuffix() string {
	return string(mt)
}

// MetricRecords holds the converted Arrow records for each metric type.
// Only types that have data will have non-nil records.
type MetricRecords struct {
	Gauge                *arrow.RecordBatch
	Sum                  *arrow.RecordBatch
	Histogram            *arrow.RecordBatch
	ExponentialHistogram *arrow.RecordBatch
	Summary              *arrow.RecordBatch
}

// Release releases all non-nil records.
func (r *MetricRecords) Release() {
	if r.Gauge != nil {
		(*r.Gauge).Release()
	}
	if r.Sum != nil {
		(*r.Sum).Release()
	}
	if r.Histogram != nil {
		(*r.Histogram).Release()
	}
	if r.ExponentialHistogram != nil {
		(*r.ExponentialHistogram).Release()
	}
	if r.Summary != nil {
		(*r.Summary).Release()
	}
}

// ForEach iterates over all non-nil records with their metric type.
func (r *MetricRecords) ForEach(fn func(MetricType, arrow.RecordBatch)) {
	if r.Gauge != nil {
		fn(MetricTypeGauge, *r.Gauge)
	}
	if r.Sum != nil {
		fn(MetricTypeSum, *r.Sum)
	}
	if r.Histogram != nil {
		fn(MetricTypeHistogram, *r.Histogram)
	}
	if r.ExponentialHistogram != nil {
		fn(MetricTypeExponentialHistogram, *r.ExponentialHistogram)
	}
	if r.Summary != nil {
		fn(MetricTypeSummary, *r.Summary)
	}
}

// MetricsConverter converts OTLP metrics to separate Arrow record batches per metric type.
type MetricsConverter struct {
	allocator memory.Allocator

	// Schemas for each type
	gaugeSchema        *arrow.Schema
	sumSchema          *arrow.Schema
	histogramSchema    *arrow.Schema
	expHistogramSchema *arrow.Schema
	summarySchema      *arrow.Schema
}

// NewMetricsConverter creates a new MetricsConverter.
func NewMetricsConverter(allocator memory.Allocator) *MetricsConverter {
	return &MetricsConverter{
		allocator:          allocator,
		gaugeSchema:        MetricsGaugeSchema(),
		sumSchema:          MetricsSumSchema(),
		histogramSchema:    MetricsHistogramSchema(),
		expHistogramSchema: MetricsExponentialHistogramSchema(),
		summarySchema:      MetricsSummarySchema(),
	}
}

// GetSchema returns the schema for a specific metric type.
func (c *MetricsConverter) GetSchema(metricType MetricType) *arrow.Schema {
	switch metricType {
	case MetricTypeGauge:
		return c.gaugeSchema
	case MetricTypeSum:
		return c.sumSchema
	case MetricTypeHistogram:
		return c.histogramSchema
	case MetricTypeExponentialHistogram:
		return c.expHistogramSchema
	case MetricTypeSummary:
		return c.summarySchema
	default:
		return nil
	}
}

// Convert converts OTLP metrics to separate Arrow record batches per metric type.
// Returns MetricRecords with non-nil records only for types that have data.
func (c *MetricsConverter) Convert(metrics pmetric.Metrics) (*MetricRecords, error) {
	// Create builders for each type
	gaugeBuilder := array.NewRecordBuilder(c.allocator, c.gaugeSchema)
	sumBuilder := array.NewRecordBuilder(c.allocator, c.sumSchema)
	histogramBuilder := array.NewRecordBuilder(c.allocator, c.histogramSchema)
	expHistogramBuilder := array.NewRecordBuilder(c.allocator, c.expHistogramSchema)
	summaryBuilder := array.NewRecordBuilder(c.allocator, c.summarySchema)

	defer gaugeBuilder.Release()
	defer sumBuilder.Release()
	defer histogramBuilder.Release()
	defer expHistogramBuilder.Release()
	defer summaryBuilder.Release()

	// Track counts for each type
	var gaugeCnt, sumCnt, histogramCnt, expHistogramCnt, summaryCnt int

	// Iterate through all metrics
	resourceMetrics := metrics.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		resource := rm.Resource()
		serviceName := extractServiceName(resource)
		resourceAttrs := attributesToJSON(resource.Attributes())
		resourceDroppedAttrsCount := resource.DroppedAttributesCount()

		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			scope := sm.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeAttrs := attributesToJSON(scope.Attributes())
			scopeDroppedAttrsCount := scope.DroppedAttributesCount()

			metricsSlice := sm.Metrics()
			for k := 0; k < metricsSlice.Len(); k++ {
				metric := metricsSlice.At(k)
				name := metric.Name()
				description := metric.Description()
				unit := metric.Unit()
				metadata := metadataToJSON(metric.Metadata())

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					cnt := c.appendGauge(gaugeBuilder, metric.Gauge(),
						name, description, unit, metadata,
						serviceName, resourceAttrs, resourceDroppedAttrsCount,
						scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount)
					gaugeCnt += cnt

				case pmetric.MetricTypeSum:
					cnt := c.appendSum(sumBuilder, metric.Sum(),
						name, description, unit, metadata,
						serviceName, resourceAttrs, resourceDroppedAttrsCount,
						scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount)
					sumCnt += cnt

				case pmetric.MetricTypeHistogram:
					cnt := c.appendHistogram(histogramBuilder, metric.Histogram(),
						name, description, unit, metadata,
						serviceName, resourceAttrs, resourceDroppedAttrsCount,
						scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount)
					histogramCnt += cnt

				case pmetric.MetricTypeExponentialHistogram:
					cnt := c.appendExponentialHistogram(expHistogramBuilder, metric.ExponentialHistogram(),
						name, description, unit, metadata,
						serviceName, resourceAttrs, resourceDroppedAttrsCount,
						scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount)
					expHistogramCnt += cnt

				case pmetric.MetricTypeSummary:
					cnt := c.appendSummary(summaryBuilder, metric.Summary(),
						name, description, unit, metadata,
						serviceName, resourceAttrs, resourceDroppedAttrsCount,
						scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount)
					summaryCnt += cnt
				}
			}
		}
	}

	// Build records only for types that have data
	result := &MetricRecords{}

	if gaugeCnt > 0 {
		rec := gaugeBuilder.NewRecordBatch()
		result.Gauge = &rec
	}
	if sumCnt > 0 {
		rec := sumBuilder.NewRecordBatch()
		result.Sum = &rec
	}
	if histogramCnt > 0 {
		rec := histogramBuilder.NewRecordBatch()
		result.Histogram = &rec
	}
	if expHistogramCnt > 0 {
		rec := expHistogramBuilder.NewRecordBatch()
		result.ExponentialHistogram = &rec
	}
	if summaryCnt > 0 {
		rec := summaryBuilder.NewRecordBatch()
		result.Summary = &rec
	}

	return result, nil
}

// =============================================================================
// Field indices for each metric type schema
// =============================================================================

// Common field indices (shared by all metric types)
const (
	idxSepTimeUnixNano = iota
	idxSepServiceName
	idxSepResourceAttrs
	idxSepResourceDroppedAttrsCount
	idxSepScopeName
	idxSepScopeVersion
	idxSepScopeAttrs
	idxSepScopeDroppedAttrsCount
	idxSepMetricName
	idxSepDescription
	idxSepUnit
	idxSepMetadata
	idxSepAttributes
	idxSepStartTimeUnixNano
	idxSepFlags
	// Type-specific fields start at 15
)

// Gauge-specific indices (after common fields)
const (
	idxGaugeAsDouble = 15 + iota
	idxGaugeAsInt
	idxGaugeExemplarsTimeUnixNano
	idxGaugeExemplarsAsDouble
	idxGaugeExemplarsSpanId
	idxGaugeExemplarsTraceId
	idxGaugeExemplarsAsInt
	idxGaugeExemplarsFilteredAttrs
)

// Sum-specific indices
const (
	idxSumAsDouble = 15 + iota
	idxSumAsInt
	idxSumIsMonotonic
	idxSumAggTemporality
	idxSumExemplarsTimeUnixNano
	idxSumExemplarsAsDouble
	idxSumExemplarsSpanId
	idxSumExemplarsTraceId
	idxSumExemplarsAsInt
	idxSumExemplarsFilteredAttrs
)

// Histogram-specific indices
const (
	idxHistCount = 15 + iota
	idxHistSum
	idxHistMin
	idxHistMax
	idxHistBucketCounts
	idxHistExplicitBounds
	idxHistAggTemporality
	idxHistExemplarsTimeUnixNano
	idxHistExemplarsAsDouble
	idxHistExemplarsSpanId
	idxHistExemplarsTraceId
	idxHistExemplarsAsInt
	idxHistExemplarsFilteredAttrs
)

// ExponentialHistogram-specific indices
const (
	idxExpHistCount = 15 + iota
	idxExpHistSum
	idxExpHistMin
	idxExpHistMax
	idxExpHistScale
	idxExpHistZeroCount
	idxExpHistZeroThreshold
	idxExpHistPositiveOffset
	idxExpHistPositiveBuckets
	idxExpHistNegativeOffset
	idxExpHistNegativeBuckets
	idxExpHistAggTemporality
	idxExpHistExemplarsTimeUnixNano
	idxExpHistExemplarsAsDouble
	idxExpHistExemplarsSpanId
	idxExpHistExemplarsTraceId
	idxExpHistExemplarsAsInt
	idxExpHistExemplarsFilteredAttrs
)

// Summary-specific indices
const (
	idxSummaryCount = 15 + iota
	idxSummarySum
	idxSummaryQuantileValuesQuantile
	idxSummaryQuantileValuesValue
)

// =============================================================================
// Common field appenders
// =============================================================================

func (c *MetricsConverter) appendCommonFields(
	builder *array.RecordBuilder,
	timestamp int64,
	serviceName string,
	resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName string,
	scopeVersion string,
	scopeAttrs string,
	scopeDroppedAttrsCount uint32,
	name string,
	description string,
	unit string,
	metadata string,
	attrs string,
	startTimestamp int64,
	flags uint32,
) {
	// TimeUnixNano
	builder.Field(idxSepTimeUnixNano).(*array.TimestampBuilder).Append(arrow.Timestamp(timestamp))

	// ServiceName
	appendNullableString(builder.Field(idxSepServiceName).(*array.StringBuilder), serviceName)

	// ResourceAttributes
	builder.Field(idxSepResourceAttrs).(*array.StringBuilder).Append(resourceAttrs)
	builder.Field(idxSepResourceDroppedAttrsCount).(*array.Int64Builder).Append(int64(resourceDroppedAttrsCount))

	// ScopeName
	appendNullableString(builder.Field(idxSepScopeName).(*array.StringBuilder), scopeName)

	// ScopeVersion
	appendNullableString(builder.Field(idxSepScopeVersion).(*array.StringBuilder), scopeVersion)

	// ScopeAttributes
	builder.Field(idxSepScopeAttrs).(*array.StringBuilder).Append(scopeAttrs)
	builder.Field(idxSepScopeDroppedAttrsCount).(*array.Int64Builder).Append(int64(scopeDroppedAttrsCount))

	// Metric identity
	builder.Field(idxSepMetricName).(*array.StringBuilder).Append(name)
	appendNullableString(builder.Field(idxSepDescription).(*array.StringBuilder), description)
	appendNullableString(builder.Field(idxSepUnit).(*array.StringBuilder), unit)
	appendNullableMetadata(builder.Field(idxSepMetadata).(*array.StringBuilder), metadata)

	// Attributes
	builder.Field(idxSepAttributes).(*array.StringBuilder).Append(attrs)

	// StartTimeUnixNano
	if startTimestamp > 0 {
		builder.Field(idxSepStartTimeUnixNano).(*array.TimestampBuilder).Append(arrow.Timestamp(startTimestamp))
	} else {
		builder.Field(idxSepStartTimeUnixNano).(*array.TimestampBuilder).AppendNull()
	}

	// Flags
	builder.Field(idxSepFlags).(*array.Int64Builder).Append(int64(flags))
}

func appendNullableString(builder *array.StringBuilder, value string) {
	if value != "" {
		builder.Append(value)
	} else {
		builder.AppendNull()
	}
}

func appendNullableMetadata(builder *array.StringBuilder, metadata string) {
	if metadata != "" && metadata != "{}" {
		builder.Append(metadata)
	} else {
		builder.AppendNull()
	}
}

// appendExemplars appends exemplar data at the specified field index offset.
func appendExemplars(builder *array.RecordBuilder, exemplars pmetric.ExemplarSlice, startIdx int) {
	timeUnixNanoBuilder := builder.Field(startIdx).(*array.ListBuilder)
	asDoubleBuilder := builder.Field(startIdx + 1).(*array.ListBuilder)
	spanIdBuilder := builder.Field(startIdx + 2).(*array.ListBuilder)
	traceIdBuilder := builder.Field(startIdx + 3).(*array.ListBuilder)
	asIntBuilder := builder.Field(startIdx + 4).(*array.ListBuilder)
	filteredAttrsBuilder := builder.Field(startIdx + 5).(*array.ListBuilder)

	if exemplars.Len() == 0 {
		timeUnixNanoBuilder.AppendNull()
		asDoubleBuilder.AppendNull()
		spanIdBuilder.AppendNull()
		traceIdBuilder.AppendNull()
		asIntBuilder.AppendNull()
		filteredAttrsBuilder.AppendNull()
		return
	}

	timeUnixNanoBuilder.Append(true)
	asDoubleBuilder.Append(true)
	spanIdBuilder.Append(true)
	traceIdBuilder.Append(true)
	asIntBuilder.Append(true)
	filteredAttrsBuilder.Append(true)

	for i := 0; i < exemplars.Len(); i++ {
		ex := exemplars.At(i)
		timeUnixNanoBuilder.ValueBuilder().(*array.TimestampBuilder).Append(
			arrow.Timestamp(ex.Timestamp().AsTime().UnixMicro()),
		)
		spanIdBuilder.ValueBuilder().(*array.StringBuilder).Append(spanIDToString(ex.SpanID()))
		traceIdBuilder.ValueBuilder().(*array.StringBuilder).Append(traceIDToString(ex.TraceID()))

		switch ex.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			asDoubleBuilder.ValueBuilder().(*array.Float64Builder).Append(ex.DoubleValue())
			asIntBuilder.ValueBuilder().(*array.Int64Builder).Append(0)
		case pmetric.ExemplarValueTypeInt:
			asDoubleBuilder.ValueBuilder().(*array.Float64Builder).Append(0)
			asIntBuilder.ValueBuilder().(*array.Int64Builder).Append(ex.IntValue())
		default:
			asDoubleBuilder.ValueBuilder().(*array.Float64Builder).Append(0)
			asIntBuilder.ValueBuilder().(*array.Int64Builder).Append(0)
		}

		filteredAttrsBuilder.ValueBuilder().(*array.StringBuilder).Append(attributesToJSON(ex.FilteredAttributes()))
	}
}

// =============================================================================
// Type-specific appenders
// =============================================================================

func (c *MetricsConverter) appendGauge(
	builder *array.RecordBuilder,
	gauge pmetric.Gauge,
	name, description, unit, metadata, serviceName, resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName, scopeVersion, scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) int {
	dataPoints := gauge.DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		c.appendCommonFields(
			builder,
			dp.Timestamp().AsTime().UnixMicro(),
			serviceName, resourceAttrs, resourceDroppedAttrsCount,
			scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount,
			name, description, unit, metadata,
			attributesToJSON(dp.Attributes()),
			dp.StartTimestamp().AsTime().UnixMicro(),
			uint32(dp.Flags()),
		)

		// Gauge value
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			builder.Field(idxGaugeAsDouble).(*array.Float64Builder).Append(dp.DoubleValue())
			builder.Field(idxGaugeAsInt).(*array.Int64Builder).AppendNull()
		case pmetric.NumberDataPointValueTypeInt:
			builder.Field(idxGaugeAsDouble).(*array.Float64Builder).AppendNull()
			builder.Field(idxGaugeAsInt).(*array.Int64Builder).Append(dp.IntValue())
		default:
			builder.Field(idxGaugeAsDouble).(*array.Float64Builder).AppendNull()
			builder.Field(idxGaugeAsInt).(*array.Int64Builder).AppendNull()
		}

		// Exemplars
		appendExemplars(builder, dp.Exemplars(), idxGaugeExemplarsTimeUnixNano)
	}
	return dataPoints.Len()
}

func (c *MetricsConverter) appendSum(
	builder *array.RecordBuilder,
	sum pmetric.Sum,
	name, description, unit, metadata, serviceName, resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName, scopeVersion, scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) int {
	dataPoints := sum.DataPoints()
	isMonotonic := sum.IsMonotonic()
	aggTemporality := aggregationTemporalityToString(sum.AggregationTemporality())

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		c.appendCommonFields(
			builder,
			dp.Timestamp().AsTime().UnixMicro(),
			serviceName, resourceAttrs, resourceDroppedAttrsCount,
			scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount,
			name, description, unit, metadata,
			attributesToJSON(dp.Attributes()),
			dp.StartTimestamp().AsTime().UnixMicro(),
			uint32(dp.Flags()),
		)

		// Sum value
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			builder.Field(idxSumAsDouble).(*array.Float64Builder).Append(dp.DoubleValue())
			builder.Field(idxSumAsInt).(*array.Int64Builder).AppendNull()
		case pmetric.NumberDataPointValueTypeInt:
			builder.Field(idxSumAsDouble).(*array.Float64Builder).AppendNull()
			builder.Field(idxSumAsInt).(*array.Int64Builder).Append(dp.IntValue())
		default:
			builder.Field(idxSumAsDouble).(*array.Float64Builder).AppendNull()
			builder.Field(idxSumAsInt).(*array.Int64Builder).AppendNull()
		}

		// Sum-specific fields
		builder.Field(idxSumIsMonotonic).(*array.BooleanBuilder).Append(isMonotonic)
		builder.Field(idxSumAggTemporality).(*array.StringBuilder).Append(aggTemporality)

		// Exemplars
		appendExemplars(builder, dp.Exemplars(), idxSumExemplarsTimeUnixNano)
	}
	return dataPoints.Len()
}

func (c *MetricsConverter) appendHistogram(
	builder *array.RecordBuilder,
	histogram pmetric.Histogram,
	name, description, unit, metadata, serviceName, resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName, scopeVersion, scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) int {
	dataPoints := histogram.DataPoints()
	aggTemporality := aggregationTemporalityToString(histogram.AggregationTemporality())

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		c.appendCommonFields(
			builder,
			dp.Timestamp().AsTime().UnixMicro(),
			serviceName, resourceAttrs, resourceDroppedAttrsCount,
			scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount,
			name, description, unit, metadata,
			attributesToJSON(dp.Attributes()),
			dp.StartTimestamp().AsTime().UnixMicro(),
			uint32(dp.Flags()),
		)

		// Histogram-specific fields
		builder.Field(idxHistCount).(*array.Int64Builder).Append(int64(dp.Count()))

		if dp.HasSum() {
			builder.Field(idxHistSum).(*array.Float64Builder).Append(dp.Sum())
		} else {
			builder.Field(idxHistSum).(*array.Float64Builder).AppendNull()
		}

		if dp.HasMin() {
			builder.Field(idxHistMin).(*array.Float64Builder).Append(dp.Min())
		} else {
			builder.Field(idxHistMin).(*array.Float64Builder).AppendNull()
		}

		if dp.HasMax() {
			builder.Field(idxHistMax).(*array.Float64Builder).Append(dp.Max())
		} else {
			builder.Field(idxHistMax).(*array.Float64Builder).AppendNull()
		}

		// BucketCounts
		bucketCounts := dp.BucketCounts()
		bucketCountsBuilder := builder.Field(idxHistBucketCounts).(*array.ListBuilder)
		bucketCountsBuilder.Append(true)
		for j := 0; j < bucketCounts.Len(); j++ {
			bucketCountsBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(bucketCounts.At(j)))
		}

		// ExplicitBounds
		explicitBounds := dp.ExplicitBounds()
		explicitBoundsBuilder := builder.Field(idxHistExplicitBounds).(*array.ListBuilder)
		explicitBoundsBuilder.Append(true)
		for j := 0; j < explicitBounds.Len(); j++ {
			explicitBoundsBuilder.ValueBuilder().(*array.Float64Builder).Append(explicitBounds.At(j))
		}

		// Aggregation temporality
		builder.Field(idxHistAggTemporality).(*array.StringBuilder).Append(aggTemporality)

		// Exemplars
		appendExemplars(builder, dp.Exemplars(), idxHistExemplarsTimeUnixNano)
	}
	return dataPoints.Len()
}

func (c *MetricsConverter) appendExponentialHistogram(
	builder *array.RecordBuilder,
	expHistogram pmetric.ExponentialHistogram,
	name, description, unit, metadata, serviceName, resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName, scopeVersion, scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) int {
	dataPoints := expHistogram.DataPoints()
	aggTemporality := aggregationTemporalityToString(expHistogram.AggregationTemporality())

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		c.appendCommonFields(
			builder,
			dp.Timestamp().AsTime().UnixMicro(),
			serviceName, resourceAttrs, resourceDroppedAttrsCount,
			scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount,
			name, description, unit, metadata,
			attributesToJSON(dp.Attributes()),
			dp.StartTimestamp().AsTime().UnixMicro(),
			uint32(dp.Flags()),
		)

		// ExpHistogram-specific fields
		builder.Field(idxExpHistCount).(*array.Int64Builder).Append(int64(dp.Count()))

		if dp.HasSum() {
			builder.Field(idxExpHistSum).(*array.Float64Builder).Append(dp.Sum())
		} else {
			builder.Field(idxExpHistSum).(*array.Float64Builder).AppendNull()
		}

		if dp.HasMin() {
			builder.Field(idxExpHistMin).(*array.Float64Builder).Append(dp.Min())
		} else {
			builder.Field(idxExpHistMin).(*array.Float64Builder).AppendNull()
		}

		if dp.HasMax() {
			builder.Field(idxExpHistMax).(*array.Float64Builder).Append(dp.Max())
		} else {
			builder.Field(idxExpHistMax).(*array.Float64Builder).AppendNull()
		}

		builder.Field(idxExpHistScale).(*array.Int32Builder).Append(dp.Scale())
		builder.Field(idxExpHistZeroCount).(*array.Int64Builder).Append(int64(dp.ZeroCount()))
		builder.Field(idxExpHistZeroThreshold).(*array.Float64Builder).Append(dp.ZeroThreshold())

		// Positive buckets
		positive := dp.Positive()
		builder.Field(idxExpHistPositiveOffset).(*array.Int32Builder).Append(positive.Offset())

		positiveBucketsBuilder := builder.Field(idxExpHistPositiveBuckets).(*array.ListBuilder)
		positiveBucketCounts := positive.BucketCounts()
		positiveBucketsBuilder.Append(true)
		for j := 0; j < positiveBucketCounts.Len(); j++ {
			positiveBucketsBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(positiveBucketCounts.At(j)))
		}

		// Negative buckets
		negative := dp.Negative()
		builder.Field(idxExpHistNegativeOffset).(*array.Int32Builder).Append(negative.Offset())

		negativeBucketsBuilder := builder.Field(idxExpHistNegativeBuckets).(*array.ListBuilder)
		negativeBucketCounts := negative.BucketCounts()
		negativeBucketsBuilder.Append(true)
		for j := 0; j < negativeBucketCounts.Len(); j++ {
			negativeBucketsBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(negativeBucketCounts.At(j)))
		}

		// Aggregation temporality
		builder.Field(idxExpHistAggTemporality).(*array.StringBuilder).Append(aggTemporality)

		// Exemplars
		appendExemplars(builder, dp.Exemplars(), idxExpHistExemplarsTimeUnixNano)
	}
	return dataPoints.Len()
}

func (c *MetricsConverter) appendSummary(
	builder *array.RecordBuilder,
	summary pmetric.Summary,
	name, description, unit, metadata, serviceName, resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName, scopeVersion, scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) int {
	dataPoints := summary.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		c.appendCommonFields(
			builder,
			dp.Timestamp().AsTime().UnixMicro(),
			serviceName, resourceAttrs, resourceDroppedAttrsCount,
			scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount,
			name, description, unit, metadata,
			attributesToJSON(dp.Attributes()),
			dp.StartTimestamp().AsTime().UnixMicro(),
			uint32(dp.Flags()),
		)

		// Summary-specific fields
		builder.Field(idxSummaryCount).(*array.Int64Builder).Append(int64(dp.Count()))
		builder.Field(idxSummarySum).(*array.Float64Builder).Append(dp.Sum())

		// QuantileValues
		quantileValues := dp.QuantileValues()
		quantileBuilder := builder.Field(idxSummaryQuantileValuesQuantile).(*array.ListBuilder)
		valueBuilder := builder.Field(idxSummaryQuantileValuesValue).(*array.ListBuilder)

		quantileBuilder.Append(true)
		valueBuilder.Append(true)

		for j := 0; j < quantileValues.Len(); j++ {
			qv := quantileValues.At(j)
			quantileBuilder.ValueBuilder().(*array.Float64Builder).Append(qv.Quantile())
			valueBuilder.ValueBuilder().(*array.Float64Builder).Append(qv.Value())
		}
	}
	return dataPoints.Len()
}

// metadataToJSON converts metric metadata to a JSON string.
func metadataToJSON(metadata pcommon.Map) string {
	m := make(map[string]any)
	metadata.Range(func(k string, v pcommon.Value) bool {
		m[k] = valueToInterface(v)
		return true
	})
	data, _ := json.Marshal(m)
	return string(data)
}

// aggregationTemporalityToString converts an AggregationTemporality to its string representation.
func aggregationTemporalityToString(at pmetric.AggregationTemporality) string {
	switch at {
	case pmetric.AggregationTemporalityUnspecified:
		return "UNSPECIFIED"
	case pmetric.AggregationTemporalityDelta:
		return "DELTA"
	case pmetric.AggregationTemporalityCumulative:
		return "CUMULATIVE"
	default:
		return "UNKNOWN"
	}
}
