package arrow

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsConverter_Convert_Gauge(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test.gauge")
	m.SetDescription("A test gauge")
	m.SetUnit("1")
	gauge := m.SetEmptyGauge()

	dp := gauge.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetDoubleValue(42.5)
	dp.Attributes().PutStr("env", "test")

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Only gauge should have data
	assert.NotNil(t, records.Gauge)
	assert.Nil(t, records.Sum)
	assert.Nil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)

	// Verify gauge record
	gaugeRecord := *records.Gauge
	assert.Equal(t, int64(1), gaugeRecord.NumRows())
	assert.Equal(t, int64(23), gaugeRecord.NumCols()) // 15 common + 2 value + 6 exemplars
}

func TestMetricsConverter_Convert_Sum(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test.counter")
	m.SetDescription("A test counter")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(100)

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Only sum should have data
	assert.Nil(t, records.Gauge)
	assert.NotNil(t, records.Sum)
	assert.Nil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)

	// Verify sum record
	sumRecord := *records.Sum
	assert.Equal(t, int64(1), sumRecord.NumRows())
	assert.Equal(t, int64(25), sumRecord.NumCols()) // 15 common + 2 value + 2 sum-specific + 6 exemplars
}

func TestMetricsConverter_Convert_Histogram(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test.histogram")
	histogram := m.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := histogram.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetMin(1.0)
	dp.SetMax(20.0)
	dp.ExplicitBounds().FromRaw([]float64{1.0, 5.0, 10.0})
	dp.BucketCounts().FromRaw([]uint64{2, 5, 2, 1})

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Only histogram should have data
	assert.Nil(t, records.Gauge)
	assert.Nil(t, records.Sum)
	assert.NotNil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)

	// Verify histogram record
	histRecord := *records.Histogram
	assert.Equal(t, int64(1), histRecord.NumRows())
	assert.Equal(t, int64(28), histRecord.NumCols()) // 15 common + 7 histogram (includes agg) + 6 exemplars
}

func TestMetricsConverter_Convert_ExponentialHistogram(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test.exp_histogram")
	expHistogram := m.SetEmptyExponentialHistogram()
	expHistogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := expHistogram.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetScale(2)
	dp.SetZeroCount(1)
	dp.SetZeroThreshold(0.001)
	dp.Positive().SetOffset(0)
	dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
	dp.Negative().SetOffset(0)
	dp.Negative().BucketCounts().FromRaw([]uint64{1, 1})

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Only exponential histogram should have data
	assert.Nil(t, records.Gauge)
	assert.Nil(t, records.Sum)
	assert.Nil(t, records.Histogram)
	assert.NotNil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)

	// Verify exponential histogram record
	expHistRecord := *records.ExponentialHistogram
	assert.Equal(t, int64(1), expHistRecord.NumRows())
	assert.Equal(t, int64(33), expHistRecord.NumCols()) // 15 common + 12 exp-hist (includes agg) + 6 exemplars
}

func TestMetricsConverter_Convert_Summary(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test.summary")
	summary := m.SetEmptySummary()

	dp := summary.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(10)
	dp.SetSum(100.0)

	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.5)
	qv.SetValue(10.0)

	qv2 := dp.QuantileValues().AppendEmpty()
	qv2.SetQuantile(0.99)
	qv2.SetValue(20.0)

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Only summary should have data
	assert.Nil(t, records.Gauge)
	assert.Nil(t, records.Sum)
	assert.Nil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.NotNil(t, records.Summary)

	// Verify summary record (no exemplars for summary)
	summaryRecord := *records.Summary
	assert.Equal(t, int64(1), summaryRecord.NumRows())
	assert.Equal(t, int64(19), summaryRecord.NumCols()) // 15 common + 4 summary
}

func TestMetricsConverter_Convert_Mixed(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()

	// Add gauge
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("test.gauge")
	gauge := m1.SetEmptyGauge()
	dp1 := gauge.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp1.SetDoubleValue(42.5)

	// Add sum
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("test.counter")
	sum := m2.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp2.SetIntValue(100)

	// Add histogram
	m3 := sm.Metrics().AppendEmpty()
	m3.SetName("test.histogram")
	histogram := m3.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp3 := histogram.DataPoints().AppendEmpty()
	dp3.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp3.SetCount(10)
	dp3.ExplicitBounds().FromRaw([]float64{1.0, 5.0})
	dp3.BucketCounts().FromRaw([]uint64{2, 5, 3})

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// All three types should have data
	assert.NotNil(t, records.Gauge)
	assert.NotNil(t, records.Sum)
	assert.NotNil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)

	// Each type should have 1 row
	assert.Equal(t, int64(1), (*records.Gauge).NumRows())
	assert.Equal(t, int64(1), (*records.Sum).NumRows())
	assert.Equal(t, int64(1), (*records.Histogram).NumRows())
}

func TestMetricsConverter_Convert_Empty(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// All should be nil for empty metrics
	assert.Nil(t, records.Gauge)
	assert.Nil(t, records.Sum)
	assert.Nil(t, records.Histogram)
	assert.Nil(t, records.ExponentialHistogram)
	assert.Nil(t, records.Summary)
}

func TestMetricRecords_ForEach(t *testing.T) {
	allocator := memory.NewGoAllocator()
	converter := NewMetricsConverter(allocator)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Add gauge
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("test.gauge")
	gauge := m1.SetEmptyGauge()
	dp1 := gauge.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp1.SetDoubleValue(42.5)

	// Add sum
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("test.counter")
	sum := m2.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp2.SetIntValue(100)

	records, err := converter.Convert(metrics)
	require.NoError(t, err)
	defer records.Release()

	// Track which types were iterated
	visited := make(map[MetricType]bool)
	records.ForEach(func(mt MetricType, _ arrow.RecordBatch) {
		visited[mt] = true
	})

	assert.True(t, visited[MetricTypeGauge])
	assert.True(t, visited[MetricTypeSum])
	assert.False(t, visited[MetricTypeHistogram])
	assert.False(t, visited[MetricTypeExponentialHistogram])
	assert.False(t, visited[MetricTypeSummary])
}

func TestMetricType_SignalType(t *testing.T) {
	tests := []struct {
		metricType MetricType
		expected   string
	}{
		{MetricTypeGauge, "metrics_gauge"},
		{MetricTypeSum, "metrics_sum"},
		{MetricTypeHistogram, "metrics_histogram"},
		{MetricTypeExponentialHistogram, "metrics_exponential_histogram"},
		{MetricTypeSummary, "metrics_summary"},
	}

	for _, tt := range tests {
		t.Run(string(tt.metricType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.metricType.SignalType())
		})
	}
}

func TestGetMetricSchema(t *testing.T) {
	tests := []struct {
		metricType   MetricType
		expectedCols int
	}{
		{MetricTypeGauge, 23},
		{MetricTypeSum, 25},
		{MetricTypeHistogram, 28},
		{MetricTypeExponentialHistogram, 33},
		{MetricTypeSummary, 19},
	}

	for _, tt := range tests {
		t.Run(string(tt.metricType), func(t *testing.T) {
			schema := GetMetricSchema(tt.metricType)
			require.NotNil(t, schema)
			assert.Equal(t, tt.expectedCols, schema.NumFields())
		})
	}
}
