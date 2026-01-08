package arrow

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Common test fixtures used across tests

const (
	testdataDir = "testdata"
)

var (
	// testTime provides a consistent test timestamp
	testTime = time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	// testTraceID is a sample trace ID for testing
	testTraceID = pcommon.TraceID([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})

	// testSpanID is a sample span ID for testing
	testSpanID = pcommon.SpanID([8]byte{
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	})

	// testParentSpanID is a sample parent span ID for testing
	testParentSpanID = pcommon.SpanID([8]byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	})

	// linkedTraceID is a sample trace ID for link testing
	linkedTraceID = pcommon.TraceID([16]byte{
		0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
		0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0,
	})

	// linkedSpanID is a sample span ID for link testing
	linkedSpanID = pcommon.SpanID([8]byte{
		0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8,
	})

	// zeroTraceID represents an empty trace ID
	zeroTraceID = pcommon.TraceID([16]byte{})

	// zeroSpanID represents an empty span ID
	zeroSpanID = pcommon.SpanID([8]byte{})
)

// =============================================================================
// Shared deterministic test data for golden file tests and unit tests
// These functions create predictable, reusable test data
// =============================================================================

// createDeterministicTraces creates predictable traces for testing
// Used by: golden file tests, round-trip tests, basic conversion tests
func createDeterministicTraces() ptrace.Traces {
	traces := ptrace.NewTraces()

	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	rs.Resource().Attributes().PutStr("deployment.environment", "test")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")
	ss.Scope().SetVersion("1.0.0")

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(testTraceID)
	span.SetSpanID(testSpanID)
	span.SetParentSpanID(testParentSpanID)
	span.SetName("test-span")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutInt("http.status_code", 200)

	event := span.Events().AppendEmpty()
	event.SetName("test-event")
	event.SetTimestamp(pcommon.NewTimestampFromTime(testTime.Add(50 * time.Millisecond)))
	event.Attributes().PutStr("event.type", "test")

	link := span.Links().AppendEmpty()
	link.SetTraceID(linkedTraceID)
	link.SetSpanID(linkedSpanID)
	link.Attributes().PutStr("link.type", "follows")

	span.Status().SetCode(ptrace.StatusCodeOk)
	span.Status().SetMessage("success")

	return traces
}

// createDeterministicLogs creates predictable logs for testing
// Used by: golden file tests, round-trip tests, basic conversion tests
func createDeterministicLogs() plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	rl.Resource().Attributes().PutStr("deployment.environment", "test")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test-scope")
	sl.Scope().SetVersion("1.0.0")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Nanosecond)))
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("Test log message")
	lr.Attributes().PutStr("log.level", "info")
	lr.Attributes().PutInt("request.id", 12345)
	lr.SetTraceID(testTraceID)
	lr.SetSpanID(testSpanID)

	return logs
}

// createDeterministicMetrics creates predictable metrics for testing
// Used by: golden file tests, round-trip tests, basic conversion tests
func createDeterministicMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	rm.Resource().Attributes().PutStr("deployment.environment", "test")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0.0")

	// Gauge metric
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("test.gauge")
	gauge.SetDescription("A test gauge metric")
	gauge.SetUnit("1")
	gdp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	gdp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	gdp.SetDoubleValue(42.5)
	gdp.Attributes().PutStr("host", "localhost")

	// Sum metric
	sum := sm.Metrics().AppendEmpty()
	sum.SetName("test.counter")
	sum.SetDescription("A test counter metric")
	sum.SetUnit("requests")
	s := sum.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	s.SetIsMonotonic(true)
	sdp := s.DataPoints().AppendEmpty()
	sdp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	sdp.SetIntValue(100)
	sdp.Attributes().PutStr("endpoint", "/api/v1")

	return metrics
}

// readParquetAsTable reads a Parquet file and returns an Arrow table
func readParquetAsTable(t *testing.T, path string) arrow.Table {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err, "Failed to open parquet file: %s", path)
	defer func() { _ = f.Close() }()

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err, "Failed to create parquet reader")
	defer func() { _ = reader.Close() }()

	allocator := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, allocator)
	require.NoError(t, err, "Failed to create arrow reader")

	table, err := arrowReader.ReadTable(context.Background())
	require.NoError(t, err, "Failed to read table")

	return table
}

// compareRecordWithTable compares an Arrow record against a table read from parquet
func compareRecordWithTable(t *testing.T, record arrow.RecordBatch, table arrow.Table) {
	t.Helper()

	// Compare row counts
	assert.Equal(t, record.NumRows(), table.NumRows(), "Row count mismatch")
	assert.Equal(t, record.NumCols(), table.NumCols(), "Column count mismatch")

	// Compare schema field names
	for i := 0; i < int(record.NumCols()); i++ {
		recordField := record.Schema().Field(i)
		tableField := table.Schema().Field(i)
		assert.Equal(t, recordField.Name, tableField.Name, "Field name mismatch at index %d", i)
	}

	// Compare non-list column values
	for i := 0; i < int(record.NumCols()); i++ {
		fieldName := record.Schema().Field(i).Name
		recordCol := record.Column(i)

		// Skip list types for detailed comparison (structure verified above)
		if _, isList := record.Schema().Field(i).Type.(*arrow.ListType); isList {
			continue
		}

		// Get table column data
		tableCol := table.Column(i)
		if tableCol.Len() == 0 {
			continue
		}

		chunk := tableCol.Data().Chunk(0)
		for row := 0; row < recordCol.Len(); row++ {
			if recordCol.IsNull(row) {
				assert.True(t, chunk.IsNull(row), "Column %s row %d: expected null in table", fieldName, row)
			} else {
				assert.False(t, chunk.IsNull(row), "Column %s row %d: unexpected null in table", fieldName, row)
				assert.Equal(t, recordCol.ValueStr(row), chunk.ValueStr(row),
					"Column %s row %d value mismatch", fieldName, row)
			}
		}
	}
}

// =============================================================================
// Edge case test data for Logs
// =============================================================================

// createEmptyLogs creates an empty logs structure
func createEmptyLogs() plog.Logs {
	return plog.NewLogs()
}

// createLogsWithNullableFields creates logs with empty optional fields (null values)
func createLogsWithNullableFields() plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	// No service.name - should result in null
	res := rl.Resource()
	res.Attributes().PutStr("other.attr", "value")

	sl := rl.ScopeLogs().AppendEmpty()
	// Empty scope name and version

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	// No severity text - should result in null
	lr.SetSeverityNumber(plog.SeverityNumberWarn)
	lr.Body().SetStr("Warning message")
	// No trace_id or span_id - should result in null

	return logs
}

// createLogsWithComplexBody creates logs with complex JSON body
func createLogsWithComplexBody() plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "complex-service")

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	lr.SetSeverityNumber(plog.SeverityNumberDebug)

	// Create a complex body with map type
	bodyMap := lr.Body().SetEmptyMap()
	bodyMap.PutStr("message", "structured log")
	bodyMap.PutInt("code", 200)
	bodyMap.PutBool("success", true)

	// Nested slice
	nestedSlice := bodyMap.PutEmptySlice("tags")
	nestedSlice.AppendEmpty().SetStr("tag1")
	nestedSlice.AppendEmpty().SetStr("tag2")

	return logs
}

// createLogsWithMultipleResources creates logs from multiple resources and scopes
func createLogsWithMultipleResources() plog.Logs {
	logs := plog.NewLogs()

	// First resource
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "service-a")

	sl1 := rl1.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-a")

	lr1 := sl1.LogRecords().AppendEmpty()
	lr1.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	lr1.SetSeverityNumber(plog.SeverityNumberInfo)
	lr1.Body().SetStr("Log from service A")

	// Second resource
	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "service-b")

	sl2 := rl2.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("scope-b")

	lr2 := sl2.LogRecords().AppendEmpty()
	lr2.SetTimestamp(pcommon.NewTimestampFromTime(testTime.Add(1000)))
	lr2.SetSeverityNumber(plog.SeverityNumberError)
	lr2.Body().SetStr("Log from service B")

	// Multiple logs in second scope
	lr3 := sl2.LogRecords().AppendEmpty()
	lr3.SetTimestamp(pcommon.NewTimestampFromTime(testTime.Add(2000)))
	lr3.SetSeverityNumber(plog.SeverityNumberWarn)
	lr3.Body().SetStr("Another log from service B")

	return logs
}

// =============================================================================
// Edge case test data for Traces
// =============================================================================

// createEmptyTraces creates an empty traces structure
func createEmptyTraces() ptrace.Traces {
	return ptrace.NewTraces()
}

// createTracesWithNullableFields creates traces with empty optional fields
func createTracesWithNullableFields() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	// No service.name

	ss := rs.ScopeSpans().AppendEmpty()
	// Empty scope name/version

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(testTraceID)
	span.SetSpanID(testSpanID)
	// No parent span ID
	span.SetName("minimal-span")
	span.SetKind(ptrace.SpanKindInternal)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))
	// No trace state, no events, no links

	return traces
}

// createTracesWithAllSpanKinds creates traces with all possible span kinds
func createTracesWithAllSpanKinds() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "span-kinds-test")
	ss := rs.ScopeSpans().AppendEmpty()

	spanKinds := []ptrace.SpanKind{
		ptrace.SpanKindUnspecified,
		ptrace.SpanKindInternal,
		ptrace.SpanKindServer,
		ptrace.SpanKindClient,
		ptrace.SpanKindProducer,
		ptrace.SpanKindConsumer,
	}

	for i, kind := range spanKinds {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(testTraceID)
		span.SetSpanID(pcommon.SpanID([8]byte{byte(i), 0, 0, 0, 0, 0, 0, 0}))
		span.SetName(spanKindToString(kind))
		span.SetKind(kind)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))
	}

	return traces
}

// createTracesWithStatusCodes creates traces with all possible status codes
func createTracesWithStatusCodes() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "status-codes-test")
	ss := rs.ScopeSpans().AppendEmpty()

	statusCodes := []ptrace.StatusCode{
		ptrace.StatusCodeUnset,
		ptrace.StatusCodeOk,
		ptrace.StatusCodeError,
	}

	for i, code := range statusCodes {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(testTraceID)
		span.SetSpanID(pcommon.SpanID([8]byte{byte(i), 0, 0, 0, 0, 0, 0, 0}))
		span.SetName("status-test")
		span.SetKind(ptrace.SpanKindInternal)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))
		span.Status().SetCode(code)
		if code == ptrace.StatusCodeError {
			span.Status().SetMessage("Error occurred")
		}
	}

	return traces
}

// createTracesWithMultipleEvents creates a trace with multiple span events
func createTracesWithMultipleEvents() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "events-test")
	ss := rs.ScopeSpans().AppendEmpty()

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(testTraceID)
	span.SetSpanID(testSpanID)
	span.SetName("multi-event-span")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))

	// Add multiple events
	for i := 0; i < 3; i++ {
		event := span.Events().AppendEmpty()
		event.SetTimestamp(pcommon.NewTimestampFromTime(testTime.Add(time.Duration(i*10) * time.Millisecond)))
		event.SetName("event-" + string(rune('a'+i)))
		event.Attributes().PutInt("event.index", int64(i))
	}

	return traces
}

// createTracesWithMultipleLinks creates a trace with multiple span links
func createTracesWithMultipleLinks() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "links-test")
	ss := rs.ScopeSpans().AppendEmpty()

	span := ss.Spans().AppendEmpty()
	span.SetTraceID(testTraceID)
	span.SetSpanID(testSpanID)
	span.SetName("multi-link-span")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(testTime.Add(100 * time.Millisecond)))

	// Add multiple links
	for i := 0; i < 3; i++ {
		link := span.Links().AppendEmpty()
		link.SetTraceID(pcommon.TraceID([16]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
		link.SetSpanID(pcommon.SpanID([8]byte{byte(i), 1, 0, 0, 0, 0, 0, 0}))
		link.Attributes().PutInt("link.index", int64(i))
	}

	return traces
}

// =============================================================================
// Edge case test data for Metrics
// =============================================================================

// createEmptyMetrics creates an empty metrics structure
func createEmptyMetrics() pmetric.Metrics {
	return pmetric.NewMetrics()
}

// createMetricsWithNullableFields creates metrics with empty optional fields
func createMetricsWithNullableFields() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	// No service.name

	sm := rm.ScopeMetrics().AppendEmpty()
	// Empty scope

	m := sm.Metrics().AppendEmpty()
	m.SetName("minimal.gauge")
	// No description, no unit
	gauge := m.SetEmptyGauge()

	dp := gauge.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetDoubleValue(42.0)
	// No attributes, no start timestamp

	return metrics
}

// createMetricsWithGauge creates metrics with gauge data points
func createMetricsWithGauge() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "gauge-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0.0")

	// Double gauge
	m := sm.Metrics().AppendEmpty()
	m.SetName("cpu.usage")
	m.SetDescription("CPU usage percentage")
	m.SetUnit("%")
	gauge := m.SetEmptyGauge()

	dp := gauge.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime.Add(-1 * time.Minute)))
	dp.SetDoubleValue(75.5)
	dp.Attributes().PutStr("cpu", "0")
	dp.Attributes().PutStr("mode", "user")

	// Int gauge
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("memory.used")
	m2.SetDescription("Memory used in bytes")
	m2.SetUnit("By")
	gauge2 := m2.SetEmptyGauge()

	dp2 := gauge2.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp2.SetIntValue(1073741824) // 1 GB
	dp2.Attributes().PutStr("host", "server-1")

	return metrics
}

// createMetricsWithSum creates metrics with sum/counter data points
func createMetricsWithSum() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "sum-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	// Monotonic cumulative sum (counter)
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.requests")
	m.SetDescription("Total HTTP requests")
	m.SetUnit("1")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime.Add(-1 * time.Hour)))
	dp.SetIntValue(12345)
	dp.Attributes().PutStr("method", "GET")
	dp.Attributes().PutStr("status_code", "200")

	// Non-monotonic delta sum
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("temperature.delta")
	m2.SetDescription("Temperature change")
	m2.SetUnit("degC")
	sum2 := m2.SetEmptySum()
	sum2.SetIsMonotonic(false)
	sum2.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp2 := sum2.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp2.SetDoubleValue(-2.5)
	dp2.Attributes().PutStr("location", "outdoor")

	return metrics
}

// createMetricsWithHistogram creates metrics with histogram data points
func createMetricsWithHistogram() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "histogram-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("http.request.duration")
	m.SetDescription("HTTP request duration")
	m.SetUnit("ms")
	hist := m.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := hist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime.Add(-1 * time.Minute)))
	dp.SetCount(100)
	dp.SetSum(5432.1)
	dp.SetMin(10.5)
	dp.SetMax(250.0)
	dp.ExplicitBounds().FromRaw([]float64{10, 25, 50, 100, 250})
	dp.BucketCounts().FromRaw([]uint64{5, 15, 30, 35, 12, 3})
	dp.Attributes().PutStr("method", "POST")

	// Add exemplars
	ex := dp.Exemplars().AppendEmpty()
	ex.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	ex.SetTraceID(testTraceID)
	ex.SetSpanID(testSpanID)
	ex.SetDoubleValue(45.2)
	ex.FilteredAttributes().PutStr("http.route", "/api/users")

	return metrics
}

// createMetricsWithSummary creates metrics with summary data points
func createMetricsWithSummary() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "summary-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("http.request.latency")
	m.SetDescription("HTTP request latency percentiles")
	m.SetUnit("ms")
	summary := m.SetEmptySummary()

	dp := summary.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime.Add(-1 * time.Minute)))
	dp.SetCount(1000)
	dp.SetSum(12345.67)
	dp.Attributes().PutStr("endpoint", "/api/v1/users")

	// Add quantile values
	qv1 := dp.QuantileValues().AppendEmpty()
	qv1.SetQuantile(0.5)
	qv1.SetValue(10.5)

	qv2 := dp.QuantileValues().AppendEmpty()
	qv2.SetQuantile(0.9)
	qv2.SetValue(25.3)

	qv3 := dp.QuantileValues().AppendEmpty()
	qv3.SetQuantile(0.99)
	qv3.SetValue(95.7)

	return metrics
}

// createMetricsWithExponentialHistogram creates metrics with exponential histogram data points
func createMetricsWithExponentialHistogram() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "exp-histogram-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("http.response.time")
	m.SetDescription("HTTP response time exponential histogram")
	m.SetUnit("ms")
	expHist := m.SetEmptyExponentialHistogram()
	expHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := expHist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(testTime.Add(-1 * time.Minute)))
	dp.SetCount(500)
	dp.SetSum(25432.1)
	dp.SetMin(0.5)
	dp.SetMax(500.0)
	dp.SetScale(3)
	dp.SetZeroCount(2)
	dp.SetZeroThreshold(0.001)

	// Positive buckets
	dp.Positive().SetOffset(1)
	dp.Positive().BucketCounts().FromRaw([]uint64{10, 20, 50, 100, 150, 100, 50, 18})

	// Negative buckets (for negative values - rare but possible)
	dp.Negative().SetOffset(0)
	dp.Negative().BucketCounts().FromRaw([]uint64{})

	dp.Attributes().PutStr("http.method", "GET")

	// Add exemplar
	ex := dp.Exemplars().AppendEmpty()
	ex.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	ex.SetTraceID(testTraceID)
	ex.SetSpanID(testSpanID)
	ex.SetIntValue(125)

	return metrics
}

// createMetricsWithMixed creates metrics with multiple metric types in one resource
func createMetricsWithMixed() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	res := rm.Resource()
	res.Attributes().PutStr("service.name", "mixed-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	// Gauge
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("cpu.usage")
	m1.SetUnit("%")
	gauge := m1.SetEmptyGauge()
	dp1 := gauge.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp1.SetDoubleValue(50.0)

	// Sum
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("requests.count")
	m2.SetUnit("1")
	sum := m2.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp2.SetIntValue(1000)

	// Histogram
	m3 := sm.Metrics().AppendEmpty()
	m3.SetName("request.latency")
	m3.SetUnit("ms")
	hist := m3.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp3 := hist.DataPoints().AppendEmpty()
	dp3.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	dp3.SetCount(100)
	dp3.SetSum(1234.5)
	dp3.ExplicitBounds().FromRaw([]float64{10, 50, 100})
	dp3.BucketCounts().FromRaw([]uint64{20, 50, 25, 5})

	return metrics
}
