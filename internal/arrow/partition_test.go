package arrow

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitByPartition_EmptyBatch(t *testing.T) {
	result, err := SplitByPartition(nil, "timestamp", memory.DefaultAllocator, "UTC")
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestSplitByPartition_SinglePartition(t *testing.T) {
	allocator := memory.DefaultAllocator

	// Create a schema with a timestamp column
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Create a batch with all rows in the same hour
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	// Base time: 2026-01-12 09:15:00 UTC
	baseTime := time.Date(2026, 1, 12, 9, 15, 0, 0, time.UTC)

	// Add 3 rows, all within the same hour (09:xx)
	idBuilder := builder.Field(0).(*array.Int64Builder)
	tsBuilder := builder.Field(1).(*array.TimestampBuilder)
	valueBuilder := builder.Field(2).(*array.StringBuilder)

	for i := 0; i < 3; i++ {
		idBuilder.Append(int64(i + 1))
		ts := baseTime.Add(time.Duration(i*10) * time.Minute) // 09:15, 09:25, 09:35
		tsBuilder.Append(arrow.Timestamp(ts.UnixMicro()))
		valueBuilder.Append("test")
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()

	// Split by partition
	result, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	// Should return single partition
	require.Len(t, result, 1)
	assert.Equal(t, int64(3), result[0].RecordCount)
	assert.Equal(t, int64(3), result[0].Batch.NumRows())

	// Verify partition timestamp is at hour boundary
	expectedPartitionTime := time.Date(2026, 1, 12, 9, 0, 0, 0, time.UTC)
	assert.Equal(t, expectedPartitionTime, result[0].Timestamp)
}

func TestSplitByPartition_MultiplePartitions(t *testing.T) {
	allocator := memory.DefaultAllocator

	// Create a schema with a timestamp column
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	tsBuilder := builder.Field(1).(*array.TimestampBuilder)
	valueBuilder := builder.Field(2).(*array.StringBuilder)

	// Add rows spanning two hours:
	// Row 1, 2: 09:45, 09:55 (hour 9)
	// Row 3, 4: 10:05, 10:15 (hour 10)
	timestamps := []time.Time{
		time.Date(2026, 1, 12, 9, 45, 0, 0, time.UTC),
		time.Date(2026, 1, 12, 9, 55, 0, 0, time.UTC),
		time.Date(2026, 1, 12, 10, 5, 0, 0, time.UTC),
		time.Date(2026, 1, 12, 10, 15, 0, 0, time.UTC),
	}

	for i, ts := range timestamps {
		idBuilder.Append(int64(i + 1))
		tsBuilder.Append(arrow.Timestamp(ts.UnixMicro()))
		valueBuilder.Append("test")
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()

	// Split by partition
	result, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	// Should return two partitions
	require.Len(t, result, 2)

	// First partition: hour 9
	assert.Equal(t, int64(2), result[0].RecordCount)
	assert.Equal(t, int64(2), result[0].Batch.NumRows())
	expectedHour9 := time.Date(2026, 1, 12, 9, 0, 0, 0, time.UTC)
	assert.Equal(t, expectedHour9, result[0].Timestamp)

	// Verify IDs in first partition (should be 1, 2)
	idCol0 := result[0].Batch.Column(0).(*array.Int64)
	assert.Equal(t, int64(1), idCol0.Value(0))
	assert.Equal(t, int64(2), idCol0.Value(1))

	// Second partition: hour 10
	assert.Equal(t, int64(2), result[1].RecordCount)
	assert.Equal(t, int64(2), result[1].Batch.NumRows())
	expectedHour10 := time.Date(2026, 1, 12, 10, 0, 0, 0, time.UTC)
	assert.Equal(t, expectedHour10, result[1].Timestamp)

	// Verify IDs in second partition (should be 3, 4)
	idCol1 := result[1].Batch.Column(0).(*array.Int64)
	assert.Equal(t, int64(3), idCol1.Value(0))
	assert.Equal(t, int64(4), idCol1.Value(1))
}

func TestSplitByPartition_ThreePartitions(t *testing.T) {
	allocator := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	tsBuilder := builder.Field(1).(*array.TimestampBuilder)

	// Add rows spanning three hours (out of order to test sorting)
	timestamps := []time.Time{
		time.Date(2026, 1, 12, 11, 30, 0, 0, time.UTC), // hour 11
		time.Date(2026, 1, 12, 9, 30, 0, 0, time.UTC),  // hour 9
		time.Date(2026, 1, 12, 10, 30, 0, 0, time.UTC), // hour 10
		time.Date(2026, 1, 12, 9, 45, 0, 0, time.UTC),  // hour 9
		time.Date(2026, 1, 12, 11, 45, 0, 0, time.UTC), // hour 11
	}

	for i, ts := range timestamps {
		idBuilder.Append(int64(i + 1))
		tsBuilder.Append(arrow.Timestamp(ts.UnixMicro()))
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()

	result, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	// Should return three partitions, sorted by hour
	require.Len(t, result, 3)

	// Partition 1: hour 9 (rows 2, 4 with IDs 2, 4)
	assert.Equal(t, int64(2), result[0].RecordCount)
	idCol0 := result[0].Batch.Column(0).(*array.Int64)
	assert.Equal(t, int64(2), idCol0.Value(0))
	assert.Equal(t, int64(4), idCol0.Value(1))

	// Partition 2: hour 10 (row 3 with ID 3)
	assert.Equal(t, int64(1), result[1].RecordCount)
	idCol1 := result[1].Batch.Column(0).(*array.Int64)
	assert.Equal(t, int64(3), idCol1.Value(0))

	// Partition 3: hour 11 (rows 1, 5 with IDs 1, 5)
	assert.Equal(t, int64(2), result[2].RecordCount)
	idCol2 := result[2].Batch.Column(0).(*array.Int64)
	assert.Equal(t, int64(1), idCol2.Value(0))
	assert.Equal(t, int64(5), idCol2.Value(1))
}

func TestSplitByPartition_InvalidTimestampColumn(t *testing.T) {
	allocator := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).Append(1)

	batch := builder.NewRecordBatch()
	defer batch.Release()

	_, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timestamp column \"timestamp\" not found")
}

func TestSplitByPartition_NonTimestampColumn(t *testing.T) {
	allocator := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.BinaryTypes.String, Nullable: false}, // Not a timestamp!
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).Append("test")

	batch := builder.NewRecordBatch()
	defer batch.Release()

	_, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a timestamp type")
}

func TestSplitByPartition_WithTimezone(t *testing.T) {
	allocator := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	// Time that is 09:30 in JST (UTC+9) but 00:30 UTC
	// This should be partitioned as hour 0 in UTC, but hour 9 in JST
	jst := time.FixedZone("JST", 9*3600)
	ts := time.Date(2026, 1, 12, 9, 30, 0, 0, jst)

	builder.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMicro()))

	batch := builder.NewRecordBatch()
	defer batch.Release()

	// Split with JST timezone
	result, err := SplitByPartition(batch, "timestamp", allocator, "Asia/Tokyo")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	require.Len(t, result, 1)

	// The partition timestamp should be 09:00 JST
	expectedTime := time.Date(2026, 1, 12, 9, 0, 0, 0, jst)
	// Note: time.LoadLocation("Asia/Tokyo") will return JST
	loc, _ := time.LoadLocation("Asia/Tokyo")
	assert.Equal(t, expectedTime.In(loc).Hour(), result[0].Timestamp.Hour())
}

func TestSplitByPartition_WithListColumns(t *testing.T) {
	allocator := memory.DefaultAllocator

	// Create schema with a list column (like traces have for events)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	tsBuilder := builder.Field(1).(*array.TimestampBuilder)
	listBuilder := builder.Field(2).(*array.ListBuilder)
	valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)

	// Row 1: hour 9, tags = ["a", "b"]
	idBuilder.Append(1)
	tsBuilder.Append(arrow.Timestamp(time.Date(2026, 1, 12, 9, 30, 0, 0, time.UTC).UnixMicro()))
	listBuilder.Append(true)
	valueBuilder.Append("a")
	valueBuilder.Append("b")

	// Row 2: hour 10, tags = ["c"]
	idBuilder.Append(2)
	tsBuilder.Append(arrow.Timestamp(time.Date(2026, 1, 12, 10, 30, 0, 0, time.UTC).UnixMicro()))
	listBuilder.Append(true)
	valueBuilder.Append("c")

	batch := builder.NewRecordBatch()
	defer batch.Release()

	result, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	require.Len(t, result, 2)

	// Verify first partition has correct list data
	listCol0 := result[0].Batch.Column(2).(*array.List)
	assert.Equal(t, 1, listCol0.Len())

	// Verify second partition has correct list data
	listCol1 := result[1].Batch.Column(2).(*array.List)
	assert.Equal(t, 1, listCol1.Len())
}

func TestSplitByPartition_WithNullableTimestamps(t *testing.T) {
	allocator := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	tsBuilder := builder.Field(1).(*array.TimestampBuilder)

	// Row 1: has timestamp
	idBuilder.Append(1)
	tsBuilder.Append(arrow.Timestamp(time.Date(2026, 1, 12, 9, 30, 0, 0, time.UTC).UnixMicro()))

	// Row 2: null timestamp (will be skipped in partitioning)
	idBuilder.Append(2)
	tsBuilder.AppendNull()

	// Row 3: has timestamp
	idBuilder.Append(3)
	tsBuilder.Append(arrow.Timestamp(time.Date(2026, 1, 12, 10, 30, 0, 0, time.UTC).UnixMicro()))

	batch := builder.NewRecordBatch()
	defer batch.Release()

	result, err := SplitByPartition(batch, "timestamp", allocator, "UTC")
	require.NoError(t, err)
	defer func() {
		for _, pb := range result {
			pb.Release()
		}
	}()

	// Should have two partitions (null timestamp row is skipped)
	require.Len(t, result, 2)
	assert.Equal(t, int64(1), result[0].RecordCount) // hour 9: row 1
	assert.Equal(t, int64(1), result[1].RecordCount) // hour 10: row 3
}

func TestTimestampToTime(t *testing.T) {
	loc := time.UTC
	baseTime := time.Date(2026, 1, 12, 9, 30, 45, 123456789, loc)

	tests := []struct {
		name     string
		unit     arrow.TimeUnit
		value    arrow.Timestamp
		expected time.Time
	}{
		{
			name:     "Nanosecond",
			unit:     arrow.Nanosecond,
			value:    arrow.Timestamp(baseTime.UnixNano()),
			expected: baseTime,
		},
		{
			name:     "Microsecond",
			unit:     arrow.Microsecond,
			value:    arrow.Timestamp(baseTime.UnixMicro()),
			expected: baseTime.Truncate(time.Microsecond),
		},
		{
			name:     "Millisecond",
			unit:     arrow.Millisecond,
			value:    arrow.Timestamp(baseTime.UnixMilli()),
			expected: baseTime.Truncate(time.Millisecond),
		},
		{
			name:     "Second",
			unit:     arrow.Second,
			value:    arrow.Timestamp(baseTime.Unix()),
			expected: baseTime.Truncate(time.Second),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := timestampToTime(tc.value, tc.unit, loc)
			assert.Equal(t, tc.expected, result)
		})
	}
}
