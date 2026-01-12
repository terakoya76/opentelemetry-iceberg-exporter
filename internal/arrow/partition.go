package arrow

import (
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PartitionedBatch represents a RecordBatch partitioned by a specific timestamp.
// Each batch contains only rows that belong to the same partition hour.
type PartitionedBatch struct {
	// Timestamp is the partition timestamp (truncated to hour boundary).
	Timestamp time.Time
	// Batch is the Arrow RecordBatch containing only rows for this partition.
	Batch arrow.RecordBatch
	// RecordCount is the number of records in this batch.
	RecordCount int64
}

// Release releases the underlying RecordBatch.
func (pb *PartitionedBatch) Release() {
	if pb.Batch != nil {
		pb.Batch.Release()
	}
}

// SplitByPartition splits an Arrow RecordBatch into multiple batches based on
// the hour partition of a timestamp column. This ensures each resulting batch
// contains only rows that belong to a single Iceberg partition.
//
// Parameters:
//   - batch: The input RecordBatch to split
//   - timestampColumn: The name of the timestamp column to use for partitioning
//   - allocator: Memory allocator for creating new RecordBatches
//   - timezone: Timezone for partition calculation (e.g., "UTC", "America/New_York")
//
// Returns:
//   - A slice of PartitionedBatch, sorted by timestamp
//   - An error if the timestamp column is not found or has an invalid type
//
// The function groups rows by their epoch-hour (hours since Unix epoch) to match
// Iceberg's HourTransform behavior. Each resulting batch will have consistent
// partition values when registered with Iceberg.
func SplitByPartition(
	batch arrow.RecordBatch,
	timestampColumn string,
	allocator memory.Allocator,
	timezone string,
) ([]PartitionedBatch, error) {
	if batch == nil || batch.NumRows() == 0 {
		return nil, nil
	}

	// Find the timestamp column index
	schema := batch.Schema()
	colIndices := schema.FieldIndices(timestampColumn)
	if len(colIndices) == 0 {
		return nil, fmt.Errorf("timestamp column %q not found in schema", timestampColumn)
	}
	colIdx := colIndices[0]

	// Get the timestamp column
	col := batch.Column(colIdx)
	tsArray, ok := col.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("column %q is not a timestamp type, got %T", timestampColumn, col)
	}

	// Load timezone
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}

	// Get the timestamp unit for conversion
	tsType := tsArray.DataType().(*arrow.TimestampType)
	unit := tsType.Unit

	// Group row indices by partition hour
	partitionGroups := make(map[int64][]int) // epoch-hour -> row indices
	for i := 0; i < int(batch.NumRows()); i++ {
		if tsArray.IsNull(i) {
			// Skip null timestamps - they can't be partitioned
			continue
		}

		// Convert Arrow timestamp value to time.Time
		ts := timestampToTime(tsArray.Value(i), unit, loc)

		// Calculate epoch-hour (hours since Unix epoch)
		epochHour := ts.Unix() / 3600

		partitionGroups[epochHour] = append(partitionGroups[epochHour], i)
	}

	// If all rows belong to the same partition, return the original batch
	if len(partitionGroups) == 1 {
		for epochHour, indices := range partitionGroups {
			if len(indices) == int(batch.NumRows()) {
				// All rows in one partition, retain and return original
				batch.Retain()
				partitionTime := time.Unix(epochHour*3600, 0).In(loc)
				return []PartitionedBatch{
					{
						Timestamp:   partitionTime,
						Batch:       batch,
						RecordCount: batch.NumRows(),
					},
				}, nil
			}
		}
	}

	// Sort epoch-hours for deterministic output order
	epochHours := make([]int64, 0, len(partitionGroups))
	for epochHour := range partitionGroups {
		epochHours = append(epochHours, epochHour)
	}
	sort.Slice(epochHours, func(i, j int) bool { return epochHours[i] < epochHours[j] })

	// Create a separate RecordBatch for each partition
	result := make([]PartitionedBatch, 0, len(epochHours))
	for _, epochHour := range epochHours {
		indices := partitionGroups[epochHour]
		partitionTime := time.Unix(epochHour*3600, 0).In(loc)

		partitionBatch, err := sliceRecordBatch(batch, indices, allocator)
		if err != nil {
			// Release any batches we've already created
			for _, pb := range result {
				pb.Release()
			}
			return nil, fmt.Errorf("failed to create partition batch for hour %d: %w", epochHour, err)
		}

		result = append(result, PartitionedBatch{
			Timestamp:   partitionTime,
			Batch:       partitionBatch,
			RecordCount: int64(len(indices)),
		})
	}

	return result, nil
}

// timestampToTime converts an Arrow timestamp value to time.Time.
func timestampToTime(value arrow.Timestamp, unit arrow.TimeUnit, loc *time.Location) time.Time {
	var t time.Time
	switch unit {
	case arrow.Nanosecond:
		t = time.Unix(0, int64(value))
	case arrow.Microsecond:
		t = time.Unix(0, int64(value)*1000)
	case arrow.Millisecond:
		t = time.Unix(0, int64(value)*1000000)
	case arrow.Second:
		t = time.Unix(int64(value), 0)
	default:
		t = time.Unix(0, int64(value))
	}
	return t.In(loc)
}

// sliceRecordBatch creates a new RecordBatch containing only the specified row indices.
func sliceRecordBatch(batch arrow.RecordBatch, indices []int, allocator memory.Allocator) (arrow.RecordBatch, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("cannot create batch with zero rows")
	}

	schema := batch.Schema()
	numCols := int(batch.NumCols())

	// Build new arrays for each column
	builders := make([]array.Builder, numCols)
	for i := 0; i < numCols; i++ {
		builders[i] = array.NewBuilder(allocator, schema.Field(i).Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	// Copy data for selected indices
	for _, rowIdx := range indices {
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := batch.Column(colIdx)
			if err := appendValue(builders[colIdx], col, rowIdx); err != nil {
				return nil, fmt.Errorf("failed to append value at row %d, col %d: %w", rowIdx, colIdx, err)
			}
		}
	}

	// Build the arrays
	arrays := make([]arrow.Array, numCols)
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}

	// Create the new RecordBatch
	return array.NewRecordBatch(schema, arrays, int64(len(indices))), nil
}

// appendValue appends a single value from an array to a builder.
func appendValue(builder array.Builder, arr arrow.Array, idx int) error {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Append(arr.(*array.String).Value(idx))
	case *array.Int8Builder:
		b.Append(arr.(*array.Int8).Value(idx))
	case *array.Int16Builder:
		b.Append(arr.(*array.Int16).Value(idx))
	case *array.Int32Builder:
		b.Append(arr.(*array.Int32).Value(idx))
	case *array.Int64Builder:
		b.Append(arr.(*array.Int64).Value(idx))
	case *array.Uint8Builder:
		b.Append(arr.(*array.Uint8).Value(idx))
	case *array.Uint16Builder:
		b.Append(arr.(*array.Uint16).Value(idx))
	case *array.Uint32Builder:
		b.Append(arr.(*array.Uint32).Value(idx))
	case *array.Uint64Builder:
		b.Append(arr.(*array.Uint64).Value(idx))
	case *array.Float32Builder:
		b.Append(arr.(*array.Float32).Value(idx))
	case *array.Float64Builder:
		b.Append(arr.(*array.Float64).Value(idx))
	case *array.BooleanBuilder:
		b.Append(arr.(*array.Boolean).Value(idx))
	case *array.TimestampBuilder:
		b.Append(arr.(*array.Timestamp).Value(idx))
	case *array.BinaryBuilder:
		b.Append(arr.(*array.Binary).Value(idx))
	case *array.ListBuilder:
		return appendListValue(b, arr.(*array.List), idx)
	default:
		return fmt.Errorf("unsupported array type: %T", builder)
	}

	return nil
}

// appendListValue appends a list value from a List array to a ListBuilder.
func appendListValue(builder *array.ListBuilder, arr *array.List, idx int) error {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return nil
	}

	builder.Append(true)
	start, end := arr.ValueOffsets(idx)
	valueArr := arr.ListValues()
	valueBuilder := builder.ValueBuilder()

	for i := int(start); i < int(end); i++ {
		if err := appendValue(valueBuilder, valueArr, i); err != nil {
			return err
		}
	}

	return nil
}
