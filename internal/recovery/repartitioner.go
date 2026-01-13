package recovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/logger"
)

// CrossPartitionError is the error substring that indicates a file spans multiple partitions.
// https://github.com/apache/iceberg-go/blob/v0.4.0/table/internal/utils.go#L229-L232
const CrossPartitionError = "more than one value for partition field"

// Repartitioner handles re-partitioning of files that span multiple partitions.
type Repartitioner struct {
	fileIO        iceberg.FileIO
	allocator     memory.Allocator
	timezone      string
	compression   string
	pathGenerator *iceberg.PathGenerator
	logger        *logger.VerboseLogger
}

// NewRepartitioner creates a new Repartitioner.
func NewRepartitioner(
	fileIO iceberg.FileIO,
	timezone string,
	compression string,
	vlogger *logger.VerboseLogger,
) (*Repartitioner, error) {
	// Create path generator with hourly granularity (repartitioner always uses hourly)
	pathGen, err := iceberg.NewPathGenerator(iceberg.PathConfig{
		Granularity: constants.GranularityHourly,
		Timezone:    timezone,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create path generator: %w", err)
	}

	return &Repartitioner{
		fileIO:        fileIO,
		allocator:     memory.NewGoAllocator(),
		timezone:      timezone,
		compression:   compression,
		pathGenerator: pathGen,
		logger:        vlogger,
	}, nil
}

// RepartitionedFile represents a file that was created from re-partitioning.
type RepartitionedFile struct {
	// DataFile is the new file's metadata
	DataFile DataFile
	// OriginalPath is the path of the original file that was re-partitioned
	OriginalPath string
}

// IsCrossPartitionError checks if an error indicates a cross-partition file.
func IsCrossPartitionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), CrossPartitionError)
}

// Repartition reads a file that spans multiple partitions, splits it by partition,
// writes new files, and returns the new file metadata.
func (r *Repartitioner) Repartition(ctx context.Context, file DataFile) ([]RepartitionedFile, error) {
	r.logger.Debug("re-partitioning file that spans multiple partitions",
		zap.String("file", file.Path),
		zap.String("table", file.TableName))

	// Read the parquet file from storage
	data, err := r.fileIO.Read(ctx, file.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", file.Path, err)
	}

	// Parse the parquet data into an Arrow RecordBatch
	batch, err := arrow.ReadParquet(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parquet file %s: %w", file.Path, err)
	}
	defer batch.Release()

	// Determine the timestamp column based on table type
	timestampColumn := r.getTimestampColumn(file.TableName)

	// Split by partition
	partitionedBatches, err := arrow.SplitByPartition(batch, timestampColumn, r.allocator, r.timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to split file %s by partition: %w", file.Path, err)
	}
	defer func() {
		for _, pb := range partitionedBatches {
			pb.Release()
		}
	}()

	r.logger.Debug("split file into partitions",
		zap.String("file", file.Path),
		zap.Int("partition_count", len(partitionedBatches)))

	// Write new files for each partition
	var result []RepartitionedFile
	for _, pb := range partitionedBatches {
		newFile, err := r.writePartitionedFile(ctx, file, pb)
		if err != nil {
			// removes files that were written during a failed re-partition operation
			for _, f := range result {
				if delErr := r.fileIO.Delete(ctx, f.DataFile.Path); delErr != nil {
					r.logger.Error("failed to cleanup file during error recovery",
						zap.String("path", f.DataFile.Path),
						zap.Error(delErr))
				}
			}
			return nil, fmt.Errorf("failed to write partitioned file: %w", err)
		}
		result = append(result, *newFile)
	}

	return result, nil
}

// getTimestampColumn returns the timestamp column name for a given table type.
func (r *Repartitioner) getTimestampColumn(tableName string) string {
	switch {
	case strings.HasPrefix(tableName, "otel_traces"):
		return arrow.FieldTraceStartTimeUnixNano
	case strings.HasPrefix(tableName, "otel_logs"):
		return arrow.FieldLogTimeUnixNano
	case strings.HasPrefix(tableName, "otel_metrics"):
		return arrow.FieldMetricTimeUnixNano
	default:
		// Default to traces timestamp column
		return arrow.FieldTraceStartTimeUnixNano
	}
}

// writePartitionedFile writes a single partitioned batch to storage.
func (r *Repartitioner) writePartitionedFile(
	ctx context.Context,
	originalFile DataFile,
	pb arrow.PartitionedBatch,
) (*RepartitionedFile, error) {
	// Write to parquet
	data, err := arrow.WriteParquet(pb.Batch, arrow.ParquetWriterOptions{
		Compression: r.compression,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write parquet: %w", err)
	}

	// Generate new path using the unified path generator
	pathOpts := iceberg.PathOptions{
		TableName: originalFile.TableName,
		Timestamp: pb.Timestamp,
	}
	newPath := r.pathGenerator.GeneratePath(pathOpts)

	// Write to storage
	if err := r.fileIO.Write(ctx, newPath, data, iceberg.DefaultWriteOptions()); err != nil {
		return nil, fmt.Errorf("failed to write file %s: %w", newPath, err)
	}

	r.logger.Debug("wrote re-partitioned file",
		zap.String("path", newPath),
		zap.Time("partition", pb.Timestamp),
		zap.Int64("records", pb.RecordCount),
		zap.Int("bytes", len(data)))

	// Create the new DataFile with partition values from the generator
	newFile := DataFile{
		Path:            newPath,
		URI:             r.fileIO.GetURI(newPath),
		Size:            int64(len(data)),
		LastModified:    time.Now(),
		RecordCount:     pb.RecordCount,
		TableName:       originalFile.TableName,
		PartitionValues: r.pathGenerator.ExtractPartitionValues(pathOpts),
	}

	return &RepartitionedFile{
		DataFile:     newFile,
		OriginalPath: originalFile.Path,
	}, nil
}
