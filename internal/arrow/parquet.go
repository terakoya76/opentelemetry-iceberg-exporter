package arrow

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// ParquetWriterOptions holds options for writing Parquet files.
type ParquetWriterOptions struct {
	// Compression is the compression algorithm to use.
	Compression string
}

// stripFieldIDMetadata creates a new schema without PARQUET:field_id metadata.
// This is necessary because iceberg-go's add-files operation does not support
// Parquet files that have embedded field IDs. The field IDs should be managed
// by Iceberg's catalog metadata, not embedded in the Parquet file schema.
func stripFieldIDMetadata(schema *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, schema.NumFields())
	for i, f := range schema.Fields() {
		// Create new metadata without PARQUET:field_id
		var newMeta arrow.Metadata
		if f.Metadata.Len() > 0 {
			keys := make([]string, 0, f.Metadata.Len())
			vals := make([]string, 0, f.Metadata.Len())
			for j := 0; j < f.Metadata.Len(); j++ {
				k := f.Metadata.Keys()[j]
				if k != "PARQUET:field_id" {
					keys = append(keys, k)
					vals = append(vals, f.Metadata.Values()[j])
				}
			}
			if len(keys) > 0 {
				newMeta = arrow.NewMetadata(keys, vals)
			}
		}

		fields[i] = arrow.Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: newMeta,
		}
	}

	// Preserve schema-level metadata (e.g., schema version info)
	meta := schema.Metadata()
	return arrow.NewSchema(fields, &meta)
}

// ReadParquet reads parquet data into an Arrow RecordBatch.
// The caller is responsible for releasing the returned RecordBatch.
func ReadParquet(data []byte) (result arrow.RecordBatch, err error) {
	// Create a parquet file reader from the byte buffer
	// bytes.Reader implements io.ReaderAt and io.Seeker which satisfies parquet.ReaderAtSeeker
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer func() { _ = pf.Close() }()

	// Collect file metadata for diagnostics before attempting to read
	fileMeta := collectParquetMetadata(pf)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	// Recover from panics in arrow-go library
	// This provides graceful error handling for corrupted files or library bugs
	// - https://github.com/apache/arrow-go/issues/613
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while reading parquet data: %v (file info: %s)", r, fileMeta)
			result = nil
		}
	}()

	// Read all row groups into a table
	ctx := context.Background()
	table, err := reader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet table: %w (file info: %s)", err, fileMeta)
	}
	defer table.Release()

	// Convert table to a single RecordBatch
	// If table has multiple chunks, we need to combine them
	tableReader := array.NewTableReader(table, table.NumRows())
	defer tableReader.Release()

	if !tableReader.Next() {
		return nil, fmt.Errorf("no data in parquet file (file info: %s)", fileMeta)
	}

	record := tableReader.RecordBatch()
	record.Retain() // Retain because we're returning it

	return record, nil
}

// collectParquetMetadata gathers diagnostic information from a parquet file.
func collectParquetMetadata(pf *file.Reader) string {
	meta := pf.MetaData()
	if meta == nil {
		return "metadata unavailable"
	}

	numRowGroups := pf.NumRowGroups()
	numRows := meta.NumRows
	numCols := meta.Schema.NumColumns()

	// Collect column names for diagnostics
	var colNames []string
	schema := meta.Schema
	for i := 0; i < min(numCols, 5); i++ { // Limit to first 5 columns
		col := schema.Column(i)
		if col != nil {
			colNames = append(colNames, col.Name())
		}
	}
	if numCols > 5 {
		colNames = append(colNames, fmt.Sprintf("... and %d more", numCols-5))
	}

	return fmt.Sprintf("row_groups=%d, rows=%d, cols=%d, columns=%v",
		numRowGroups, numRows, numCols, colNames)
}

// WriteParquet writes an Arrow record to a Parquet buffer.
func WriteParquet(record arrow.RecordBatch, opts ParquetWriterOptions) ([]byte, error) {
	var buf bytes.Buffer

	// Determine compression codec
	var codec compress.Compression
	switch opts.Compression {
	case "gzip":
		codec = compress.Codecs.Gzip
	case "zstd":
		codec = compress.Codecs.Zstd
	case "snappy":
		codec = compress.Codecs.Snappy
	case "none", "":
		codec = compress.Codecs.Uncompressed
	default:
		codec = compress.Codecs.Snappy
	}

	// Create Parquet writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	// Create a schema without field_id metadata.
	// This ensures Iceberg's add-files operation can process the Parquet file,
	// as it does not support files with embedded field IDs.
	strippedSchema := stripFieldIDMetadata(record.Schema())

	// Creates a new record with the stripped schema but referencing
	// the same column arrays (efficient - no data copying)
	cols := make([]arrow.Array, record.NumCols())
	for i := int64(0); i < record.NumCols(); i++ {
		cols[i] = record.Column(int(i))
	}
	strippedRecord := array.NewRecordBatch(strippedSchema, cols, record.NumRows())
	defer strippedRecord.Release()

	// Write Parquet
	writer, err := pqarrow.NewFileWriter(strippedSchema, &buf, props, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(strippedRecord); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("failed to write record to parquet: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// CompressData compresses data using the specified algorithm.
func CompressData(data []byte, compression string) ([]byte, error) {
	switch compression {
	case "gzip":
		return compressGzip(data)
	case "zstd":
		return compressZstd(data)
	case "snappy":
		return compressSnappy(data)
	case "none", "":
		return data, nil
	default:
		return data, nil
	}
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func compressZstd(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return encoder.EncodeAll(data, nil), nil
}

func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// DecompressData decompresses data using the specified algorithm.
func DecompressData(data []byte, compression string) ([]byte, error) {
	switch compression {
	case "gzip":
		return decompressGzip(data)
	case "zstd":
		return decompressZstd(data)
	case "snappy":
		return decompressSnappy(data)
	case "none", "":
		return data, nil
	default:
		return data, nil
	}
}

func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

func decompressZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return decoder.DecodeAll(data, nil)
}

func decompressSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
