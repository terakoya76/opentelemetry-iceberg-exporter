package arrow

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
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
