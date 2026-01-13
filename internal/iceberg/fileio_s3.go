package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3FileIO implements FileIO for AWS S3 and S3-compatible storage.
type S3FileIO struct {
	client *s3.Client
	bucket string
}

// NewS3FileIO creates a new S3FileIO.
func NewS3FileIO(ctx context.Context, cfg S3FileIOConfig) (*S3FileIO, error) {
	awsCfg, err := BuildAWSConfig(ctx, AWSCredentials{
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		Region:          cfg.Region,
	})
	if err != nil {
		return nil, err
	}

	// Create S3 client with custom options
	var s3Opts []func(*s3.Options)

	// Set custom endpoint for S3-compatible storage
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	// Determine path-style vs virtual-hosted-style addressing
	// Auto-detects based on endpoint URL if not explicitly configured
	if cfg.ShouldUsePathStyle() {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(*awsCfg, s3Opts...)

	return &S3FileIO{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// Write implements FileIO.Write.
func (f *S3FileIO) Write(ctx context.Context, filePath string, data []byte, opts WriteOptions) error {
	key := f.buildKey(filePath)

	contentType := opts.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(f.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	}

	// Add metadata if provided
	if len(opts.Metadata) > 0 {
		input.Metadata = opts.Metadata
	}

	_, err := f.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// List implements FileIO.List.
func (f *S3FileIO) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	var files []FileInfo

	paginator := s3.NewListObjectsV2Paginator(f.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			// Skip "directory" entries (keys ending with /)
			if strings.HasSuffix(*obj.Key, "/") {
				continue
			}

			fi := FileInfo{
				Path: *obj.Key,
			}
			if obj.Size != nil {
				fi.Size = *obj.Size
			}
			if obj.LastModified != nil {
				fi.LastModified = *obj.LastModified
			}

			files = append(files, fi)
		}
	}

	return files, nil
}

// Read implements FileIO.Read.
func (f *S3FileIO) Read(ctx context.Context, filePath string) ([]byte, error) {
	key := f.buildKey(filePath)

	output, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer func() { _ = output.Body.Close() }()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 object body: %w", err)
	}

	return data, nil
}

// Delete implements FileIO.Delete.
func (f *S3FileIO) Delete(ctx context.Context, filePath string) error {
	key := f.buildKey(filePath)

	_, err := f.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete S3 object %s: %w", key, err)
	}

	return nil
}

// Close implements FileIO.Close.
func (f *S3FileIO) Close() error {
	// S3 client doesn't need explicit cleanup
	return nil
}

// GetURI implements FileIO.GetURI.
func (f *S3FileIO) GetURI(filePath string) string {
	key := f.buildKey(filePath)
	return fmt.Sprintf("s3://%s/%s", f.bucket, key)
}

// GetFileIOType implements FileIO.GetFileIOType.
func (f *S3FileIO) GetFileIOType() string {
	return "s3"
}

// buildKey returns the S3 key for the given path.
func (f *S3FileIO) buildKey(filePath string) string {
	return filePath
}

// GetBucket returns the bucket name.
func (f *S3FileIO) GetBucket() string {
	return f.bucket
}
