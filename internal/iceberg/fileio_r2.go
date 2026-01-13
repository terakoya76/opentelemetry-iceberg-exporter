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

// R2FileIO implements FileIO for Cloudflare R2 storage.
// R2 uses S3-compatible API with some specific requirements.
type R2FileIO struct {
	client    *s3.Client
	bucket    string
	accountID string
}

// NewR2FileIO creates a new R2FileIO.
func NewR2FileIO(ctx context.Context, cfg R2FileIOConfig) (*R2FileIO, error) {
	// R2 requires static credentials
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("R2 requires access_key_id and secret_access_key")
	}

	// Build R2 endpoint URL
	endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", cfg.AccountID)

	// Build AWS config with R2-specific settings (auto region)
	awsCfg, err := BuildAWSConfig(ctx, AWSCredentials{
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		Region:          "auto", // R2 uses "auto" region
	})
	if err != nil {
		return nil, err
	}

	// Create S3 client with R2-specific options
	client := s3.NewFromConfig(*awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// R2 requires path-style addressing
		o.UsePathStyle = true
	})

	return &R2FileIO{
		client:    client,
		bucket:    cfg.Bucket,
		accountID: cfg.AccountID,
	}, nil
}

// Write implements FileIO.Write.
func (f *R2FileIO) Write(ctx context.Context, filePath string, data []byte, opts WriteOptions) error {
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
		return fmt.Errorf("failed to upload to R2: %w", err)
	}

	return nil
}

// List implements FileIO.List.
func (f *R2FileIO) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	var files []FileInfo

	paginator := s3.NewListObjectsV2Paginator(f.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list R2 objects: %w", err)
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
func (f *R2FileIO) Read(ctx context.Context, filePath string) ([]byte, error) {
	key := f.buildKey(filePath)

	output, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get R2 object: %w", err)
	}
	defer func() { _ = output.Body.Close() }()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read R2 object body: %w", err)
	}

	return data, nil
}

// Delete implements FileIO.Delete.
func (f *R2FileIO) Delete(ctx context.Context, filePath string) error {
	key := f.buildKey(filePath)

	_, err := f.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete R2 object %s: %w", key, err)
	}

	return nil
}

// Close implements FileIO.Close.
func (f *R2FileIO) Close() error {
	// R2 client doesn't need explicit cleanup
	return nil
}

// GetURI implements FileIO.GetURI.
func (f *R2FileIO) GetURI(filePath string) string {
	key := f.buildKey(filePath)
	// Use S3 URI format for compatibility with query engines
	return fmt.Sprintf("s3://%s/%s", f.bucket, key)
}

// GetFileIOType implements FileIO.GetFileIOType.
func (f *R2FileIO) GetFileIOType() string {
	return "r2"
}

// buildKey returns the R2 key for the given path.
func (f *R2FileIO) buildKey(filePath string) string {
	return filePath
}

// GetBucket returns the bucket name.
func (f *R2FileIO) GetBucket() string {
	return f.bucket
}

// GetAccountID returns the Cloudflare account ID.
func (f *R2FileIO) GetAccountID() string {
	return f.accountID
}
