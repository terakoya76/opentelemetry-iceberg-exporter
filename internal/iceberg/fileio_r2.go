package iceberg

import (
	"bytes"
	"context"
	"fmt"

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

// Close implements FileIO.Close.
func (f *R2FileIO) Close() error {
	// R2 client doesn't need explicit cleanup
	return nil
}

// GetURI implements FileIO.GetURI.
// R2 uses S3-compatible URIs for query engine compatibility.
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
