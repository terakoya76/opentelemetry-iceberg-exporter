package iceberg

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// FileIO abstracts the storage backend for writing Parquet files.
// This follows the Iceberg FileIO pattern where implementations
// handle the physical storage (S3, R2, filesystem, etc.)
// while the catalog layer handles metadata management separately.
type FileIO interface {
	// Write writes data to the specified path.
	// The path should be relative to the configured base location.
	Write(ctx context.Context, path string, data []byte, opts WriteOptions) error

	// List lists all files under the specified prefix.
	// The prefix should be relative to the configured base location.
	// Returns FileInfo for each file found.
	List(ctx context.Context, prefix string) ([]FileInfo, error)

	// Read reads a file from the specified path.
	// The path should be relative to the configured base location.
	Read(ctx context.Context, path string) ([]byte, error)

	// Close releases any resources held by the FileIO.
	Close() error

	// GetURI returns the full URI for a given path.
	// For S3: "s3://bucket/path"
	// For R2: "s3://bucket/path" (R2 uses S3-compatible URIs)
	// For filesystem: "file:///base/path"
	GetURI(path string) string

	// GetFileIOType returns the FileIO type identifier.
	GetFileIOType() string
}

// FileInfo contains metadata about a file in storage.
type FileInfo struct {
	// Path is the relative path to the file (relative to base location).
	Path string

	// Size is the file size in bytes.
	Size int64

	// LastModified is the time the file was last modified.
	LastModified time.Time
}

// WriteOptions contains options for a single write operation.
type WriteOptions struct {
	// ContentType is the MIME type of the content.
	ContentType string

	// Metadata is optional key-value metadata to attach to the object.
	Metadata map[string]string
}

// DefaultWriteOptions returns sensible defaults for Parquet files.
func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		ContentType: "application/vnd.apache.parquet",
	}
}

// FileIOConfig holds the FileIO configuration.
type FileIOConfig struct {
	// Type specifies the storage backend: "s3", "r2", "filesystem"
	Type string `mapstructure:"type"`

	// S3 configuration (used when Type is "s3")
	S3 S3FileIOConfig `mapstructure:"s3"`

	// R2 configuration (used when Type is "r2")
	R2 R2FileIOConfig `mapstructure:"r2"`

	// Filesystem configuration (used when Type is "filesystem")
	Filesystem LocalFileIOConfig `mapstructure:"filesystem"`
}

// S3FileIOConfig holds S3-specific configuration.
type S3FileIOConfig struct {
	// Region is the AWS region (required for AWS S3, optional for S3-compatible)
	Region string `mapstructure:"region"`

	// Bucket is the S3 bucket name (required)
	Bucket string `mapstructure:"bucket"`

	// Endpoint is the S3-compatible endpoint URL (optional, for MinIO, R2, etc.)
	Endpoint string `mapstructure:"endpoint"`

	// AccessKeyID is the AWS access key (optional, uses default credential chain if empty)
	AccessKeyID string `mapstructure:"access_key_id"`

	// SecretAccessKey is the AWS secret key (optional, uses default credential chain if empty)
	SecretAccessKey string `mapstructure:"secret_access_key"`

	// Compression specifies the Parquet compression algorithm
	Compression string `mapstructure:"compression"`
}

// R2FileIOConfig holds Cloudflare R2-specific configuration.
type R2FileIOConfig struct {
	// AccountID is the Cloudflare account ID (required)
	AccountID string `mapstructure:"account_id"`

	// Bucket is the R2 bucket name (required)
	Bucket string `mapstructure:"bucket"`

	// AccessKeyID is the R2 access key (required)
	AccessKeyID string `mapstructure:"access_key_id"`

	// SecretAccessKey is the R2 secret key (required)
	SecretAccessKey string `mapstructure:"secret_access_key"`

	// Compression specifies the Parquet compression algorithm
	Compression string `mapstructure:"compression"`
}

// LocalFileIOConfig holds filesystem-specific configuration.
type LocalFileIOConfig struct {
	// BasePath is the base directory for storing files (required)
	BasePath string `mapstructure:"base_path"`

	// Compression specifies the Parquet compression algorithm
	Compression string `mapstructure:"compression"`
}

// Validate validates the FileIO configuration.
// This is the only public Validate method - nested configs use private validate()
// to prevent OTel SDK from auto-calling them regardless of storage type.
func (c *FileIOConfig) Validate() error {
	switch c.Type {
	case "s3", "":
		return c.S3.validate()
	case "r2":
		return c.R2.validate()
	case "filesystem":
		return c.Filesystem.validate()
	default:
		return fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// validate validates the S3 configuration.
// Private to prevent OTel SDK from auto-calling when S3 isn't used.
func (c *S3FileIOConfig) validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if c.Region == "" && c.Endpoint == "" {
		return fmt.Errorf("s3.region or s3.endpoint is required")
	}
	return validateCompression(c.Compression)
}

// validate validates the R2 configuration.
// Private to prevent OTel SDK from auto-calling when R2 isn't used.
func (c *R2FileIOConfig) validate() error {
	if c.AccountID == "" {
		return fmt.Errorf("r2.account_id is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("r2.bucket is required")
	}
	if c.AccessKeyID == "" {
		return fmt.Errorf("r2.access_key_id is required")
	}
	if c.SecretAccessKey == "" {
		return fmt.Errorf("r2.secret_access_key is required")
	}
	return validateCompression(c.Compression)
}

// validate validates the filesystem configuration.
// Private to prevent OTel SDK from auto-calling when filesystem isn't used.
func (c *LocalFileIOConfig) validate() error {
	if c.BasePath == "" {
		return fmt.Errorf("filesystem.base_path is required")
	}
	return validateCompression(c.Compression)
}

func validateCompression(compression string) error {
	switch compression {
	case "", "none", "gzip", "zstd", "snappy":
		return nil
	default:
		return fmt.Errorf("compression must be one of: none, gzip, zstd, snappy")
	}
}

// GetCompression returns the configured compression, defaulting to snappy.
func (c *S3FileIOConfig) GetCompression() string {
	if c.Compression == "" {
		return "snappy"
	}
	return c.Compression
}

// GetCompression returns the configured compression, defaulting to snappy.
func (c *R2FileIOConfig) GetCompression() string {
	if c.Compression == "" {
		return "snappy"
	}
	return c.Compression
}

// GetCompression returns the configured compression, defaulting to snappy.
func (c *LocalFileIOConfig) GetCompression() string {
	if c.Compression == "" {
		return "snappy"
	}
	return c.Compression
}

// ShouldUsePathStyle determines whether to use path-style addressing for S3.
// Returns true if path-style should be used, false for virtual-hosted-style.
//
// Auto-detection logic:
// - If no endpoint is specified (using AWS S3 with region), use virtual-hosted-style
// - If endpoint contains ".amazonaws.com", use virtual-hosted-style
// - If endpoint is localhost, loopback IP, or private IP, use path-style
// - For any other custom endpoint, use path-style (safer default for S3-compatible services)
func (c *S3FileIOConfig) ShouldUsePathStyle() bool {
	// No endpoint means using AWS S3 with region-based endpoint
	// AWS S3 supports virtual-hosted-style
	if c.Endpoint == "" {
		return false
	}

	return shouldUsePathStyleForEndpoint(c.Endpoint)
}

// shouldUsePathStyleForEndpoint analyzes an endpoint URL to determine
// if path-style addressing should be used.
func shouldUsePathStyleForEndpoint(endpoint string) bool {
	// Parse the endpoint URL
	u, err := url.Parse(endpoint)
	if err != nil {
		// If we can't parse it, default to path-style (safer)
		return true
	}

	host := u.Host
	// Remove port if present
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	// AWS S3 endpoints use virtual-hosted-style
	if strings.HasSuffix(host, ".amazonaws.com") {
		return false
	}

	// Google Cloud Storage also supports virtual-hosted-style
	if strings.HasSuffix(host, ".googleapis.com") {
		return false
	}

	// Localhost and loopback addresses require path-style
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	// Check if it's an IP address (not a domain name)
	// IP-based endpoints typically require path-style
	if ip := net.ParseIP(host); ip != nil {
		return true
	}

	// Check for private/local network hostnames that likely need path-style
	// (common in Docker/Kubernetes environments)
	localPatterns := []string{
		"minio",
		"localstack",
		"s3mock",
		".local",
		".internal",
		".svc",
	}
	hostLower := strings.ToLower(host)
	for _, pattern := range localPatterns {
		if strings.Contains(hostLower, pattern) {
			return true
		}
	}

	// For any other custom endpoint, default to path-style
	// This is the safer choice for S3-compatible services like MinIO
	return true
}
