package iceberg

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// AWSCredentials holds AWS/S3-compatible authentication credentials.
type AWSCredentials struct {
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string

	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string

	// Region is the AWS region (e.g., "us-east-1", "auto" for R2).
	Region string
}

// HasCredentials returns true if static credentials are configured.
// Both AccessKeyID and SecretAccessKey must be non-empty.
func (c AWSCredentials) HasCredentials() bool {
	return c.AccessKeyID != "" && c.SecretAccessKey != ""
}

// BuildAWSConfig creates an AWS config with optional static credentials.
// If credentials are not provided, falls back to AWS default credential chain
// (environment variables, IAM roles, etc.).
func BuildAWSConfig(ctx context.Context, creds AWSCredentials) (*aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	if creds.Region != "" {
		opts = append(opts, config.WithRegion(creds.Region))
	}

	if creds.HasCredentials() {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				creds.AccessKeyID,
				creds.SecretAccessKey,
				"", // session token (not used for static credentials)
			),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &cfg, nil
}

// BuildAWSConfigFromStorageConfig creates an AWS config based on the storage configuration.
// This is a convenience function that extracts credentials from FileIOConfig.
func BuildAWSConfigFromStorageConfig(ctx context.Context, storageCfg FileIOConfig) (*aws.Config, error) {
	switch storageCfg.Type {
	case "s3", "":
		s3Cfg := storageCfg.S3
		return BuildAWSConfig(ctx, AWSCredentials{
			AccessKeyID:     s3Cfg.AccessKeyID,
			SecretAccessKey: s3Cfg.SecretAccessKey,
			Region:          s3Cfg.Region,
		})

	case "r2":
		r2Cfg := storageCfg.R2
		if r2Cfg.AccessKeyID == "" || r2Cfg.SecretAccessKey == "" {
			return nil, fmt.Errorf("R2 requires access_key_id and secret_access_key")
		}

		return BuildAWSConfig(ctx, AWSCredentials{
			AccessKeyID:     r2Cfg.AccessKeyID,
			SecretAccessKey: r2Cfg.SecretAccessKey,
			Region:          "auto", // R2 uses "auto" region
		})

	case "filesystem":
		// Local filesystem doesn't need AWS config, but we still create a default one
		// in case catalog operations need it
		return BuildAWSConfig(ctx, AWSCredentials{})

	default:
		return nil, fmt.Errorf("unsupported storage type for AWS config: %s", storageCfg.Type)
	}
}
