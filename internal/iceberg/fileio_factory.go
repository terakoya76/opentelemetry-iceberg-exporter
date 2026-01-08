package iceberg

import (
	"context"
	"fmt"
)

// NewFileIO creates a new FileIO based on the configuration.
func NewFileIO(ctx context.Context, cfg FileIOConfig) (FileIO, error) {
	fileIOType := cfg.Type
	if fileIOType == "" {
		fileIOType = "s3" // Default to S3
	}

	switch fileIOType {
	case "s3":
		return NewS3FileIO(ctx, cfg.S3)
	case "r2":
		return NewR2FileIO(ctx, cfg.R2)
	case "filesystem":
		return NewLocalFileIO(ctx, cfg.Filesystem)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", fileIOType)
	}
}
