package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// LocalFileIO implements FileIO for local filesystem storage.
// Useful for testing and development.
type LocalFileIO struct {
	basePath string
}

// NewLocalFileIO creates a new LocalFileIO.
func NewLocalFileIO(_ context.Context, cfg LocalFileIOConfig) (*LocalFileIO, error) {
	// Ensure base path exists
	if err := os.MkdirAll(cfg.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	return &LocalFileIO{
		basePath: absPath,
	}, nil
}

// Write implements FileIO.Write.
func (f *LocalFileIO) Write(_ context.Context, filePath string, data []byte, _ WriteOptions) error {
	fullPath := filepath.Join(f.basePath, filePath)

	// Ensure parent directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write file
	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", fullPath, err)
	}

	return nil
}

// Close implements FileIO.Close.
func (f *LocalFileIO) Close() error {
	// Nothing to clean up for filesystem
	return nil
}

// GetURI implements FileIO.GetURI.
func (f *LocalFileIO) GetURI(filePath string) string {
	fullPath := filepath.Join(f.basePath, filePath)
	return fmt.Sprintf("file://%s", fullPath)
}

// GetFileIOType implements FileIO.GetFileIOType.
func (f *LocalFileIO) GetFileIOType() string {
	return "filesystem"
}

// GetBasePath returns the base path.
func (f *LocalFileIO) GetBasePath() string {
	return f.basePath
}
