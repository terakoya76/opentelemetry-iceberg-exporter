package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// List implements FileIO.List.
func (f *LocalFileIO) List(_ context.Context, prefix string) ([]FileInfo, error) {
	var files []FileInfo

	searchPath := filepath.Join(f.basePath, prefix)

	// Check if the path exists
	if _, err := os.Stat(searchPath); os.IsNotExist(err) {
		// Return empty list if path doesn't exist
		return files, nil
	}

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path from base
		relPath, err := filepath.Rel(f.basePath, path)
		if err != nil {
			return err
		}

		// Convert to forward slashes for consistency
		relPath = strings.ReplaceAll(relPath, string(os.PathSeparator), "/")

		files = append(files, FileInfo{
			Path:         relPath,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list local files: %w", err)
	}

	return files, nil
}

// Read implements FileIO.Read.
func (f *LocalFileIO) Read(_ context.Context, filePath string) ([]byte, error) {
	fullPath := filepath.Join(f.basePath, filePath)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read local file: %w", err)
	}

	return data, nil
}

// Delete implements FileIO.Delete.
func (f *LocalFileIO) Delete(_ context.Context, filePath string) error {
	fullPath := filepath.Join(f.basePath, filePath)

	err := os.Remove(fullPath)
	if err != nil {
		// Return nil if file doesn't exist (idempotent delete)
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to delete local file %s: %w", fullPath, err)
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
