package iceberg

import (
	"context"
	"errors"
	"sync"
)

// MockFileIO is a mock implementation of FileIO for testing.
// It stores files in memory and provides methods to configure error behavior.
type MockFileIO struct {
	mu           sync.Mutex
	files        map[string][]byte
	deletedPaths []string
	deleteErr    error
}

// NewMockFileIO creates a new MockFileIO instance.
func NewMockFileIO() *MockFileIO {
	return &MockFileIO{
		files:        make(map[string][]byte),
		deletedPaths: make([]string, 0),
	}
}

// Write implements FileIO.Write.
func (m *MockFileIO) Write(_ context.Context, path string, data []byte, _ WriteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	return nil
}

// List implements FileIO.List.
func (m *MockFileIO) List(_ context.Context, _ string) ([]FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var infos []FileInfo
	for path, data := range m.files {
		infos = append(infos, FileInfo{
			Path: path,
			Size: int64(len(data)),
		})
	}
	return infos, nil
}

// Read implements FileIO.Read.
func (m *MockFileIO) Read(_ context.Context, path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.files[path]; ok {
		return data, nil
	}
	return nil, errors.New("file not found")
}

// Delete implements FileIO.Delete.
func (m *MockFileIO) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.files, path)
	m.deletedPaths = append(m.deletedPaths, path)
	return nil
}

// Close implements FileIO.Close.
func (m *MockFileIO) Close() error {
	return nil
}

// GetURI implements FileIO.GetURI.
func (m *MockFileIO) GetURI(path string) string {
	return "file://" + path
}

// GetFileIOType implements FileIO.GetFileIOType.
func (m *MockFileIO) GetFileIOType() string {
	return "mock"
}

// SetDeleteErr sets the error to return from Delete operations.
func (m *MockFileIO) SetDeleteErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteErr = err
}

// GetDeletedPaths returns a copy of the paths that have been deleted.
func (m *MockFileIO) GetDeletedPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.deletedPaths...)
}

// SetFile sets a file directly in the mock storage.
func (m *MockFileIO) SetFile(path string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
}
