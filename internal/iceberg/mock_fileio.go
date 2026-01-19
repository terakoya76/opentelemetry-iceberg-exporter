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
	writeErr     error
	readErr      error
	listErr      error
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
	if m.writeErr != nil {
		return m.writeErr
	}
	m.files[path] = data
	return nil
}

// List implements FileIO.List.
func (m *MockFileIO) List(_ context.Context, _ string) ([]FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
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
	if m.readErr != nil {
		return nil, m.readErr
	}
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

// SetWriteErr sets the error to return from Write operations.
func (m *MockFileIO) SetWriteErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeErr = err
}

// SetReadErr sets the error to return from Read operations.
func (m *MockFileIO) SetReadErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readErr = err
}

// SetListErr sets the error to return from List operations.
func (m *MockFileIO) SetListErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listErr = err
}

// GetDeletedPaths returns a copy of the paths that have been deleted.
func (m *MockFileIO) GetDeletedPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.deletedPaths...)
}

// GetFiles returns a copy of all stored files.
func (m *MockFileIO) GetFiles() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string][]byte, len(m.files))
	for k, v := range m.files {
		result[k] = v
	}
	return result
}

// SetFile sets a file directly in the mock storage.
func (m *MockFileIO) SetFile(path string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
}

// Reset clears all files and resets error states.
func (m *MockFileIO) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files = make(map[string][]byte)
	m.deletedPaths = make([]string, 0)
	m.deleteErr = nil
	m.writeErr = nil
	m.readErr = nil
	m.listErr = nil
}
