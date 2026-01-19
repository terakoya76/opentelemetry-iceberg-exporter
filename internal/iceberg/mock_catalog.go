package iceberg

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// MockCatalog is a mock implementation of Catalog for testing.
// It provides configurable error behavior and tracks method calls.
type MockCatalog struct {
	catalogType          string
	appendErr            error
	appendErrOnce        bool
	appendErrMinBatch    int // If > 0, only return error when batch size >= this value
	appendCount          int // Total number of files appended
	appendCalls          int // Number of times AppendDataFiles was called
	listDataFiles        []string
	listDataFilesByTable map[string][]string // per-table file lists (table name -> files)
	listErr              error
	ensureNsErr          error
	ensureTableErr       error
	mu                   sync.Mutex
}

// NewMockCatalog creates a new MockCatalog instance.
func NewMockCatalog() *MockCatalog {
	return &MockCatalog{
		catalogType: "mock",
	}
}

// GetCatalogType implements Catalog.GetCatalogType.
func (m *MockCatalog) GetCatalogType() string {
	return m.catalogType
}

// EnsureNamespace implements Catalog.EnsureNamespace.
func (m *MockCatalog) EnsureNamespace(_ context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureNsErr
}

// EnsureTable implements Catalog.EnsureTable.
func (m *MockCatalog) EnsureTable(_ context.Context, _, _ string, _ *arrow.Schema, _ PartitionSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureTableErr
}

// AppendDataFiles implements Catalog.AppendDataFiles.
func (m *MockCatalog) AppendDataFiles(_ context.Context, opts []AppendOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendCalls++
	m.appendCount += len(opts)
	if m.appendErr != nil {
		// If appendErrMinBatch is set, only return error for batches >= that size
		if m.appendErrMinBatch > 0 && len(opts) < m.appendErrMinBatch {
			return nil // Success for smaller batches
		}
		if m.appendErrOnce {
			err := m.appendErr
			m.appendErr = nil
			return err
		}
		return m.appendErr
	}
	return nil
}

// ListDataFiles implements Catalog.ListDataFiles.
func (m *MockCatalog) ListDataFiles(_ context.Context, _, table string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	// Check for per-table file list first
	if m.listDataFilesByTable != nil {
		if files, ok := m.listDataFilesByTable[table]; ok {
			return files, nil
		}
	}
	// Fall back to default list
	return m.listDataFiles, nil
}

// Close implements Catalog.Close.
func (m *MockCatalog) Close() error {
	return nil
}

// SetCatalogType sets the catalog type returned by GetCatalogType.
func (m *MockCatalog) SetCatalogType(catalogType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalogType = catalogType
}

// SetAppendErr sets the error to return from AppendDataFiles.
// If once is true, the error is returned only on the first call.
func (m *MockCatalog) SetAppendErr(err error, once bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendErr = err
	m.appendErrOnce = once
	m.appendErrMinBatch = 0
}

// SetAppendErrForMinBatch sets the error to return from AppendDataFiles
// only when the batch size is >= minBatchSize.
// This is useful for testing gradual batch size reduction.
func (m *MockCatalog) SetAppendErrForMinBatch(err error, minBatchSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendErr = err
	m.appendErrOnce = false
	m.appendErrMinBatch = minBatchSize
}

// SetListDataFiles sets the default files to return from ListDataFiles.
func (m *MockCatalog) SetListDataFiles(files []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listDataFiles = files
}

// SetListDataFilesForTable sets the files to return from ListDataFiles for a specific table.
// If set, this takes precedence over the default list set by SetListDataFiles.
func (m *MockCatalog) SetListDataFilesForTable(table string, files []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listDataFilesByTable == nil {
		m.listDataFilesByTable = make(map[string][]string)
	}
	m.listDataFilesByTable[table] = files
}

// SetListErr sets the error to return from ListDataFiles.
func (m *MockCatalog) SetListErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listErr = err
}

// SetEnsureNamespaceErr sets the error to return from EnsureNamespace.
func (m *MockCatalog) SetEnsureNamespaceErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureNsErr = err
}

// SetEnsureTableErr sets the error to return from EnsureTable.
func (m *MockCatalog) SetEnsureTableErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureTableErr = err
}

// GetAppendCalls returns the number of times AppendDataFiles was called.
func (m *MockCatalog) GetAppendCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendCalls
}

// GetAppendCount returns the total number of files passed to AppendDataFiles.
func (m *MockCatalog) GetAppendCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendCount
}

// Reset clears all state and resets error configurations.
func (m *MockCatalog) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalogType = "mock"
	m.appendErr = nil
	m.appendErrOnce = false
	m.appendErrMinBatch = 0
	m.appendCount = 0
	m.appendCalls = 0
	m.listDataFiles = nil
	m.listDataFilesByTable = nil
	m.listErr = nil
	m.ensureNsErr = nil
	m.ensureTableErr = nil
}
