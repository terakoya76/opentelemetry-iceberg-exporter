package iceberg

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// NoCatalog is a no-op Catalog implementation.
// Used when catalog registration is disabled (catalog_type is "none" or empty).
type NoCatalog struct{}

// NewNoCatalog creates a new NoCatalog.
func NewNoCatalog() *NoCatalog {
	return &NoCatalog{}
}

// EnsureNamespace implements Catalog.EnsureNamespace (no-op).
func (c *NoCatalog) EnsureNamespace(_ context.Context, _ string) error {
	return nil
}

// EnsureTable implements Catalog.EnsureTable (no-op).
func (c *NoCatalog) EnsureTable(_ context.Context, _, _ string, _ *arrow.Schema, _ PartitionSpec) error {
	return nil
}

// AppendRecords implements Catalog.AppendRecords.
func (c *NoCatalog) AppendRecords(_ context.Context, _, _ string, _ arrow.RecordBatch, _ iceberg.Properties) error {
	return nil
}

// AppendDataFiles implements Catalog.AppendDataFiles.
func (c *NoCatalog) AppendDataFiles(_ context.Context, _ []AppendOptions) error {
	return nil
}

// ListDataFiles implements Catalog.ListDataFiles (no-op).
func (c *NoCatalog) ListDataFiles(_ context.Context, _, _ string) ([]string, error) {
	return []string{}, nil
}

// Close implements Catalog.Close (no-op).
func (c *NoCatalog) Close() error {
	return nil
}

// GetCatalogType implements Catalog.GetCatalogType.
func (c *NoCatalog) GetCatalogType() string {
	return CatalogTypeNone
}
