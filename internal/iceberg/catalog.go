package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

const (
	// CatalogTypeNone is the catalog type for no-op catalog (direct FileIO writes only).
	CatalogTypeNone = "none"

	// CatalogTypeRest is the catalog type for REST catalog.
	CatalogTypeRest = "rest"
)

// Catalog abstracts Iceberg catalog operations.
// Implementations handle metadata management for Iceberg tables.
type Catalog interface {
	// EnsureNamespace creates the namespace if it doesn't exist.
	// Returns nil if namespace already exists.
	EnsureNamespace(ctx context.Context, namespace string) error

	// EnsureTable ensures the table exists with the given schema.
	// Creates the table if it doesn't exist.
	// Returns nil if table already exists (does not update schema).
	EnsureTable(ctx context.Context, namespace, table string, schema *arrow.Schema, partitionSpec PartitionSpec) error

	// AppendRecords appends Arrow records directly to an Iceberg table.
	// All operations are performed atomically within a transaction.
	AppendRecords(ctx context.Context, namespace, table string, record arrow.RecordBatch, props iceberg.Properties) error

	// AppendDataFiles registers one or more data files with a table atomically.
	// All files must belong to the same namespace and table.
	AppendDataFiles(ctx context.Context, opts []AppendOptions) error

	// ListDataFiles returns a list of data file paths registered with the table.
	// Returns the full URIs of all data files in the current snapshot's manifests.
	// If the table doesn't exist or has no snapshots, returns an empty list.
	ListDataFiles(ctx context.Context, namespace, table string) ([]string, error)

	// Close releases any resources held by the catalog.
	Close() error

	// GetCatalogType returns the catalog type identifier.
	GetCatalogType() string
}

// AppendOptions contains options for appending a data file to a table.
type AppendOptions struct {
	// Namespace is the Iceberg namespace (database).
	Namespace string

	// Table is the table name.
	Table string

	// FilePath is the full URI of the data file (e.g., "s3://bucket/path/file.parquet").
	FilePath string

	// FileSizeBytes is the size of the file in bytes.
	FileSizeBytes int64

	// RecordCount is the number of records in the file.
	RecordCount int64

	// PartitionValues are the partition column values for this file (Hive-style).
	PartitionValues PartitionValues
}

// CatalogConfig holds the catalog configuration.
type CatalogConfig struct {
	// Type specifies the catalog type: CatalogTypeRest ("rest") or CatalogTypeNone ("none")
	Type string `mapstructure:"type"`

	// Namespace is the default namespace for all tables.
	Namespace string `mapstructure:"namespace"`

	// REST catalog configuration.
	REST RESTCatalogConfig `mapstructure:"rest"`

	// Tables contains table name overrides.
	Tables TableNamesConfig `mapstructure:"tables"`
}

// RESTCatalogConfig holds REST catalog configuration.
type RESTCatalogConfig struct {
	// URI is the REST catalog endpoint (required).
	URI string `mapstructure:"uri"`

	// Warehouse is the warehouse location (required for most REST catalogs).
	Warehouse string `mapstructure:"warehouse"`

	// Token is the bearer token for authentication.
	Token string `mapstructure:"token"`
}

// TableNamesConfig holds table name overrides.
type TableNamesConfig struct {
	// Traces is the table name for traces (default: "otel_traces").
	Traces string `mapstructure:"traces"`

	// Logs is the table name for logs (default: "otel_logs").
	Logs string `mapstructure:"logs"`

	// Metrics is the table name prefix for metrics (default: "otel_metrics").
	// With separated schemas, actual table names will be:
	// - {prefix}_gauge (default: "otel_metrics_gauge")
	// - {prefix}_sum (default: "otel_metrics_sum")
	// - {prefix}_histogram (default: "otel_metrics_histogram")
	// - {prefix}_exponential_histogram (default: "otel_metrics_exponential_histogram")
	// - {prefix}_summary (default: "otel_metrics_summary")
	Metrics string `mapstructure:"metrics"`

	// MetricsGauge is the explicit table name for gauge metrics (overrides Metrics prefix).
	MetricsGauge string `mapstructure:"metrics_gauge"`

	// MetricsSum is the explicit table name for sum metrics (overrides Metrics prefix).
	MetricsSum string `mapstructure:"metrics_sum"`

	// MetricsHistogram is the explicit table name for histogram metrics (overrides Metrics prefix).
	MetricsHistogram string `mapstructure:"metrics_histogram"`

	// MetricsExponentialHistogram is the explicit table name for exponential histogram metrics (overrides Metrics prefix).
	MetricsExponentialHistogram string `mapstructure:"metrics_exponential_histogram"`

	// MetricsSummary is the explicit table name for summary metrics (overrides Metrics prefix).
	MetricsSummary string `mapstructure:"metrics_summary"`
}

// Validate validates the catalog configuration.
// This is the only public Validate method - nested configs use private validate()
// to prevent OTel SDK from auto-calling them regardless of catalog type.
func (c *CatalogConfig) Validate() error {
	switch c.Type {
	case "":
		return fmt.Errorf("catalog.type is required: must be one of %q or %q", CatalogTypeRest, CatalogTypeNone)
	case CatalogTypeNone:
		return nil // Explicitly disabled
	case CatalogTypeRest:
		return c.REST.validate()
	default:
		return fmt.Errorf("unknown catalog type: %s (must be one of %q or %q)", c.Type, CatalogTypeRest, CatalogTypeNone)
	}
}

// validate validates the REST catalog configuration.
// Private to prevent OTel SDK from auto-calling when REST catalog isn't used.
func (c *RESTCatalogConfig) validate() error {
	if c.URI == "" {
		return fmt.Errorf("rest.uri is required")
	}
	return nil
}

// GetNamespace returns the namespace, defaulting to "default".
func (c *CatalogConfig) GetNamespace() string {
	if c.Namespace == "" {
		return "default"
	}
	return c.Namespace
}

// GetTableName returns the table name for a signal type.
// For metrics, signalType should be "metrics_gauge", "metrics_sum", etc.
func (c *TableNamesConfig) GetTableName(signalType string) string {
	switch signalType {
	case "traces":
		if c.Traces != "" {
			return c.Traces
		}
		return "otel_traces"
	case "logs":
		if c.Logs != "" {
			return c.Logs
		}
		return "otel_logs"
	// Separated metric types
	case "metrics_gauge":
		if c.MetricsGauge != "" {
			return c.MetricsGauge
		}
		return c.getMetricsPrefix() + "_gauge"
	case "metrics_sum":
		if c.MetricsSum != "" {
			return c.MetricsSum
		}
		return c.getMetricsPrefix() + "_sum"
	case "metrics_histogram":
		if c.MetricsHistogram != "" {
			return c.MetricsHistogram
		}
		return c.getMetricsPrefix() + "_histogram"
	case "metrics_exponential_histogram":
		if c.MetricsExponentialHistogram != "" {
			return c.MetricsExponentialHistogram
		}
		return c.getMetricsPrefix() + "_exponential_histogram"
	case "metrics_summary":
		if c.MetricsSummary != "" {
			return c.MetricsSummary
		}
		return c.getMetricsPrefix() + "_summary"
	// Legacy unified metrics (for backward compatibility)
	case "metrics":
		if c.Metrics != "" {
			return c.Metrics
		}
		return "otel_metrics"
	default:
		return signalType
	}
}

// getMetricsPrefix returns the metrics table prefix.
func (c *TableNamesConfig) getMetricsPrefix() string {
	if c.Metrics != "" {
		return c.Metrics
	}
	return "otel_metrics"
}

// IsEnabled returns true if catalog registration is enabled.
func (c *CatalogConfig) IsEnabled() bool {
	return c.Type != "" && c.Type != CatalogTypeNone
}
