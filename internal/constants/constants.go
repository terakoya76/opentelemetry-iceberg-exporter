package constants

// ExporterName is the canonical name used by the exporter for all storage paths.
// This constant is the single source of truth for the exporter name prefix.
const ExporterName = "opentelemetry-iceberg-exporter"

// Granularity constants for partition granularity levels.
const (
	GranularityHourly  = "hourly"
	GranularityDaily   = "daily"
	GranularityMonthly = "monthly"
)
