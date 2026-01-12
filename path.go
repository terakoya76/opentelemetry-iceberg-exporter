package icebergexporter

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/iceberg"
)

// PathConfig holds configuration for path generation.
type PathConfig struct {
	// Granularity is the partition granularity: "hourly", "daily", "monthly"
	Granularity string

	// Timezone is the timezone for partition values.
	Timezone string

	// IncludeServiceName adds service_name to the partition path.
	IncludeServiceName bool
}

// DefaultPathConfig returns the default path configuration.
func DefaultPathConfig() PathConfig {
	return PathConfig{
		Granularity:        "hourly",
		Timezone:           "UTC",
		IncludeServiceName: false,
	}
}

// PathGenerator generates Iceberg-compatible file paths.
type PathGenerator struct {
	config PathConfig
	loc    *time.Location
}

// NewPathGenerator creates a new PathGenerator.
func NewPathGenerator(config PathConfig) (*PathGenerator, error) {
	loc, err := time.LoadLocation(config.Timezone)
	if err != nil {
		loc = time.UTC
	}

	return &PathGenerator{
		config: config,
		loc:    loc,
	}, nil
}

// GeneratePath generates an Iceberg-compatible file path.
// Path format: {ExporterName}/{table}/data/{partition_path}/{filename}.parquet
func (g *PathGenerator) GeneratePath(opts PathOptions) string {
	// Base path: {ExporterName}/{table}/data
	basePath := fmt.Sprintf("%s/%s/data", constants.ExporterName, opts.TableName)

	// Partition path
	partitionPath := g.generatePartitionPath(opts)

	// Filename: {sequence}-{task_id}-{uuid}.parquet
	// Iceberg convention: 00000-0-{uuid}.parquet for simplicity
	filename := fmt.Sprintf("00000-0-%s.parquet", uuid.New().String()[:8])

	return fmt.Sprintf("%s/%s/%s", basePath, partitionPath, filename)
}

// PathOptions contains options for generating a file path.
type PathOptions struct {
	// TableName is the Iceberg table name.
	TableName string

	// Timestamp is the data timestamp for partitioning.
	Timestamp time.Time

	// ServiceName is the service name (optional, for additional partitioning).
	ServiceName string
}

// generatePartitionPath generates the Hive-style partition path.
func (g *PathGenerator) generatePartitionPath(opts PathOptions) string {
	ts := opts.Timestamp.In(g.loc)

	var parts []string

	// Add service name partition if configured and present
	if g.config.IncludeServiceName && opts.ServiceName != "" {
		sanitized := sanitizePartitionValue(opts.ServiceName)
		parts = append(parts, fmt.Sprintf("service_name=%s", sanitized))
	}

	// Add time-based partitions
	switch g.config.Granularity {
	case "monthly":
		parts = append(parts,
			fmt.Sprintf("year=%d", ts.Year()),
			fmt.Sprintf("month=%02d", ts.Month()),
		)
	case "daily":
		parts = append(parts,
			fmt.Sprintf("year=%d", ts.Year()),
			fmt.Sprintf("month=%02d", ts.Month()),
			fmt.Sprintf("day=%02d", ts.Day()),
		)
	case "hourly":
		fallthrough
	default:
		parts = append(parts,
			fmt.Sprintf("year=%d", ts.Year()),
			fmt.Sprintf("month=%02d", ts.Month()),
			fmt.Sprintf("day=%02d", ts.Day()),
			fmt.Sprintf("hour=%02d", ts.Hour()),
		)
	}

	return strings.Join(parts, "/")
}

// ExtractPartitionValues extracts partition values for Iceberg catalog registration.
func (g *PathGenerator) ExtractPartitionValues(opts PathOptions) iceberg.PartitionValues {
	ts := opts.Timestamp.In(g.loc)

	values := iceberg.PartitionValues{
		Year:  fmt.Sprintf("%d", ts.Year()),
		Month: fmt.Sprintf("%02d", ts.Month()),
	}

	// Add day partition value for daily or hourly granularity
	if g.config.Granularity == "daily" || g.config.Granularity == "hourly" {
		values.Day = fmt.Sprintf("%02d", ts.Day())
	}

	// Add hour partition value for hourly granularity
	if g.config.Granularity == "hourly" {
		values.Hour = fmt.Sprintf("%02d", ts.Hour())
	}

	return values
}

// sanitizePartitionValue sanitizes a value for use in partition paths.
// Replaces invalid characters with underscores.
var invalidPartitionChars = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

func sanitizePartitionValue(value string) string {
	// Replace invalid characters
	sanitized := invalidPartitionChars.ReplaceAllString(value, "_")

	// Collapse multiple underscores
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}

	// Trim leading/trailing underscores
	sanitized = strings.Trim(sanitized, "_")

	// Ensure non-empty
	if sanitized == "" {
		sanitized = "unknown"
	}

	return sanitized
}
