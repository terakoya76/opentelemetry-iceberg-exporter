package iceberg

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
)

// PartitionValues represents partition column values for Iceberg tables.
// Empty strings indicate the partition value is not set.
type PartitionValues struct {
	// Year is the year partition value (e.g., "2024").
	Year string

	// Month is the month partition value (e.g., "01").
	Month string

	// Day is the day partition value (e.g., "15").
	// Empty if granularity is monthly.
	Day string

	// Hour is the hour partition value (e.g., "08").
	// Empty if granularity is monthly or daily.
	Hour string
}

// ToMap converts PartitionValues to a map[string]string.
// Only non-empty values are included in the map.
func (p PartitionValues) ToMap() map[string]string {
	m := make(map[string]string)
	if p.Year != "" {
		m["year"] = p.Year
	}
	if p.Month != "" {
		m["month"] = p.Month
	}
	if p.Day != "" {
		m["day"] = p.Day
	}
	if p.Hour != "" {
		m["hour"] = p.Hour
	}
	return m
}

// IsZero returns true if all partition values are empty.
func (p PartitionValues) IsZero() bool {
	return p.Year == "" && p.Month == "" && p.Day == "" && p.Hour == ""
}

// PartitionSpec defines the partition specification for a table.
type PartitionSpec struct {
	// Fields are the partition fields.
	Fields []PartitionField
}

// PartitionField defines a single partition field.
type PartitionField struct {
	// Name is the partition column name.
	Name string

	// Transform is the partition transform (identity, year, month, day, hour, etc.).
	Transform string

	// SourceColumn is the source column name for time-based transforms.
	SourceColumn string
}

// OTELPartitionSpec returns a partition spec for OTEL data based on granularity.
func OTELPartitionSpec(timestampColumn, granularity string) PartitionSpec {
	// Map granularity config to transform and partition name
	var transform, name string
	switch granularity {
	case "monthly":
		transform = "month"
		name = "month"
	case "daily":
		transform = "day"
		name = "day"
	case "hourly", "":
		transform = "hour"
		name = "hour"
	default:
		// Default to hourly for unknown values
		transform = "hour"
		name = "hour"
	}

	return PartitionSpec{
		Fields: []PartitionField{
			{Name: name, Transform: transform, SourceColumn: timestampColumn},
		},
	}
}

// PathConfig holds configuration for path generation.
type PathConfig struct {
	// Granularity is the partition granularity: "hourly", "daily", "monthly"
	Granularity string

	// Timezone is the timezone for partition values.
	Timezone string
}

// PathOptions contains options for generating a file path.
type PathOptions struct {
	// TableName is the Iceberg table name.
	TableName string

	// Timestamp is the data timestamp for partitioning.
	Timestamp time.Time
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
	partitionPath := g.GeneratePartitionPath(opts)

	// Filename: {sequence}-{task_id}-{uuid}.parquet
	// Iceberg convention: 00000-0-{uuid}.parquet for simplicity
	filename := fmt.Sprintf("00000-0-%s.parquet", uuid.New().String()[:8])

	return fmt.Sprintf("%s/%s/%s", basePath, partitionPath, filename)
}

// GeneratePartitionPath generates the Hive-style partition path.
// This is exported so it can be used by components that need just the partition path.
func (g *PathGenerator) GeneratePartitionPath(opts PathOptions) string {
	ts := opts.Timestamp.In(g.loc)

	var parts []string

	// Add time-based partitions
	switch g.config.Granularity {
	case constants.GranularityMonthly:
		parts = append(parts,
			fmt.Sprintf("year=%d", ts.Year()),
			fmt.Sprintf("month=%02d", ts.Month()),
		)
	case constants.GranularityDaily:
		parts = append(parts,
			fmt.Sprintf("year=%d", ts.Year()),
			fmt.Sprintf("month=%02d", ts.Month()),
			fmt.Sprintf("day=%02d", ts.Day()),
		)
	case constants.GranularityHourly:
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
func (g *PathGenerator) ExtractPartitionValues(opts PathOptions) PartitionValues {
	ts := opts.Timestamp.In(g.loc)

	values := PartitionValues{
		Year:  fmt.Sprintf("%d", ts.Year()),
		Month: fmt.Sprintf("%02d", ts.Month()),
	}

	// Add day partition value for daily or hourly granularity
	if g.config.Granularity == constants.GranularityDaily || g.config.Granularity == constants.GranularityHourly {
		values.Day = fmt.Sprintf("%02d", ts.Day())
	}

	// Add hour partition value for hourly granularity
	if g.config.Granularity == constants.GranularityHourly {
		values.Hour = fmt.Sprintf("%02d", ts.Hour())
	}

	return values
}

// Config returns the generator's configuration.
func (g *PathGenerator) Config() PathConfig {
	return g.config
}

// Location returns the generator's time location.
func (g *PathGenerator) Location() *time.Location {
	return g.loc
}
