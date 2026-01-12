package iceberg

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
