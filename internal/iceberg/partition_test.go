package iceberg

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/terakoya76/opentelemetry-iceberg-exporter/internal/constants"
)

func TestOTELPartitionSpec(t *testing.T) {
	tests := []struct {
		name              string
		granularity       string
		expectedTransform string
		expectedName      string
	}{
		{"Hourly granularity", "hourly", "hour", "hour"},
		{"Daily granularity", "daily", "day", "day"},
		{"Monthly granularity", "monthly", "month", "month"},
		{"Empty defaults to hourly", "", "hour", "hour"},
		{"Unknown defaults to hourly", "unknown", "hour", "hour"},
	}

	timestampColumn := "start_time_unix_nano"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := OTELPartitionSpec(timestampColumn, tt.granularity)

			// Should have exactly 1 partition field (R2 Data Catalog requirement)
			require.Len(t, spec.Fields, 1, "should have exactly 1 partition field")

			field := spec.Fields[0]
			assert.Equal(t, tt.expectedTransform, field.Transform, "transform mismatch")
			assert.Equal(t, tt.expectedName, field.Name, "partition name mismatch")
			assert.Equal(t, timestampColumn, field.SourceColumn, "source column mismatch")
		})
	}
}

func TestPartitionValues_ToMap(t *testing.T) {
	tests := []struct {
		name     string
		pv       PartitionValues
		expected map[string]string
	}{
		{
			name: "Full partition values (hourly)",
			pv: PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "14",
			},
			expected: map[string]string{
				"year":  "2024",
				"month": "06",
				"day":   "15",
				"hour":  "14",
			},
		},
		{
			name: "Daily granularity (no hour)",
			pv: PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
			},
			expected: map[string]string{
				"year":  "2024",
				"month": "06",
				"day":   "15",
			},
		},
		{
			name: "Monthly granularity (no day/hour)",
			pv: PartitionValues{
				Year:  "2024",
				Month: "06",
			},
			expected: map[string]string{
				"year":  "2024",
				"month": "06",
			},
		},
		{
			name:     "Empty partition values",
			pv:       PartitionValues{},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.pv.ToMap()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPartitionValues_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		pv       PartitionValues
		expected bool
	}{
		{
			name:     "Empty partition values",
			pv:       PartitionValues{},
			expected: true,
		},
		{
			name: "With year only",
			pv: PartitionValues{
				Year: "2024",
			},
			expected: false,
		},
		{
			name: "With month only",
			pv: PartitionValues{
				Month: "06",
			},
			expected: false,
		},
		{
			name: "Full partition values",
			pv: PartitionValues{
				Year:  "2024",
				Month: "06",
				Day:   "15",
				Hour:  "14",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.pv.IsZero())
		})
	}
}

func TestNewPathGenerator(t *testing.T) {
	t.Run("Valid timezone", func(t *testing.T) {
		cfg := PathConfig{
			Granularity: constants.GranularityHourly,
			Timezone:    "America/New_York",
		}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)
		assert.NotNil(t, gen)
		assert.Equal(t, "America/New_York", gen.Location().String())
	})

	t.Run("Invalid timezone falls back to UTC", func(t *testing.T) {
		cfg := PathConfig{
			Granularity: constants.GranularityHourly,
			Timezone:    "Invalid/Timezone",
		}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)
		assert.NotNil(t, gen)
		assert.Equal(t, "UTC", gen.Location().String())
	})
}

func TestPathGenerator_GeneratePath(t *testing.T) {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	t.Run("Hourly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityHourly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Contains(t, path, "opentelemetry-iceberg-exporter/otel_traces/data")
		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.Contains(t, path, "day=15")
		assert.Contains(t, path, "hour=10")
		assert.True(t, strings.HasSuffix(path, ".parquet"))
	})

	t.Run("Daily granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityDaily, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_logs",
			Timestamp: ts,
		})

		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.Contains(t, path, "day=15")
		assert.NotContains(t, path, "hour=")
	})

	t.Run("Monthly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityMonthly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		path := gen.GeneratePath(PathOptions{
			TableName: "otel_metrics",
			Timestamp: ts,
		})

		assert.Contains(t, path, "year=2024")
		assert.Contains(t, path, "month=06")
		assert.NotContains(t, path, "day=")
		assert.NotContains(t, path, "hour=")
	})

	t.Run("Timezone affects partition", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityHourly, Timezone: "America/New_York"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		// 10:30 UTC = 6:30 EST (during summer, 4 hours behind)
		path := gen.GeneratePath(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Contains(t, path, "hour=06")
	})
}

func TestPathGenerator_GeneratePartitionPath(t *testing.T) {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	t.Run("Hourly partition path", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityHourly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		partPath := gen.GeneratePartitionPath(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Equal(t, "year=2024/month=06/day=15/hour=10", partPath)
	})

	t.Run("Daily partition path", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityDaily, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		partPath := gen.GeneratePartitionPath(PathOptions{
			TableName: "otel_logs",
			Timestamp: ts,
		})

		assert.Equal(t, "year=2024/month=06/day=15", partPath)
	})

	t.Run("Monthly partition path", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityMonthly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		partPath := gen.GeneratePartitionPath(PathOptions{
			TableName: "otel_metrics",
			Timestamp: ts,
		})

		assert.Equal(t, "year=2024/month=06", partPath)
	})
}

func TestPathGenerator_ExtractPartitionValues(t *testing.T) {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	t.Run("Hourly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityHourly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_traces",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Equal(t, "15", values.Day)
		assert.Equal(t, "10", values.Hour)
	})

	t.Run("Daily granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityDaily, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_logs",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Equal(t, "15", values.Day)
		assert.Empty(t, values.Hour)
	})

	t.Run("Monthly granularity", func(t *testing.T) {
		cfg := PathConfig{Granularity: constants.GranularityMonthly, Timezone: "UTC"}
		gen, err := NewPathGenerator(cfg)
		require.NoError(t, err)

		values := gen.ExtractPartitionValues(PathOptions{
			TableName: "otel_metrics",
			Timestamp: ts,
		})

		assert.Equal(t, "2024", values.Year)
		assert.Equal(t, "06", values.Month)
		assert.Empty(t, values.Day)
		assert.Empty(t, values.Hour)
	})
}
