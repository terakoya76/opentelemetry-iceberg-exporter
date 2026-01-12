package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
