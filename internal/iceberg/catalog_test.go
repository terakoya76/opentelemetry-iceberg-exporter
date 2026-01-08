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
