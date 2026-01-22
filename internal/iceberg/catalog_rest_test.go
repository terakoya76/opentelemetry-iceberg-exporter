package iceberg

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	arrowconv "github.com/terakoya76/opentelemetry-iceberg-exporter/internal/arrow"
)

func Test_arrowSchemaToIcebergSchema(t *testing.T) {
	t.Run("AllSignalTypes", func(t *testing.T) {
		tests := []struct {
			name   string
			schema *arrow.Schema
		}{
			{"Traces", arrowconv.TracesSchema()},
			{"Logs", arrowconv.LogsSchema()},
			{"Gauge", arrowconv.MetricsGaugeSchema()},
			{"Sum", arrowconv.MetricsSumSchema()},
			{"Histogram", arrowconv.MetricsHistogramSchema()},
			{"ExponentialHistogram", arrowconv.MetricsExponentialHistogramSchema()},
			{"Summary", arrowconv.MetricsSummarySchema()},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				icebergSchema, err := arrowSchemaToIcebergSchema(tt.schema)
				require.NoError(t, err)
				require.NotNil(t, icebergSchema)

				// Verify field count matches
				assert.Equal(t, tt.schema.NumFields(), len(icebergSchema.Fields()))

				// Verify all fields have positive IDs (assigned by AssignFreshSchemaIDs)
				// and field names match
				for i, arrowField := range tt.schema.Fields() {
					icebergField := icebergSchema.Fields()[i]

					assert.Greater(t, icebergField.ID, 0,
						"Field %s: ID should be positive", arrowField.Name)
					assert.Equal(t, arrowField.Name, icebergField.Name,
						"Field name mismatch at index %d", i)
				}
			})
		}
	})

	t.Run("WithoutFieldIDMetadata", func(t *testing.T) {
		// Create a schema without PARQUET:field_id metadata
		// With ArrowSchemaToIcebergWithFreshIDs, this should work - it assigns fresh IDs
		fields := []arrow.Field{
			{Name: "test_field", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		schemaWithoutFieldIDs := arrow.NewSchema(fields, nil)

		icebergSchema, err := arrowSchemaToIcebergSchema(schemaWithoutFieldIDs)
		require.NoError(t, err)
		require.NotNil(t, icebergSchema)

		// Verify fresh IDs are assigned
		assert.Equal(t, 1, len(icebergSchema.Fields()))
		assert.Equal(t, "test_field", icebergSchema.Fields()[0].Name)
		assert.Greater(t, icebergSchema.Fields()[0].ID, 0, "Should have fresh ID assigned")
	})

	t.Run("ListTypeGetsElementID", func(t *testing.T) {
		// Create a schema with a list field (without PARQUET:field_id metadata)
		// ArrowSchemaToIcebergWithFreshIDs should assign fresh IDs to both
		// the field and the list element
		fields := []arrow.Field{
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		}
		schemaWithList := arrow.NewSchema(fields, nil)

		icebergSchema, err := arrowSchemaToIcebergSchema(schemaWithList)
		require.NoError(t, err)
		require.NotNil(t, icebergSchema)

		assert.Equal(t, 1, len(icebergSchema.Fields()))
		tagsField := icebergSchema.Fields()[0]
		assert.Equal(t, "tags", tagsField.Name)
		assert.Greater(t, tagsField.ID, 0, "Field should have fresh ID")

		// Verify it's a list type with element ID
		listType, ok := tagsField.Type.(*iceberg.ListType)
		require.True(t, ok, "should be a ListType")
		assert.Greater(t, listType.ElementID, 0, "List element should have fresh ID")
		assert.NotEqual(t, tagsField.ID, listType.ElementID, "Field ID and element ID should be different")
	})
}

// mockErrorResponse simulates iceberg-go's errorResponse behavior
type mockErrorResponse struct {
	Type    string
	Message string
}

func (e mockErrorResponse) Error() string {
	return e.Type + ": " + e.Message
}

func Test_normalizeIcebergError(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantContains []string
	}{
		{
			name:         "nil error",
			err:          nil,
			wantContains: []string{"unknown error", "nil"},
		},
		{
			name:         "empty error message",
			err:          errors.New(""),
			wantContains: []string{"error with no message"},
		},
		{
			name:         "iceberg-go empty errorResponse (colon space)",
			err:          mockErrorResponse{Type: "", Message: ""},
			wantContains: []string{"REST API returned error with no details"},
		},
		{
			name:         "iceberg-go partial errorResponse (type only)",
			err:          mockErrorResponse{Type: "BadRequest", Message: ""},
			wantContains: []string{"BadRequest"},
		},
		{
			name:         "connection refused error",
			err:          errors.New("dial tcp: connection refused"),
			wantContains: []string{"connection refused", "ensure the REST catalog service is running"},
		},
		{
			name:         "no such host error",
			err:          errors.New("dial tcp: lookup catalog: no such host"),
			wantContains: []string{"no such host", "check the catalog URI hostname"},
		},
		{
			name:         "timeout error",
			err:          fmt.Errorf("context deadline exceeded (timeout)"),
			wantContains: []string{"timeout", "catalog service may be overloaded"},
		},
		{
			name:         "normal error message",
			err:          errors.New("table already exists"),
			wantContains: []string{"table already exists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeIcebergError(tt.err)

			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("normalizeIcebergError() = %q, should contain %q", result, want)
				}
			}
		})
	}
}
