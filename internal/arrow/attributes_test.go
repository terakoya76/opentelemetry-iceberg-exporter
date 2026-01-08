package arrow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestExtractServiceName(t *testing.T) {
	testCases := []struct {
		name     string
		setup    func() pcommon.Resource
		expected string
	}{
		{
			name: "service.name present",
			setup: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr("service.name", "my-service")
				return res
			},
			expected: "my-service",
		},
		{
			name: "service.name absent",
			setup: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr("other.attr", "value")
				return res
			},
			expected: "",
		},
		{
			name: "empty resource",
			setup: func() pcommon.Resource {
				return pcommon.NewResource()
			},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.setup()
			result := extractServiceName(res)
			assert.Equal(t, tc.expected, result)
		})
	}
}
