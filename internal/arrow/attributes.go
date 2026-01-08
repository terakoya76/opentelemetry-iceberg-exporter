package arrow

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func extractServiceName(resource pcommon.Resource) string {
	if val, ok := resource.Attributes().Get("service.name"); ok {
		return val.AsString()
	}
	return ""
}
