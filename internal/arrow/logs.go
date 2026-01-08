package arrow

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// LogsConverter converts OTLP logs to Arrow record batches.
type LogsConverter struct {
	allocator memory.Allocator
	schema    *arrow.Schema
}

// NewLogsConverter creates a new LogsConverter.
func NewLogsConverter(allocator memory.Allocator) *LogsConverter {
	return &LogsConverter{
		allocator: allocator,
		schema:    LogsSchema(),
	}
}

// Schema returns the Arrow schema for logs.
func (c *LogsConverter) Schema() *arrow.Schema {
	return c.schema
}

// Convert converts OTLP logs to an Arrow record batch.
func (c *LogsConverter) Convert(logs plog.Logs) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(c.allocator, c.schema)
	defer builder.Release()

	resourceLogs := logs.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		resource := rl.Resource()
		serviceName := extractServiceName(resource)
		resourceAttrs := attributesToJSON(resource.Attributes())
		resourceDroppedAttrsCount := resource.DroppedAttributesCount()

		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			scope := sl.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeAttrs := attributesToJSON(scope.Attributes())
			scopeDroppedAttrsCount := scope.DroppedAttributesCount()

			logRecords := sl.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logRecord := logRecords.At(k)
				if err := c.appendLog(builder, logRecord, serviceName, resourceAttrs, resourceDroppedAttrsCount, scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount); err != nil {
					return nil, fmt.Errorf("failed to append log: %w", err)
				}
			}
		}
	}

	return builder.NewRecordBatch(), nil
}

func (c *LogsConverter) appendLog(
	builder *array.RecordBuilder,
	logRecord plog.LogRecord,
	serviceName string,
	resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName string,
	scopeVersion string,
	scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) error {
	// Field indices (must match LogsSchema order - follows OTel proto order)
	const (
		idxTimeUnixNano = iota
		idxSeverityNumber
		idxSeverityText
		idxBody
		idxLogAttrs
		idxDroppedAttrsCount
		idxLogFlags
		idxTraceId
		idxSpanId
		idxObservedTimeUnixNano
		idxEventName
		idxServiceName
		idxResourceAttrs
		idxResourceDroppedAttrsCount
		idxScopeName
		idxScopeVersion
		idxScopeAttrs
		idxScopeDroppedAttrsCount
	)

	// LogRecord fields in proto order

	// TimeUnixNano (proto field 1)
	builder.Field(idxTimeUnixNano).(*array.TimestampBuilder).Append(
		arrow.Timestamp(logRecord.Timestamp().AsTime().UnixMicro()),
	)

	// SeverityNumber (proto field 2)
	builder.Field(idxSeverityNumber).(*array.Int32Builder).Append(int32(logRecord.SeverityNumber()))

	// SeverityText (proto field 3)
	severityText := logRecord.SeverityText()
	if severityText != "" {
		builder.Field(idxSeverityText).(*array.StringBuilder).Append(severityText)
	} else {
		builder.Field(idxSeverityText).(*array.StringBuilder).AppendNull()
	}

	// Body (proto field 5) - convert to JSON string
	body := logValueToJSON(logRecord.Body())
	builder.Field(idxBody).(*array.StringBuilder).Append(body)

	// LogAttributes (proto field 6)
	builder.Field(idxLogAttrs).(*array.StringBuilder).Append(attributesToJSON(logRecord.Attributes()))

	// DroppedAttributesCount (proto field 7)
	builder.Field(idxDroppedAttrsCount).(*array.Int64Builder).Append(int64(logRecord.DroppedAttributesCount()))

	// LogFlags (proto field 8)
	builder.Field(idxLogFlags).(*array.Int64Builder).Append(int64(logRecord.Flags()))

	// TraceId (proto field 9)
	traceID := traceIDToString(logRecord.TraceID())
	if traceID != "" && traceID != "00000000000000000000000000000000" {
		builder.Field(idxTraceId).(*array.StringBuilder).Append(traceID)
	} else {
		builder.Field(idxTraceId).(*array.StringBuilder).AppendNull()
	}

	// SpanId (proto field 10)
	spanID := spanIDToString(logRecord.SpanID())
	if spanID != "" && spanID != "0000000000000000" {
		builder.Field(idxSpanId).(*array.StringBuilder).Append(spanID)
	} else {
		builder.Field(idxSpanId).(*array.StringBuilder).AppendNull()
	}

	// ObservedTimeUnixNano (proto field 11)
	observedTs := logRecord.ObservedTimestamp()
	if observedTs != 0 {
		builder.Field(idxObservedTimeUnixNano).(*array.TimestampBuilder).Append(
			arrow.Timestamp(observedTs.AsTime().UnixMicro()),
		)
	} else {
		builder.Field(idxObservedTimeUnixNano).(*array.TimestampBuilder).AppendNull()
	}

	// EventName (proto field 12) - placeholder for future OTel proto field
	// The event_name field is not yet available in plog.LogRecord
	builder.Field(idxEventName).(*array.StringBuilder).AppendNull()

	// Resource fields
	if serviceName != "" {
		builder.Field(idxServiceName).(*array.StringBuilder).Append(serviceName)
	} else {
		builder.Field(idxServiceName).(*array.StringBuilder).AppendNull()
	}
	builder.Field(idxResourceAttrs).(*array.StringBuilder).Append(resourceAttrs)
	builder.Field(idxResourceDroppedAttrsCount).(*array.Int64Builder).Append(int64(resourceDroppedAttrsCount))

	// Scope fields
	if scopeName != "" {
		builder.Field(idxScopeName).(*array.StringBuilder).Append(scopeName)
	} else {
		builder.Field(idxScopeName).(*array.StringBuilder).AppendNull()
	}

	if scopeVersion != "" {
		builder.Field(idxScopeVersion).(*array.StringBuilder).Append(scopeVersion)
	} else {
		builder.Field(idxScopeVersion).(*array.StringBuilder).AppendNull()
	}

	builder.Field(idxScopeAttrs).(*array.StringBuilder).Append(scopeAttrs)
	builder.Field(idxScopeDroppedAttrsCount).(*array.Int64Builder).Append(int64(scopeDroppedAttrsCount))

	return nil
}

func logValueToJSON(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return fmt.Sprintf("%d", v.Int())
	case pcommon.ValueTypeDouble:
		return fmt.Sprintf("%f", v.Double())
	case pcommon.ValueTypeBool:
		if v.Bool() {
			return "true"
		}
		return "false"
	case pcommon.ValueTypeBytes:
		return string(v.Bytes().AsRaw())
	case pcommon.ValueTypeSlice, pcommon.ValueTypeMap:
		data, _ := json.Marshal(valueToInterface(v))
		return string(data)
	default:
		return ""
	}
}
