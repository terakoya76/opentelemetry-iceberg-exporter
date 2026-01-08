package arrow

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TracesConverter converts OTLP traces to Arrow record batches.
type TracesConverter struct {
	allocator memory.Allocator
	schema    *arrow.Schema
}

// NewTracesConverter creates a new TracesConverter.
func NewTracesConverter(allocator memory.Allocator) *TracesConverter {
	return &TracesConverter{
		allocator: allocator,
		schema:    TracesSchema(),
	}
}

// Schema returns the Arrow schema for traces.
func (c *TracesConverter) Schema() *arrow.Schema {
	return c.schema
}

// Convert converts OTLP traces to an Arrow record batch.
func (c *TracesConverter) Convert(traces ptrace.Traces) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(c.allocator, c.schema)
	defer builder.Release()

	resourceSpans := traces.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resource := rs.Resource()
		serviceName := extractServiceName(resource)
		resourceAttrs := attributesToJSON(resource.Attributes())
		resourceDroppedAttrsCount := resource.DroppedAttributesCount()

		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			scope := ss.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeAttrs := attributesToJSON(scope.Attributes())
			scopeDroppedAttrsCount := scope.DroppedAttributesCount()

			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if err := c.appendSpan(builder, span, serviceName, resourceAttrs, resourceDroppedAttrsCount, scopeName, scopeVersion, scopeAttrs, scopeDroppedAttrsCount); err != nil {
					return nil, fmt.Errorf("failed to append span: %w", err)
				}
			}
		}
	}

	return builder.NewRecordBatch(), nil
}

func (c *TracesConverter) appendSpan(
	builder *array.RecordBuilder,
	span ptrace.Span,
	serviceName string,
	resourceAttrs string,
	resourceDroppedAttrsCount uint32,
	scopeName string,
	scopeVersion string,
	scopeAttrs string,
	scopeDroppedAttrsCount uint32,
) error {
	// Field indices (must match TracesSchema order - follows OTel proto order)
	const (
		idxTraceId = iota
		idxSpanId
		idxTraceState
		idxParentSpanId
		idxSpanFlags
		idxSpanName
		idxSpanKind
		idxStartTimeUnixNano
		idxEndTimeUnixNano
		idxSpanAttrs
		idxDroppedAttrsCount
		idxEventsTimeUnixNano
		idxEventsName
		idxEventsAttrs
		idxEventsDroppedAttrsCount
		idxDroppedEventsCount
		idxLinksTraceId
		idxLinksSpanId
		idxLinksTraceState
		idxLinksAttrs
		idxLinksDroppedAttrsCount
		idxLinksFlags
		idxDroppedLinksCount
		idxStatusCode
		idxStatusMessage
		idxDuration
		idxServiceName
		idxResourceAttrs
		idxResourceDroppedAttrsCount
		idxScopeName
		idxScopeVersion
		idxScopeAttrs
		idxScopeDroppedAttrsCount
	)

	// Span identity fields (proto fields 1-7)
	builder.Field(idxTraceId).(*array.StringBuilder).Append(traceIDToString(span.TraceID()))
	builder.Field(idxSpanId).(*array.StringBuilder).Append(spanIDToString(span.SpanID()))

	traceState := span.TraceState().AsRaw()
	if traceState != "" {
		builder.Field(idxTraceState).(*array.StringBuilder).Append(traceState)
	} else {
		builder.Field(idxTraceState).(*array.StringBuilder).AppendNull()
	}

	parentSpanID := spanIDToString(span.ParentSpanID())
	if parentSpanID != "" && parentSpanID != "0000000000000000" {
		builder.Field(idxParentSpanId).(*array.StringBuilder).Append(parentSpanID)
	} else {
		builder.Field(idxParentSpanId).(*array.StringBuilder).AppendNull()
	}

	builder.Field(idxSpanFlags).(*array.Int64Builder).Append(int64(span.Flags()))
	builder.Field(idxSpanName).(*array.StringBuilder).Append(span.Name())
	builder.Field(idxSpanKind).(*array.StringBuilder).Append(spanKindToString(span.Kind()))

	// Timestamps (proto fields 8-9)
	builder.Field(idxStartTimeUnixNano).(*array.TimestampBuilder).Append(arrow.Timestamp(span.StartTimestamp().AsTime().UnixMicro()))
	builder.Field(idxEndTimeUnixNano).(*array.TimestampBuilder).Append(arrow.Timestamp(span.EndTimestamp().AsTime().UnixMicro()))

	// Attributes (proto fields 10-11)
	builder.Field(idxSpanAttrs).(*array.StringBuilder).Append(attributesToJSON(span.Attributes()))
	builder.Field(idxDroppedAttrsCount).(*array.Int64Builder).Append(int64(span.DroppedAttributesCount()))

	// Events (proto field 12)
	events := span.Events()
	eventsTimeUnixNanoBuilder := builder.Field(idxEventsTimeUnixNano).(*array.ListBuilder)
	eventsNameBuilder := builder.Field(idxEventsName).(*array.ListBuilder)
	eventsAttrsBuilder := builder.Field(idxEventsAttrs).(*array.ListBuilder)
	eventsDroppedAttrsCountBuilder := builder.Field(idxEventsDroppedAttrsCount).(*array.ListBuilder)

	eventsTimeUnixNanoBuilder.Append(true)
	eventsNameBuilder.Append(true)
	eventsAttrsBuilder.Append(true)
	eventsDroppedAttrsCountBuilder.Append(true)

	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		eventsTimeUnixNanoBuilder.ValueBuilder().(*array.TimestampBuilder).Append(
			arrow.Timestamp(event.Timestamp().AsTime().UnixMicro()),
		)
		eventsNameBuilder.ValueBuilder().(*array.StringBuilder).Append(event.Name())
		eventsAttrsBuilder.ValueBuilder().(*array.StringBuilder).Append(attributesToJSON(event.Attributes()))
		eventsDroppedAttrsCountBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(event.DroppedAttributesCount()))
	}

	// Dropped events count (proto field 13)
	builder.Field(idxDroppedEventsCount).(*array.Int64Builder).Append(int64(span.DroppedEventsCount()))

	// Links (proto field 14)
	links := span.Links()
	linksTraceIdBuilder := builder.Field(idxLinksTraceId).(*array.ListBuilder)
	linksSpanIdBuilder := builder.Field(idxLinksSpanId).(*array.ListBuilder)
	linksTraceStateBuilder := builder.Field(idxLinksTraceState).(*array.ListBuilder)
	linksAttrsBuilder := builder.Field(idxLinksAttrs).(*array.ListBuilder)
	linksDroppedAttrsCountBuilder := builder.Field(idxLinksDroppedAttrsCount).(*array.ListBuilder)
	linksFlagsBuilder := builder.Field(idxLinksFlags).(*array.ListBuilder)

	linksTraceIdBuilder.Append(true)
	linksSpanIdBuilder.Append(true)
	linksTraceStateBuilder.Append(true)
	linksAttrsBuilder.Append(true)
	linksDroppedAttrsCountBuilder.Append(true)
	linksFlagsBuilder.Append(true)

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linksTraceIdBuilder.ValueBuilder().(*array.StringBuilder).Append(traceIDToString(link.TraceID()))
		linksSpanIdBuilder.ValueBuilder().(*array.StringBuilder).Append(spanIDToString(link.SpanID()))
		linksTraceStateBuilder.ValueBuilder().(*array.StringBuilder).Append(link.TraceState().AsRaw())
		linksAttrsBuilder.ValueBuilder().(*array.StringBuilder).Append(attributesToJSON(link.Attributes()))
		linksDroppedAttrsCountBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(link.DroppedAttributesCount()))
		linksFlagsBuilder.ValueBuilder().(*array.Int64Builder).Append(int64(link.Flags()))
	}

	// Dropped links count (proto field 15)
	builder.Field(idxDroppedLinksCount).(*array.Int64Builder).Append(int64(span.DroppedLinksCount()))

	// Status (proto field 16)
	status := span.Status()
	statusCode := statusCodeToString(status.Code())
	if statusCode != "" {
		builder.Field(idxStatusCode).(*array.StringBuilder).Append(statusCode)
	} else {
		builder.Field(idxStatusCode).(*array.StringBuilder).AppendNull()
	}

	statusMessage := status.Message()
	if statusMessage != "" {
		builder.Field(idxStatusMessage).(*array.StringBuilder).Append(statusMessage)
	} else {
		builder.Field(idxStatusMessage).(*array.StringBuilder).AppendNull()
	}

	// Duration (calculated, not in proto)
	duration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds()
	builder.Field(idxDuration).(*array.Int64Builder).Append(duration)

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

// Helper functions

func attributesToJSON(attrs pcommon.Map) string {
	m := make(map[string]any)
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = valueToInterface(v)
		return true
	})
	data, _ := json.Marshal(m)
	return string(data)
}

func valueToInterface(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeSlice:
		slice := v.Slice()
		result := make([]any, slice.Len())
		for i := 0; i < slice.Len(); i++ {
			result[i] = valueToInterface(slice.At(i))
		}
		return result
	case pcommon.ValueTypeMap:
		m := make(map[string]any)
		v.Map().Range(func(k string, v pcommon.Value) bool {
			m[k] = valueToInterface(v)
			return true
		})
		return m
	default:
		return nil
	}
}

func traceIDToString(id pcommon.TraceID) string {
	return hex.EncodeToString(id[:])
}

func spanIDToString(id pcommon.SpanID) string {
	return hex.EncodeToString(id[:])
}

func spanKindToString(kind ptrace.SpanKind) string {
	switch kind {
	case ptrace.SpanKindUnspecified:
		return "UNSPECIFIED"
	case ptrace.SpanKindInternal:
		return "INTERNAL"
	case ptrace.SpanKindServer:
		return "SERVER"
	case ptrace.SpanKindClient:
		return "CLIENT"
	case ptrace.SpanKindProducer:
		return "PRODUCER"
	case ptrace.SpanKindConsumer:
		return "CONSUMER"
	default:
		return "UNKNOWN"
	}
}

func statusCodeToString(code ptrace.StatusCode) string {
	switch code {
	case ptrace.StatusCodeUnset:
		return ""
	case ptrace.StatusCodeOk:
		return "OK"
	case ptrace.StatusCodeError:
		return "ERROR"
	default:
		return ""
	}
}

// TimestampToPartition converts a timestamp to a partition path.
func TimestampToPartition(t time.Time, format string, timezone string) string {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format(format)
}
