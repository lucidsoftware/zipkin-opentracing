package io.opentracing.contrib.zipkin.propagation;

import io.opentracing.SpanContext;
import io.opentracing.contrib.zipkin.ZipkinSpanContext;
import io.opentracing.propagation.TextMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class TextMapPropagation {

    private TextMapPropagation() {
    }

    public static BiConsumer<SpanContext, TextMap> injector = (spanContext, carrier) -> {
        if (spanContext instanceof ZipkinSpanContext) {
            ZipkinSpanContext spanContext1 = (ZipkinSpanContext)spanContext;
            carrier.put("TraceId", Long.toHexString(spanContext1.getTraceId()));
            carrier.put("SpanId", Long.toHexString(spanContext1.getId()));
            Long parentId = spanContext1.getParentId();
            if (parentId != null) {
                carrier.put("ParentSpanId", Long.toHexString(parentId));
            }
            for (Map.Entry<String, String> baggageItem : spanContext.baggageItems()) {
                carrier.put("Baggage-" + baggageItem.getKey(), baggageItem.getValue());
            }
        }
    };

    public static Function<TextMap, SpanContext> extractor = carrier -> {
        Long traceId = null;
        Long spanId = null;
        Long parentSpanId = null;
        final Map<String, String> baggageItems = new HashMap<>();
        for (Map.Entry<String, String> entry : carrier) {
            switch (entry.getKey()) {
                case "TraceId":
                    traceId = Long.parseUnsignedLong(entry.getValue(), 4);
                    break;
                case "SpanId":
                    spanId = Long.parseUnsignedLong(entry.getValue(), 4);
                    break;
                case "ParentSpanId":
                    parentSpanId = Long.parseUnsignedLong(entry.getValue(), 4);
                    break;
                default:
                    if (entry.getKey().startsWith("Baggage-")) {
                        baggageItems.put(entry.getKey().substring("Baggage-".length()), entry.getValue());
                    }
            }
        }
        if (spanId != null && traceId != null) {
            return new ZipkinSpanContext(spanId, parentSpanId, traceId, baggageItems.entrySet());
        }
        return Collections::emptyList;
    };

}
