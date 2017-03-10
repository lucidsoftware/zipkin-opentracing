package io.opentracing.contrib.zipkin.propagation;

import io.opentracing.SpanContext;
import io.opentracing.contrib.zipkin.ZipkinSpanContext;
import io.opentracing.propagation.TextMap;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class HttpHeadersPropagation {

    private HttpHeadersPropagation() {
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            assert false;
            return null;
        }
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            assert false;
            return null;
        }
    }

    public static BiConsumer<SpanContext, TextMap> injector = (spanContext, carrier) -> {
        if (spanContext instanceof ZipkinSpanContext) {
            ZipkinSpanContext spanContext1 = (ZipkinSpanContext)spanContext;
            carrier.put("X-B3-TraceId", Long.toHexString(spanContext1.getTraceId()));
            carrier.put("X-B3-SpanId", Long.toHexString(spanContext1.getId()));
            Long parentId = spanContext1.getParentId();
            if (parentId != null) {
                carrier.put("X-B3-ParentSpanId", Long.toHexString(parentId));
            }
            for (Map.Entry<String, String> baggageItem : spanContext.baggageItems()) {
                carrier.put("X-B3-Baggage-" + encode(baggageItem.getKey()), encode(baggageItem.getValue()));
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
                case "X-B3-TraceId":
                    traceId = Long.parseUnsignedLong(entry.getValue(), 16);
                    break;
                case "X-B3-SpanId":
                    spanId = Long.parseUnsignedLong(entry.getValue(), 16);
                    break;
                case "X-B3-ParentSpanId":
                    parentSpanId = Long.parseUnsignedLong(entry.getValue(), 16);
                    break;
                default:
                    if (entry.getKey().startsWith("X-B3-Baggage-")) {
                        baggageItems.put(decode(entry.getKey().substring("X-B3-Baggage-".length())), decode(entry.getValue()));
                    }
            }
        }
        if (spanId != null && traceId != null) {
            return new ZipkinSpanContext(spanId, parentSpanId, traceId, baggageItems.entrySet());
        }
        return Collections::emptyList;
    };

}
