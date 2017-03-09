package io.opentracing.contrib.zipkin;

import io.opentracing.SpanContext;
import java.util.Map;

public final class ZipkinSpanContext implements SpanContext {

    private final long id;
    private final Long parentId;
    private final long traceId;
    private final Iterable<Map.Entry<String, String>> baggageItems;

    public ZipkinSpanContext(long id, Long parentId, long traceId, Iterable<Map.Entry<String, String>> baggageItems) {
        this.id = id;
        this.parentId = parentId;
        this.traceId = traceId;
        this.baggageItems = baggageItems;
    }

    public long getId() {
        return id;
    }

    public Long getParentId() {
        return parentId;
    }

    public long getTraceId() {
        return traceId;
    }

    public Iterable<Map.Entry<String, String>> baggageItems() {
        return baggageItems;
    }

}
