package io.opentracing.impl;

import io.opentracing.SpanContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinTracer extends AbstractTracer {

    private final Random random;
    private final Reporter<Span> reporter;

    private ZipkinTracer(Builder builder) {
        this.random = builder.random;
        this.reporter = builder.reporter;
    }

    AbstractSpanBuilder createSpanBuilder(String operationName) {
        return new ZipkinSpanBuilder(operationName, random, reporter);
    }

    public Map<String, Object> getTraceState(SpanContext spanContext) {
        Map<String, Object> state = new HashMap<>();
        if (spanContext instanceof ZipkinSpanContext) {
            ZipkinSpanContext zipkinContext = (ZipkinSpanContext)spanContext;
            Long id = zipkinContext.getId();
            if (id != null) {
                state.put(ZipkinSpanBuilder.ID, id);
            }
            Long parentId = zipkinContext.getParentId();
            if (parentId != null) {
                state.put(ZipkinSpanBuilder.PARENT_ID, parentId);
            }
            Long traceId = zipkinContext.getTraceId();
            if (traceId != null) {
                state.put(ZipkinSpanBuilder.TRACE_ID, traceId);
            }
        }
        return state;
    }

    public static Builder builder(Reporter<Span> reporter) {
        return new Builder(reporter);
    }

    public static class Builder {
        Random random;
        Reporter<Span> reporter;

        public Builder(Reporter<Span> reporter) {
            random = new Random();
            this.reporter = reporter;
        }

        public Builder withRandom(Random random) {
            this.random = random;
            return this;
        }

        public ZipkinTracer build() {
            return new ZipkinTracer(this);
        }
    }
}
