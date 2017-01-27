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

    ZipkinTracer(Builder builder) {
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
            state.put(ZipkinSpanBuilder.ID, zipkinContext.getId());
            state.put(ZipkinSpanBuilder.PARENT_ID, zipkinContext.getParentId());
            state.put(ZipkinSpanBuilder.TRACE_ID, zipkinContext.getParentId());
        }
        return state;
    }

    public Builder builder() {
        return new Builder();
    }

    public static class Builder {
        Random random;
        Reporter<Span> reporter;

        public Builder withRandom(Random random) {
            this.random = random;
            return this;
        }

        public Builder withReporter(Reporter<Span> reporter) {
            this.reporter = reporter;
            return this;
        }

        public ZipkinTracer build() {
            return new ZipkinTracer(this);
        }
    }
}
