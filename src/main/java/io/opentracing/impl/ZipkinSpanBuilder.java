package io.opentracing.impl;

import io.opentracing.SpanContext;
import java.util.Map;
import java.util.Random;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinSpanBuilder extends AbstractSpanBuilder {

    public static final String ID = "span_id";
    public static final String TRACE_ID = "trace_id";
    public static final String PARENT_ID = "parent_id";

    private static Long convertLong(Object value) {
        return value instanceof Number ? ((Number)value).longValue() : Long.parseLong(value.toString());
    }

    private final Span.Builder builder;
    private final Reporter<Span> reporter;

    public ZipkinSpanBuilder(String operationName, Random random, Reporter<Span> reporter) {
        super(operationName);
        this.builder = Span.builder().id(random.nextLong()).name(operationName);
        this.reporter = reporter;
    }

    protected AbstractSpan createSpan() {
        builder.timestamp(start.toEpochMilli());
        for (Reference reference : references) {
            SpanContext context = reference.getReferredTo();
            if (context instanceof ZipkinSpanContext) {
                builder.parentId(((ZipkinSpanContext)context).getParentId());
                builder.traceId(((ZipkinSpanContext)context).getTraceId());
            }
        }
        return new ZipkinSpan(builder.build()) {
            public void finish() {
                super.finish();
                reporter.report(toZipkinSpan());
            }

            public void finish(long finishMicros) {
                super.finish(finishMicros);
                reporter.report(toZipkinSpan());
            }
        };
    }

    AbstractSpanBuilder withStateItem(String key, Object value) {
        switch (key) {
            case ID:
                builder.id(convertLong(value));
                break;
            case PARENT_ID:
                builder.parentId(convertLong(value));
                break;
            case TRACE_ID:
                builder.traceId(convertLong(value));
                break;
        }
        return this;
    }

    public boolean isTraceState(String key, Object value) {
        return key.equals(ID) || key.equals(PARENT_ID) || key.equals(TRACE_ID);
    }

}
