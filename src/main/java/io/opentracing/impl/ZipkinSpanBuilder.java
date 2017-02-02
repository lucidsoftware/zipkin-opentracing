package io.opentracing.impl;

import io.opentracing.SpanContext;
import io.opentracing.contrib.zipkin.TimeUtil;
import java.util.Random;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinSpanBuilder extends AbstractSpanBuilder implements ZipkinSpanContext {

    public static final String ID = "span_id";
    public static final String TRACE_ID = "trace_id";
    public static final String PARENT_ID = "parent_id";

    private static Long convertLong(Object value) {
        return value instanceof Number ? ((Number)value).longValue() : Long.parseLong(value.toString());
    }

    private Long id;
    private Long parentId;
    private Long traceId;
    private final Span.Builder builder;
    private final Reporter<Span> reporter;

    public ZipkinSpanBuilder(String operationName, Random random, Reporter<Span> reporter) {
        super(operationName);
        this.builder = Span.builder().id(random.nextLong()).traceId(random.nextLong()).name(operationName);
        this.reporter = reporter;
    }

    protected AbstractSpan createSpan() {
        if (id != null) {
            builder.id(id);
        }
        builder.timestamp(TimeUtil.epochMicros(start));
        builder.parentId(parentId);
        for (Reference reference : references) {
            SpanContext context = reference.getReferredTo();
            if (context instanceof ZipkinSpanContext) {
                builder.parentId(((ZipkinSpanContext)context).getId());
                Long traceId = ((ZipkinSpanContext)context).getTraceId();
                if (traceId != null) {
                    builder.traceId(traceId);
                }
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
                id = convertLong(value);
                break;
            case PARENT_ID:
                parentId = convertLong(value);
                break;
            case TRACE_ID:
                traceId = convertLong(value);
                break;
        }
        return this;
    }

    public boolean isTraceState(String key, Object value) {
        return key.equals(ID) || key.equals(PARENT_ID) || key.equals(TRACE_ID);
    }

    public Long getId() {
        return id;
    }

    public Long getParentId() {
        return parentId;
    }

    public Long getTraceId() {
        return traceId;
    }
}
