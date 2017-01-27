package io.opentracing.impl;

import io.opentracing.SpanContext;

public interface ZipkinSpanContext extends SpanContext {

    Long getId();

    Long getParentId();

    Long getTraceId();

}
