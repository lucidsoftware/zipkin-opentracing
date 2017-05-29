package io.opentracing.contrib.zipkin;

import io.opentracing.ActiveSpanSource;
import io.opentracing.BaseSpan;
import io.opentracing.References;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.zipkin.time.TimeUtil;
import io.opentracing.contrib.zipkin.time.Timer;
import io.opentracing.tag.Tags;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinSpanBuilder implements Tracer.SpanBuilder {

    private String name;
    private final Endpoint endpoint;
    private ZipkinSpanContext parent;
    private final Reporter<Span> reporter;
    private final Map<String, String> baggage;
    private final Map<String, Consumer<io.opentracing.Span>> tags;
    private final Random random;
    private final ActiveSpanSource activeSpanSource;
    private Instant start;
    private String kind;
    private boolean ignoreActiveSpan;

    public ZipkinSpanBuilder(String name, Endpoint endpoint, Random random, Reporter<Span> reporter, ActiveSpanSource activeSpanSource) {
        this.name = name;
        this.endpoint = endpoint;
        this.reporter = reporter;
        this.random = random;
        this.activeSpanSource = activeSpanSource;
        this.baggage = new HashMap<>();
        tags = new HashMap<>();
    }

    public Iterable<Map.Entry<String, String>> baggageItems() {
        return baggage.entrySet();
    }

    public Tracer.SpanBuilder asChildOf(SpanContext parent) {
        if (parent instanceof ZipkinSpanContext) {
            this.parent = (ZipkinSpanContext) parent;
            this.ignoreActiveSpan = true;
        }
        return this;
    }

    public Tracer.SpanBuilder asChildOf(BaseSpan<?> parent) {
        return asChildOf(parent.context());
    }

    public Tracer.SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
        if (referenceType.equals(References.CHILD_OF)) {
            asChildOf(referencedContext);
        }
        return this;
    }

    public Tracer.SpanBuilder ignoreActiveSpan() {
        ignoreActiveSpan = true;
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, String value) {
        if (key.equals(Tags.SPAN_KIND.getKey())) {
            kind = value;
        }
        tags.put(key, span -> span.setTag(key, value));
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, boolean value) {
        tags.put(key, span -> span.setTag(key, value));
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, Number value) {
        tags.put(key, span -> span.setTag(key, value));
        return this;
    }

    public Tracer.SpanBuilder withStartTimestamp(long microseconds) {
        start = TimeUtil.epochMicrosToInstant(microseconds);
        return this;
    }

    public io.opentracing.ActiveSpan startActive() {
        if (activeSpanSource == null) {
            throw new IllegalStateException("Cannot start active span because there was no ActiveSpanSource configured.");
        }
        return activeSpanSource.makeActive(startManual());
    }

    public io.opentracing.Span start() {
        io.opentracing.Span span = startManual();
        if (activeSpanSource != null) {
            activeSpanSource.makeActive(span);
        }
        return span;
    }

    public io.opentracing.Span startManual() {
        if (activeSpanSource != null && !ignoreActiveSpan) {
            io.opentracing.ActiveSpan activeSpan = activeSpanSource.activeSpan();
            if (activeSpan != null) {
                this.asChildOf(activeSpan);
            }
        }
        Span.Builder builder = Span.builder()
                .name(name)
                .traceId(parent == null ? random.nextLong() : parent.getTraceId());
        if (parent != null && Tags.SPAN_KIND_SERVER.equals(kind)) {
            // if server side, don't create new Zipkin span; re-use existing, for two-sided span
            builder.id(parent.getId());
            builder.parentId(parent.getParentId());
        } else {
            builder.id(random.nextLong());
            if (parent != null) {
                builder.parentId(parent.getId());
            }
        }
        io.opentracing.Span span = new ZipkinSpan(builder, endpoint, reporter, start == null ? new Timer() : new Timer(start));
        for (Map.Entry<String, Consumer<io.opentracing.Span>> tag : tags.entrySet()) {
            tag.getValue().accept(span);
        }
        return span;
    }

}
