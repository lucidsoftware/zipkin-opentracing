package io.opentracing.contrib.zipkin;

import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.zipkin.propagation.HttpHeadersPropagation;
import io.opentracing.contrib.zipkin.propagation.TextMapPropagation;
import io.opentracing.propagation.Format;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinTracer implements Tracer {

    private final Reporter<Span> reporter;
    private final Random random;
    private final Map<Format, BiConsumer> injectors;
    private final Map<Format, Function> extractors;

    private ZipkinTracer(Builder builder) {
        reporter = builder.reporter;
        random = builder.random;
        injectors = new HashMap<>(builder.injectors);
        extractors = new HashMap<>(builder.extractors);
    }

    public SpanBuilder buildSpan(String name) {
        return new ZipkinSpanBuilder(name, random, reporter);
    }

    @SuppressWarnings("unchecked")
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        injectors.get(format).accept(spanContext, carrier);
    }

    @SuppressWarnings("unchecked")
    public <C> SpanContext extract(Format<C> format, C carrier) {
        return (SpanContext)extractors.get(format).apply(carrier);
    }

    public static Builder builder(Reporter<Span> reporter) {
        return new Builder(reporter);
    }

    public static class Builder {

        private static final Random defaultRandom = new Random();

        final Reporter<Span> reporter;
        Random random;
        Map<Format, BiConsumer<SpanContext, ?>> injectors;
        Map<Format, Function<?, SpanContext>> extractors;

        public Builder(Reporter<Span> reporter) {
            this.reporter = reporter;
            random = defaultRandom;
            injectors = new HashMap<>();
            extractors = new HashMap<>();
            withInjector(Format.Builtin.TEXT_MAP, TextMapPropagation.injector);
            withExtractor(Format.Builtin.TEXT_MAP, TextMapPropagation.extractor);
            withInjector(Format.Builtin.HTTP_HEADERS, HttpHeadersPropagation.injector);
            withExtractor(Format.Builtin.HTTP_HEADERS, HttpHeadersPropagation.extractor);
            // TODO: Format.Builtin.BINARY
        }

        public Builder withRandom(Random random) {
            this.random = random;
            return this;
        }

        public <C> Builder withInjector(Format<C> format, BiConsumer<SpanContext, C> injector) {
            injectors.put(format, injector);
            return this;
        }

        public <C> Builder withExtractor(Format<C> format, Function<C, SpanContext> extractor) {
            extractors.put(format, extractor);
            return this;
        }

        public ZipkinTracer build() {
            return new ZipkinTracer(this);
        }

    }

}
