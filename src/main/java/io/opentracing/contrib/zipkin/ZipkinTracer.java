package io.opentracing.contrib.zipkin;

import io.opentracing.ActiveSpan;
import io.opentracing.ActiveSpanSource;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.zipkin.propagation.HttpHeadersPropagation;
import io.opentracing.contrib.zipkin.propagation.TextMapPropagation;
import io.opentracing.propagation.Format;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinTracer implements Tracer {

    private static final Random defaultRandom = new Random();

    private final Endpoint endpoint;
    private final Reporter<Span> reporter;
    private final Random random;
    private final Map<Format, BiConsumer> injectors;
    private final Map<Format, Function> extractors;
    private final ActiveSpanSource activeSpanSource;

    private ZipkinTracer(Builder builder) {
        if (builder.endpoint != null) {
            endpoint = builder.endpoint;
        } else {
            final Endpoint.Builder endpointBuilder = Endpoint.builder();
            String serviceName;
            try {
                serviceName = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                serviceName = "";
            }
            endpointBuilder.serviceName(serviceName);
            try {
                final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                outer:
                while (networkInterfaces.hasMoreElements()) {
                    final NetworkInterface networkInterface = networkInterfaces.nextElement();
                    if (networkInterface.isUp() && !networkInterface.isVirtual() && !networkInterface.isLoopback()) {
                        final Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                        while (addresses.hasMoreElements()) {
                            final InetAddress address = addresses.nextElement();
                            if (address instanceof Inet4Address) {
                                endpointBuilder.ipv4(ByteBuffer.wrap(address.getAddress()).getInt());
                                break outer;
                            } else if (address instanceof Inet6Address) {
                                endpointBuilder.ipv6(address.getAddress());
                                break outer;
                            }
                        }
                    }
                }
            } catch (SocketException e) {
            }
            endpoint = endpointBuilder.build();
        }
        reporter = builder.reporter;
        if (builder.random != null) {
            random = builder.random;
        } else {
            random = defaultRandom;
        }
        injectors = new HashMap<>(builder.injectors);
        extractors = new HashMap<>(builder.extractors);
        activeSpanSource = builder.activeSpanSource;
    }

    public SpanBuilder buildSpan(String name) {
        return new ZipkinSpanBuilder(name, endpoint, random, reporter, activeSpanSource);
    }

    @SuppressWarnings("unchecked")
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        injectors.get(format).accept(spanContext, carrier);
    }

    @SuppressWarnings("unchecked")
    public <C> SpanContext extract(Format<C> format, C carrier) {
        return (SpanContext)extractors.get(format).apply(carrier);
    }

    public ActiveSpan activeSpan() {
        return activeSpanSource == null ? null : activeSpanSource.activeSpan();
    }

    public ActiveSpan makeActive(io.opentracing.Span span) {
        return activeSpanSource == null ? null : activeSpanSource.makeActive(span);
    }

    public static Builder builder(Reporter<Span> reporter) {
        return new Builder(reporter);
    }

    public static class Builder {
        final Reporter<Span> reporter;
        Endpoint endpoint;
        Random random;
        Map<Format, BiConsumer<SpanContext, ?>> injectors;
        Map<Format, Function<?, SpanContext>> extractors;
        ActiveSpanSource activeSpanSource;

        public Builder(Reporter<Span> reporter) {
            this.reporter = reporter;
            injectors = new HashMap<>();
            extractors = new HashMap<>();
            withInjector(Format.Builtin.TEXT_MAP, TextMapPropagation.injector);
            withExtractor(Format.Builtin.TEXT_MAP, TextMapPropagation.extractor);
            withInjector(Format.Builtin.HTTP_HEADERS, HttpHeadersPropagation.injector);
            withExtractor(Format.Builtin.HTTP_HEADERS, HttpHeadersPropagation.extractor);
            // TODO: Format.Builtin.BINARY
        }

        public Builder withEndpoint(final Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
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

        public Builder withActiveSpanSource(ActiveSpanSource activeSpanSource) {
            this.activeSpanSource = activeSpanSource;
            return this;
        }

        public ZipkinTracer build() {
            return new ZipkinTracer(this);
        }

    }

}
