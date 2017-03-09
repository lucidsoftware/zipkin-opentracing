package io.opentracing.contrib.zipkin;

import io.opentracing.SpanContext;
import io.opentracing.contrib.zipkin.time.TimeUtil;
import io.opentracing.contrib.zipkin.time.Timer;
import io.opentracing.tag.Tags;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.reporter.Reporter;

public class ZipkinSpan implements io.opentracing.Span {

    private final Span.Builder builder;
    private final Reporter<Span> reporter;
    private final Timer timer;
    private final Endpoint.Builder endpointBuilder;
    private final Collection<Annotation.Builder> annotations;
    private final Map<String,String> baggage = new HashMap<>();
    private Boolean isClient;
    private boolean error;
    private boolean isFinished;

    public ZipkinSpan(Span.Builder span, Reporter<Span> reporter, Timer timer) {
        this.builder = span;
        this.endpointBuilder = Endpoint.builder();
        this.reporter = reporter;
        this.timer = timer;
        this.annotations = new ArrayList<>();
    }

    public SpanContext context() {
        Span span = builder.build();
        return new ZipkinSpanContext(span.id, span.parentId, span.traceId, baggage.entrySet());
    }

    public void finish() {
        finish(TimeUtil.epochMicros(timer.getEnd()));
    }

    public void finish(long finishMicros) {
        if (!isFinished) {
            isFinished = true;
            Span.Builder builder = this.builder.build().toBuilder();

            long startMicros = TimeUtil.epochMicros(timer.getStart());
            builder.timestamp(startMicros).duration(finishMicros - startMicros);

            Endpoint endpoint = endpointBuilder.build();
            if (isClient != null) {
                builder.addAnnotation(
                    Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(startMicros)
                        .value(isClient ? Constants.CLIENT_SEND : Constants.SERVER_SEND)
                        .build()
                );
                builder.addAnnotation(
                    Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(finishMicros)
                        .value(isClient ? Constants.CLIENT_RECV : Constants.SERVER_RECV)
                        .build()
                );
            }

            if (error) {
                builder.addAnnotation(Annotation.builder().endpoint(endpoint).value(Constants.ERROR).build());
            }

            for (Annotation.Builder annotationBuilder : annotations) {
                builder.addAnnotation(annotationBuilder.endpoint(endpoint).build());
            }

            reporter.report(builder.build());
        }
    }

    public void close() {
        finish();
    }

    public io.opentracing.Span setTag(String key, String value) {
        if (key.equals(Tags.SPAN_KIND.getKey())) {
            isClient = value.equals(Tags.SPAN_KIND_CLIENT) ? Boolean.TRUE : value.equals(Tags.SPAN_KIND_SERVER) ? Boolean.FALSE : null;
        } else if (key.equals(Tags.PEER_SERVICE.getKey())) {
            endpointBuilder.serviceName(value);
        } else if (key.equals(Tags.PEER_HOST_IPV6.getKey())) {
            try {
                endpointBuilder.ipv6(Inet6Address.getByName(value).getAddress());
            } catch (UnknownHostException e) {
            }
        } else {
            builder.addBinaryAnnotation(BinaryAnnotation.builder().key(key).type(BinaryAnnotation.Type.STRING).value(value).build());
        }
        return this;
    }

    public io.opentracing.Span setTag(String key, boolean value) {
        if (key.equals(Tags.ERROR.getKey())) {
            error = value;
        } else {
            builder.addBinaryAnnotation(BinaryAnnotation.builder().key(key).type(BinaryAnnotation.Type.BOOL).value(new byte[]{value ? (byte) 1 : (byte) 0}).build());
        }
        return this;
    }

    public io.opentracing.Span setTag(String key, Number value) {
        if (key.equals(Tags.PEER_HOST_IPV4.getKey())) {
            endpointBuilder.ipv4(value.intValue());
        } else if (key.equals(Tags.PEER_PORT.getKey())) {
            endpointBuilder.port(value.shortValue());
        } else {
            BinaryAnnotation.Builder annotationBuilder = BinaryAnnotation.builder().key(key);
            final ByteBuffer bytes;
            if (value instanceof Byte || value instanceof Short) {
                bytes = ByteBuffer.allocate(Short.BYTES).putShort(value.shortValue());
                annotationBuilder.type(BinaryAnnotation.Type.I16);
            } else if (value instanceof Integer || value instanceof AtomicInteger) {
                bytes = ByteBuffer.allocate(Integer.BYTES).putInt(value.intValue());
                annotationBuilder.type(BinaryAnnotation.Type.I32);
            } else if (value instanceof Long || value instanceof AtomicLong || value instanceof BigInteger) {
                bytes = ByteBuffer.allocate(Long.BYTES).putLong(value.longValue());
                annotationBuilder.type(BinaryAnnotation.Type.I64);
            } else {
                bytes = ByteBuffer.allocate(Double.BYTES).putDouble(value.doubleValue());
                annotationBuilder.type(BinaryAnnotation.Type.DOUBLE);
            }
            builder.addBinaryAnnotation(annotationBuilder.value(bytes.array()).build());
        }
        return this;
    }

    public io.opentracing.Span log(Map<String, ?> fields) {
        return log(TimeUtil.epochMicros(timer.getEnd()), fields);
    }

    public io.opentracing.Span log(long timestampMicroseconds, Map<String, ?> fields) {
        for (Map.Entry<String, ?> field : fields.entrySet()) {
            log(timestampMicroseconds, String.format("%s:%s", field.getKey(), field.getValue()));
        }
        return this;
    }

    public io.opentracing.Span log(String event) {
        return log(TimeUtil.epochMicros(timer.getEnd()), event);
    }

    public io.opentracing.Span log(long timestampMicroseconds, String event) {
        annotations.add(Annotation.builder().timestamp(timestampMicroseconds).value(event));
        return this;
    }

    public io.opentracing.Span log(String eventName, Object payload) {
        return this;
    }

    public io.opentracing.Span log(long timestampMicroseconds, String eventName, Object payload) {
        return log(timestampMicroseconds, eventName);
    }

    public io.opentracing.Span setBaggageItem(String key, String value) {
        baggage.put(key, value);
        return this;
    }

    public String getBaggageItem(String key) {
        return baggage.get(key);
    }

    public io.opentracing.Span setOperationName(String operationName) {
        builder.name(operationName);
        return this;
    }

}
