package io.opentracing.contrib.zipkin;

import io.opentracing.SpanContext;
import io.opentracing.contrib.zipkin.time.TimeUtil;
import io.opentracing.contrib.zipkin.time.Timer;
import io.opentracing.tag.Tags;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
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
    private final Endpoint endpoint;
    private final Reporter<Span> reporter;
    private final Timer timer;
    private final Map<String,String> baggage;
    private Instant error;
    private boolean isFinished;
    private String kind;
    private String peerServiceName;
    private Integer peerIpv4;
    private byte[] peerIpv6;
    private Short peerPort;

    public ZipkinSpan(final Span.Builder span, final Endpoint endpoint, final Reporter<Span> reporter, final Timer timer) {
        this.builder = span;
        this.endpoint = endpoint;
        this.reporter = reporter;
        this.timer = timer;
        baggage = new HashMap<>();
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

            long startMicros = TimeUtil.epochMicros(timer.getStart());
            builder.timestamp(startMicros).duration(finishMicros - startMicros);

            final Endpoint peer;
            if (peerPort != null || peerServiceName != null || peerIpv4 != null || peerIpv6 != null) {
                Endpoint.Builder peerBuilder = Endpoint.builder().serviceName(peerServiceName == null ? "" : peerServiceName);
                if (peerIpv4 != null) {
                    peerBuilder.ipv4(peerIpv4);
                }
                if (peerIpv6 != null) {
                    peerBuilder.ipv6(peerIpv6);
                }
                if (peerPort != null) {
                    peerBuilder.port(peerPort);
                }
                peer = peerBuilder.build();
            } else {
                peer = null;
            }

            if (Tags.SPAN_KIND_CLIENT.equals(kind)) {
                builder
                    .addAnnotation(Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(startMicros)
                        .value(Constants.CLIENT_SEND)
                        .build()
                    )
                    .addAnnotation(Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(finishMicros)
                        .value(Constants.CLIENT_RECV)
                        .build()
                    );
                if (peer != null) {
                    builder.addAnnotation(Annotation.builder().endpoint(endpoint).value(Constants.SERVER_ADDR).build());
                }
            } else if (Tags.SPAN_KIND_SERVER.equals(kind)) {
                builder
                    .addAnnotation(Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(startMicros)
                        .value(Constants.SERVER_RECV)
                        .build()
                    )
                    .addAnnotation(Annotation.builder()
                        .endpoint(endpoint)
                        .timestamp(finishMicros)
                        .value(Constants.SERVER_SEND)
                        .build()
                    );
                if (peer != null) {
                    builder.addAnnotation(Annotation.builder().endpoint(endpoint).value(Constants.CLIENT_ADDR).build());
                }
            } else if (kind != null) {
                builder.addBinaryAnnotation(BinaryAnnotation.builder()
                    .endpoint(endpoint)
                    .key(Tags.SPAN_KIND.getKey())
                    .type(BinaryAnnotation.Type.STRING)
                    .value(kind)
                    .build()
                );
            } else {
                builder.addAnnotation(Annotation.builder().endpoint(endpoint).value(Constants.LOCAL_COMPONENT).build());
            }

            if (error != null) {
                Annotation annotation = Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(TimeUtil.epochMicros(error))
                    .value(Constants.ERROR)
                    .build();
                builder.addAnnotation(annotation);
            }

            reporter.report(builder.build());
        }
    }

    public void close() {
        finish();
    }

    public io.opentracing.Span setTag(String key, String value) {
        if (key.equals(Tags.SPAN_KIND.getKey())) {
            kind = value;
        } else if (key.equals(Tags.PEER_HOST_IPV6.getKey())) {
            try {
                peerIpv6 = Inet6Address.getByName(value).getAddress();
            } catch (UnknownHostException e) {
            }
        } else if (key.equals(Tags.PEER_SERVICE.getKey())) {
            peerServiceName = value;
        } else {
            if (key.equals(Tags.PEER_HOSTNAME.getKey()) && peerServiceName == null) {
                peerServiceName = value;
            }
            builder.addBinaryAnnotation(BinaryAnnotation.builder().endpoint(endpoint).key(key).type(BinaryAnnotation.Type.STRING).value(value).build());
        }
        return this;
    }

    public io.opentracing.Span setTag(String key, boolean value) {
        if (key.equals(Tags.ERROR.getKey())) {
            error = value ? timer.getEnd() : null;
        } else {
            final byte[] bytes = new byte[]{value ? (byte) 1 : (byte) 0};
            builder.addBinaryAnnotation(BinaryAnnotation.builder().endpoint(endpoint).key(key).type(BinaryAnnotation.Type.BOOL).value(bytes).build());
        }
        return this;
    }

    public io.opentracing.Span setTag(String key, Number value) {
        if (key.equals(Tags.PEER_HOST_IPV4.getKey())) {
            peerIpv4 = value.intValue();
        } else if (key.equals(Tags.PEER_PORT.getKey())) {
            peerPort = value.shortValue();
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
            builder.addBinaryAnnotation(annotationBuilder.endpoint(endpoint).value(bytes.array()).build());
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
        builder.addAnnotation(Annotation.builder().endpoint(endpoint).timestamp(timestampMicroseconds).value(event).build());
        return this;
    }

    @Deprecated
    public io.opentracing.Span log(String eventName, Object payload) {
        return this;
    }

    @Deprecated
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
