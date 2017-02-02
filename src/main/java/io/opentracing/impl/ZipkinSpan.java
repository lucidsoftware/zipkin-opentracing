package io.opentracing.impl;

import io.opentracing.contrib.zipkin.TimeUtil;
import io.opentracing.tag.Tags;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;

public class ZipkinSpan extends AbstractSpan implements ZipkinSpanContext {

    private static Field logDataTime;
    private static Field logDataFields;
    {
        try {
            logDataTime = LogData.class.getDeclaredField("time");
            logDataTime.setAccessible(true);
            logDataFields = LogData.class.getDeclaredField("fields");
            logDataFields.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> builtInTags = new HashSet<>(Arrays.asList(
        Tags.SPAN_KIND.getKey(),
        Tags.ERROR.getKey(),
        Tags.PEER_SERVICE.getKey(),
        Tags.PEER_HOST_IPV4.getKey(),
        Tags.PEER_HOST_IPV6.getKey(),
        Tags.PEER_HOSTNAME.getKey(),
        Tags.PEER_PORT.getKey()
    ));

    private final Span span;

    public ZipkinSpan(Span span) {
        super(span.name, TimeUtil.epochMicrosToInstant(span.timestamp));
        this.span = span;
    }

    public Long getId() {
        return span.id;
    }

    public Long getParentId() {
        return span.parentId;
    }

    public Long getTraceId() {
        return span.traceId;
    }

    @SuppressWarnings("unchecked")
    public Span toZipkinSpan() {
        Span.Builder builder = span.toBuilder().timestamp(TimeUtil.epochMicros(getStart())).duration(TimeUtil.micros(getDuration()));
        Map<String, Object> tags = getTags();
        Endpoint endpoint = null;
        if (tags.containsKey(Tags.PEER_SERVICE.getKey())) {
            Endpoint.Builder endpointBuilder = Endpoint.builder();
            endpointBuilder
                .serviceName((String) tags.get(Tags.PEER_SERVICE.getKey()))
                .ipv4((Integer) tags.getOrDefault(Tags.PEER_HOST_IPV4.getKey(), 0))
                .port((Short)tags.get(Tags.PEER_PORT.getKey()));
            String ipv6Address = (String)tags.get(Tags.PEER_HOST_IPV6.getKey());
            if (ipv6Address != null) {
                try {
                endpointBuilder.ipv6(InetAddress.getByName(ipv6Address).getAddress());
                } catch (UnknownHostException e) {
                }
            }
            endpoint = endpointBuilder.build();
        }

        String spanKind = (String)tags.get(Tags.SPAN_KIND.getKey());
        if (Tags.SPAN_KIND_CLIENT.equals(spanKind)) {
            builder.addAnnotation(
                Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(TimeUtil.epochMicros(getStart()))
                    .value(Constants.CLIENT_SEND)
                    .build()
            );
            builder.addAnnotation(
                Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(TimeUtil.epochMicros(getStart().plus(getDuration())))
                    .value(Constants.CLIENT_RECV)
                    .build()
            );
        } else if (Tags.SPAN_KIND_SERVER.equals(spanKind)) {
            builder.addAnnotation(
                Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(TimeUtil.epochMicros(getStart()))
                    .value(Constants.SERVER_RECV)
                    .build()
            );
            builder.addAnnotation(
                Annotation.builder()
                    .endpoint(endpoint)
                    .timestamp(TimeUtil.epochMicros(getStart().plus(getDuration())))
                    .value(Constants.SERVER_SEND)
                    .build()
            );
        }
        if (Boolean.TRUE.equals(tags.get(Tags.ERROR.getKey()))) {
            builder.addAnnotation(Annotation.builder().endpoint(endpoint).value(Constants.ERROR).build());
        }

        getTags().forEach((key, value) -> {
            if (!builtInTags.contains(key)) {
                BinaryAnnotation.Builder annotationBuilder = BinaryAnnotation.builder().key(key);
                if (value instanceof Boolean) {
                    byte[] bytes = new byte[]{(Boolean) value ? (byte) 1 : (byte) 0};
                    annotationBuilder.type(BinaryAnnotation.Type.BOOL).value(bytes);
                } else if (value instanceof Byte || value instanceof Short) {
                    byte[] bytes = ByteBuffer.allocate(Short.BYTES).putShort(((Number) value).shortValue()).array();
                    annotationBuilder.type(BinaryAnnotation.Type.I16).value(bytes);
                } else if (value instanceof Integer || value instanceof AtomicInteger) {
                    byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(((Number) value).intValue()).array();
                    annotationBuilder.type(BinaryAnnotation.Type.I32).value(bytes);
                } else if (value instanceof Long || value instanceof AtomicLong || value instanceof BigInteger) {
                    byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Number) value).longValue()).array();
                    annotationBuilder.type(BinaryAnnotation.Type.I64).value(bytes);
                } else if (value instanceof Number) {
                    byte[] bytes = ByteBuffer.allocate(Double.BYTES).putDouble(((Number) value).doubleValue()).array();
                    annotationBuilder.type(BinaryAnnotation.Type.DOUBLE).value(bytes);
                } else {
                    annotationBuilder.type(BinaryAnnotation.Type.STRING).value(value.toString());
                }
                builder.addBinaryAnnotation(annotationBuilder.build());
            }
        });
        getLogs().forEach(log -> {
            Instant time;
            Map<String, ?> fields;
            try {
                time = (Instant)logDataTime.get(log);
                fields = (Map<String, ?>)logDataFields.get(log);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            fields.forEach((key, value) -> {
                Annotation.Builder annotationBuilder = Annotation.builder().timestamp(TimeUtil.epochMicros(getStart())).value(key + ":" + value);
                builder.addAnnotation(annotationBuilder.build());
            });
        });
        return builder.build();
    }

}
