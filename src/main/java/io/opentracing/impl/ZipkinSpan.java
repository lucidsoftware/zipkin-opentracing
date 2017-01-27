package io.opentracing.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Span;

public class ZipkinSpan extends AbstractSpan implements ZipkinSpanContext {

    private static Method logDataTime;
    private static Method logDataFields;

    {
        try {
            logDataTime = LogData.class.getDeclaredMethod("time");
            logDataTime.setAccessible(true);
            logDataFields = LogData.class.getDeclaredMethod("fields");
            logDataFields.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private final Span span;

    public ZipkinSpan(Span span) {
        super(span.name);
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
        Span.Builder builder = span.toBuilder();
        Duration duration = getDuration();
        if (duration != null) {
            builder.duration(duration.toMillis());
        }
        getTags().forEach((key, value) -> {
            BinaryAnnotation.Builder annotationBuilder = BinaryAnnotation.builder();
            if (value instanceof Boolean) {
                byte[] bytes = new byte[] { (Boolean)value ? (byte)1 : (byte)0 };
                annotationBuilder.type(BinaryAnnotation.Type.BOOL).value(bytes);
            } else if (value instanceof Byte || value instanceof Short) {
                byte[] bytes = ByteBuffer.allocate(Short.BYTES).putShort(((Number)value).shortValue()).array();
                annotationBuilder.type(BinaryAnnotation.Type.I16).value(bytes);
            } else if (value instanceof Integer || value instanceof AtomicInteger) {
                byte[] bytes = ByteBuffer.allocate(Integer.BYTES).putInt(((Number)value).intValue()).array();
                annotationBuilder.type(BinaryAnnotation.Type.I32).value(bytes);
            } else if (value instanceof Long || value instanceof AtomicLong || value instanceof BigInteger) {
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Number)value).longValue()).array();
                annotationBuilder.type(BinaryAnnotation.Type.I64).value(bytes);
            } else if (value instanceof Number) {
                byte[] bytes = ByteBuffer.allocate(Double.BYTES).putDouble(((Number)value).doubleValue()).array();
                annotationBuilder.type(BinaryAnnotation.Type.DOUBLE).value(bytes);
            } else {
                annotationBuilder.type(BinaryAnnotation.Type.STRING).value(value.toString());
            }
            builder.addBinaryAnnotation(annotationBuilder.build());
        });
        getLogs().forEach(log -> {
            Instant time;
            Map<String, ?> fields;
            try {
                time = (Instant)logDataTime.invoke(log);
                fields = (Map<String, ?>)logDataFields.invoke(log);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            fields.forEach((key, value) -> {
                Annotation.Builder annotationBuilder = Annotation.builder().timestamp(time.toEpochMilli()).value(key + ":" + value);
                builder.addAnnotation(annotationBuilder.build());
            });
        });
        return builder.build();
    }

}
