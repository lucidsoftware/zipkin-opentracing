package io.opentracing.contrib.zipkin.time;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public final class TimeUtil {

    private TimeUtil() {
    }

    public static long epochMicros(Instant instant) {
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    public static Instant epochMicrosToInstant(long micros) {
        long seconds = TimeUnit.MICROSECONDS.toSeconds(micros);
        return Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(micros - TimeUnit.SECONDS.toMicros(seconds)));
    }

}
