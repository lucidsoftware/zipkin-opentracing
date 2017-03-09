package io.opentracing.contrib.zipkin.time;

import java.time.Instant;

/**
 * A high-precision timer with an optional start time.
 * When a start time is not specified, it takes advantage of the higher-precision System.nanoTime().
 */
public final class Timer {

    private final Instant start;
    private final Long nanoStart;

    public Timer() {
        start = Instant.now();
        nanoStart = System.nanoTime();
    }

    public Timer(Instant start) {
        nanoStart = null;
        this.start = start;
    }

    public Instant getStart() {
        return start;
    }

    public Instant getEnd() {
        return nanoStart == null ? Instant.now() : start.plusNanos(System.nanoTime() - nanoStart);
    }

}
