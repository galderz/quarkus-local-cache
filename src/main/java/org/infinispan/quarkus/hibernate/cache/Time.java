package org.infinispan.quarkus.hibernate.cache;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public final class Time {

    private static final Duration FOREVER = ChronoUnit.FOREVER.getDuration();

    private Time() {
    }

    public static Duration forever() {
        return FOREVER;
    }

    public static boolean isForever(Duration duration) {
        return FOREVER == duration;
    }

    @FunctionalInterface
    public interface NanosService {
        NanosService SYSTEM = System::nanoTime;

        long nanoTime();
    }

    @FunctionalInterface
    public interface MillisService {
        MillisService SYSTEM = System::currentTimeMillis;

        long milliTime();
    }

}
