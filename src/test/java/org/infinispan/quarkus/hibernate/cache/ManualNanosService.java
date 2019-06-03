package org.infinispan.quarkus.hibernate.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ManualNanosService implements Time.NanosService {

    private final AtomicLong nanos = new AtomicLong();

    public void advance(long time, TimeUnit timeUnit) {
        final long nanoseconds = timeUnit.toNanos(time);
        nanos.addAndGet(nanoseconds);
    }

    @Override
    public long nanoTime() {
        return nanos.get();
    }

}
