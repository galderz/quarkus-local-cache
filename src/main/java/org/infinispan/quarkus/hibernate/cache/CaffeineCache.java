package org.infinispan.quarkus.hibernate.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import org.jboss.logging.Logger;

import java.time.Duration;

final class CaffeineCache implements InternalCache {

    private static final Logger log = Logger.getLogger(CaffeineCache.class);
    private static final boolean trace = log.isTraceEnabled();

    private static final Ticker TICKER = Ticker.systemTicker();
    static final Time.NanosService TIME_SERVICE = TICKER::read;

    private final Cache<Object, Object> cache;
    private final String cacheName;

    CaffeineCache(String cacheName, InternalCacheConfig config, Time.NanosService nanosTimeService) {
        Duration maxIdle = config.maxIdle;
        long objectCount = config.maxObjectCount;

        this.cacheName = cacheName;
        final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                .ticker(nanosTimeService::nanoTime);

        if (!Time.isForever(maxIdle)) {
            cacheBuilder.expireAfter(new CacheExpiryPolicy(maxIdle));
        }

        if (objectCount >= 0) {
            cacheBuilder.maximumSize(objectCount);
        }

        this.cache = cacheBuilder.build();
    }

    @Override
    public Object getOrNull(Object key) {
        final Object value = cache.getIfPresent(key);
        if (trace) {
            log.tracef("Cache get(key=%s) returns: %s", key, value);
        }

        return value;
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        if (trace) {
            log.tracef("Cache put if absent key=%s value=%s", key, value);
        }

        cache.asMap().putIfAbsent(key, value);
    }

    @Override
    public void put(Object key, Object value) {
        if (trace) {
            log.tracef("Cache put key=%s value=%s", key, value);
        }

        cache.put(key, value);
    }

    @Override
    public void invalidate(Object key) {
        if (trace) {
            log.tracef("Cache invalidate key %s", key);
        }

        cache.invalidate(key);
    }

    @Override
    public long count() {
        // Size calculated for stats, so try to get as accurate count as possible
        // by performing any cleanup operations before returning the result
        cache.cleanUp();

        final long size = cache.estimatedSize();
        if (trace) {
            log.tracef("Cache(%s) size estimated at %d elements", cacheName, size);
        }

        return size;
    }

    @Override
    public void stop() {
        if (trace) {
            log.tracef("Cleanup cache %s", cacheName);
        }

        cache.cleanUp();
    }

    @Override
    public void invalidateAll() {
        if (trace) {
            log.tracef("Invalidate all in cache %s", cacheName);
        }

        cache.invalidateAll();
    }

    private static final class CacheExpiryPolicy implements Expiry<Object, Object> {

        private final long maxIdleNanos;

        CacheExpiryPolicy(Duration maxIdle) {
            if (maxIdle == null)
                this.maxIdleNanos = -1;
            else
                this.maxIdleNanos = maxIdle.toNanos();
        }

        @Override
        public long expireAfterCreate(Object key, Object value, long currentTime) {
            return calculateExpiration();
        }

        private long calculateExpiration() {
            if (maxIdleNanos > 0)
                return maxIdleNanos;

            return Long.MAX_VALUE;
        }

        @Override
        public long expireAfterUpdate(Object key, Object value, long currentTime, long currentDuration) {
            return calculateExpiration();
        }

        @Override
        public long expireAfterRead(Object key, Object value, long currentTime, long currentDuration) {
            if (maxIdleNanos > 0) {
                return maxIdleNanos;
            }

            return Long.MAX_VALUE;
        }

    }

}
