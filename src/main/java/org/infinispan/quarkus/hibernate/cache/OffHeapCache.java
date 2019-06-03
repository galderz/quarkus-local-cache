package org.infinispan.quarkus.hibernate.cache;

import org.infinispan.quarkus.hibernate.cache.offheap.OffHeapContainer;

final class OffHeapCache implements InternalCache {

    final OffHeapContainer container;

    OffHeapCache(int desiredSize) {
        container = new OffHeapContainer(desiredSize, null, Time.forever());
    }

    @Override
    public Object getOrNull(Object key) {
        return container.get(key);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        container.put(key, value);
    }

    @Override
    public void put(Object key, Object value) {
        container.put(key, value);
    }

    @Override
    public void invalidate(Object key) {
        container.invalidate(key);
    }

    @Override
    public long count() {
        return container.count();
    }

    @Override
    public void stop() {
        container.stop();
    }

    @Override
    public void invalidateAll() {
        container.invalidateAll();
    }

}
