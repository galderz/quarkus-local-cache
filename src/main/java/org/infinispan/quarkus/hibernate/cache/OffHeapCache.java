package org.infinispan.quarkus.hibernate.cache;

import org.infinispan.quarkus.hibernate.cache.offheap.OffHeapContainer;

final class OffHeapCache implements InternalCache {

    final OffHeapContainer container;

    OffHeapCache(int desiredSize) {
        container = new OffHeapContainer(desiredSize);
    }

    @Override
    public Object getOrNull(Object key) {
        return container.get(key);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        // TODO: Customise this generated block
    }

    @Override
    public void put(Object key, Object value) {
        container.put(key, value);
    }

    @Override
    public void invalidate(Object key) {
        // TODO: Customise this generated block
    }

    @Override
    public long size() {
        return 0;  // TODO: Customise this generated block
    }

    @Override
    public void stop() {
        // TODO: Customise this generated block
    }

    @Override
    public void invalidateAll() {
        // TODO: Customise this generated block
    }

}
