package org.infinispan.quarkus.hibernate.cache;

import org.infinispan.quarkus.hibernate.cache.InternalCache;
import org.infinispan.quarkus.hibernate.cache.offheap.OffHeapContainer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
    public Object compute(Object key, BiFunction<Object, Object, Object> remappingFunction) {
        return container.compute(key, remappingFunction);
    }

    @Override
    public void invalidate(Object key) {
        // TODO: Customise this generated block
    }

    @Override
    public long size(Predicate<Map.Entry> filter) {
        return 0;  // TODO: Customise this generated block
    }

    @Override
    public void forEach(Predicate<Map.Entry> filter, Consumer<Map.Entry> action) {
        // TODO: Customise this generated block
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
