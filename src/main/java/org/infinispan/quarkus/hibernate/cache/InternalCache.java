package org.infinispan.quarkus.hibernate.cache;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

interface InternalCache {

    Object getOrNull(Object key);

    void putIfAbsent(Object key, Object value);

    void put(Object key, Object value);

    // TODO consider returning void since return is not really used
    Object compute(Object key, BiFunction<Object, Object, Object> remappingFunction);

    void invalidate(Object key);

    long size(Predicate<Map.Entry> filter);

    void forEach(Predicate<Map.Entry> filter, Consumer<Map.Entry> action);

    void stop();

    void invalidateAll();

}
