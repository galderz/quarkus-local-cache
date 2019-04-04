package org.infinispan.quarkus.hibernate.cache;

interface InternalCache {

    Object getOrNull(Object key);

    void putIfAbsent(Object key, Object value);

    void put(Object key, Object value);

    void invalidate(Object key);

    long size();

    void stop();

    void invalidateAll();

}
