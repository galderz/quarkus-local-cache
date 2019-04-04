package org.infinispan.quarkus.hibernate.cache;

interface InternalCache {

    Object getOrNull(Object key);

    void putIfAbsent(Object key, Object value);

    void put(Object key, Object value);

    void invalidate(Object key);

    // TODO rename to mean count elements because the meaning can be misleading for offheap impls
    long size();

    void stop();

    void invalidateAll();

}
