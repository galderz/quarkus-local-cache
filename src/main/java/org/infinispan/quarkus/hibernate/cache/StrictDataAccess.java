package org.infinispan.quarkus.hibernate.cache;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.jboss.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;

// TODO temporarily public, for correctness testing checks...
public final class StrictDataAccess implements InternalDataAccess {

    private static final Logger log = Logger.getLogger(StrictDataAccess.class);
    private static final boolean trace = log.isTraceEnabled();

    private final InternalCache cache;
    private final InternalRegion internalRegion;

    StrictDataAccess(InternalCache cache, InternalRegion internalRegion) {
        this.cache = cache;
        this.internalRegion = internalRegion;
    }

    @Override
    public Object get(Object session, Object key, long txTimestamp) {
        if (!internalRegion.checkValid()) {
            if (trace) {
                log.tracef("Region %s not valid", internalRegion.getName());
            }
            return null;
        }
        return cache.getOrNull(key);
    }

    @Override
    public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version) {
        return putFromLoad(session, key, value, txTimestamp, version, false);
    }

    @Override
    public boolean putFromLoad(Object session, Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) {
        if (!internalRegion.checkValid()) {
            if (trace) {
                log.tracef("Region %s not valid", internalRegion.getName());
            }
            return false;
        }

        if (minimalPutOverride && cache.getOrNull(key) != null) {
            return false;
        }

        cache.putIfAbsent(key, value);

        return true;
    }

    @Override
    public boolean insert(Object session, Object key, Object value, Object version) {
        if (!internalRegion.checkValid()) {
            return false;
        }
        write(session, key, value);
        return true;
    }

    @Override
    public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion) {
        // We update whether or not the region is valid.
        write(session, key, value);
        return true;
    }

    @Override
    public void remove(Object session, Object key) {
        // We update whether or not the region is valid. Other nodes
        // may have already restored the region so they need to
        // be informed of the change.
        write(session, key, null);
    }

    @Override
    public void removeAll() {
        cache.invalidateAll();
    }

    @Override
    public void evict(Object key) {
        cache.invalidate(key);
    }

    @Override
    public void evictAll() {
        // Invalidate the local region
        internalRegion.clear();
    }

    @Override
    public boolean afterInsert(Object session, Object key, Object value, Object version) {
        return false;
    }

    @Override
    public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) {
        return false;
    }

    private void write(Object session, Object key, Object value) {
        cache.invalidate(key);
    }

}
