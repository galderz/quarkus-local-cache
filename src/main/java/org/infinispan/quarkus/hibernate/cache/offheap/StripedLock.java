package org.infinispan.quarkus.hibernate.cache.offheap;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

final class StripedLock {

    private final ReadWriteLock[] locks;

    StripedLock() {
        locks = new ReadWriteLock[Environment.BLOCK_COUNT];
        for (int i = 0; i< locks.length; ++i)
            locks[i] = new ReentrantReadWriteLock();
    }

    ReadWriteLock getLock(byte[] obj) {
        return getLockFromHashCode(Arrays.hashCode(obj));
    }

    void lockAll() {
        for (ReadWriteLock rwLock : locks) {
            rwLock.writeLock().lock();
        }
    }

    void unlockAll() {
        for (ReadWriteLock rwLock : locks) {
            rwLock.writeLock().unlock();
        }
    }

    ReadWriteLock getLockFromHashCode(int hashCode) {
        int offset = Math.calculateOffset(hashCode);
        return locks[offset];
    }

}
