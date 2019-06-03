package org.infinispan.quarkus.hibernate.cache.offheap;

import org.infinispan.quarkus.hibernate.cache.Time;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class OffHeapContainer {

    private static final Logger log = Logger.getLogger(OffHeapContainer.class);
    private static final boolean trace = log.isTraceEnabled();

    // Max would be 1:1 ratio with memory addresses - must be a crazy machine to have that many processors
    private static final int MAX_ADDRESS_COUNT = 1 << 30;

    private static final int ADDRESS_COUNT = 1 << 20;

    private static final int HEADER_LENGTH = 1 + 4 + 4 + 4;

    private static final byte IMMORTAL = 1 << 2;

    private static final Memory MEMORY = Memory.INSTANCE;

    private final Function<byte[], byte[]> GET_BYTES = this::getBytes;

    // TODO remove once switched to memory size based eviction
    private final AtomicLong count = new AtomicLong();

    private final StripedLock locks;
    private final Marshalling marshalling;

    private final Lock lruLock = new ReentrantLock();
    private final long maxSize;
    private final Time.NanosService time;
    private final boolean isTransient;
    private final long maxIdleNanos;

    // TODO is this required? isn't this already tracked by bucket + data memory sizes?
    private long currentSize;

    private long firstAddress;
    private long lastAddress;

    // Variable to make sure memory locations aren't read after being deallocated
    // This variable should always be read first after acquiring either the read or write lock
    private boolean dellocated = false;

    final BucketMemory bucketMemory;

    public OffHeapContainer(long maxSize, Time.NanosService time, Duration maxIdle) {
        this(maxSize, Marshalling.JAVA, time, maxIdle);
    }

    OffHeapContainer(long maxSize, Marshalling marshalling, Time.NanosService time, Duration maxIdle) {
        this.maxSize = maxSize;
        this.locks = new StripedLock();

        int bucketAddressCount = getActualAddressCount(ADDRESS_COUNT);
        this.bucketMemory = createBucketMemory(bucketAddressCount);

        this.marshalling = marshalling;
        this.time = time;

        this.isTransient = !Time.isForever(maxIdle);
        this.maxIdleNanos = isTransient ? maxIdle.toNanos() : -1;
    }

    private BucketMemory createBucketMemory(int bucketAddressCount) {
        lruLock.lock();
        try {
            final BucketMemory bucketMemory = new BucketMemory(bucketAddressCount);
            currentSize += bucketMemory.usedMemory();
            return bucketMemory;
        } finally {
            lruLock.unlock();
        }
    }

    private static int getActualAddressCount(int addressCount) {
        int memoryAddresses = addressCount >= MAX_ADDRESS_COUNT
                ? MAX_ADDRESS_COUNT
                : Environment.BLOCK_COUNT;
        while (memoryAddresses < addressCount) {
            memoryAddresses <<= 1;
        }
        return memoryAddresses;
    }

    public void stop() {
        locks.lockAll();
        try {
            invalidateAll();
            bucketMemory.deallocate();
            dellocated = true;
        } finally {
            locks.unlockAll();
        }

        lruLock.lock();
        try {
            currentSize -= bucketMemory.usedMemory();
        } finally {
            lruLock.unlock();
        }
    }

    public void invalidateAll() {
        locks.lockAll();
        try {
            checkDeallocation();
            deallocateAll();
            count.set(0);
        } finally {
            locks.unlockAll();
        }
    }

    private void checkDeallocation() {
        if (dellocated) {
            throw new IllegalStateException("Map was already shut down!");
        }
    }

    private void deallocateAll() {
        bucketMemory.deallocateBuckets(this::deallocate);
    }

    public void putIfAbsent(Object key, Object value) {
        if (trace) {
            log.tracef("Cache put if absent key=%s value=%s", key, value);
        }

        final byte[] keyBytes = marshalling.marshall().apply(key);
        final byte[] valueBytes = marshalling.marshall().apply(value);
        putIfAbsentBytes(keyBytes, valueBytes);
        ensureSize();
    }

    private void putIfAbsentBytes(byte[] key, byte[] value) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
            checkDeallocation();
            long bucketAddress = bucketMemory.getBucketAddress(key);
            long entryAddress = bucketAddress == 0 ? 0 : getEntry(bucketAddress, key);

            if (entryAddress == 0) {
                final long resultAddress = toMemory(key, value);
                putEntry(bucketAddress, resultAddress, key, entryAddress);
            }
        } finally {
            lock.unlock();
        }
    }

    public Object get(Object key) {
        if (trace) {
            log.tracef("Cache get(key=%s)", key);
        }

        final Object value = marshalling.marshall()
                .andThen(GET_BYTES)
                .andThen(marshalling.unmarshall())
                .apply(key);

        if (trace) {
            log.tracef("Cache get(key=%s) returns: %s", key, value);
        }

        return value;
    }

    private byte[] getBytes(byte[] key) {
        Lock lock = locks.getLock(key).readLock();
        lock.lock();
        try {
            checkDeallocation();
            long bucketAddress = bucketMemory.getBucketAddress(key);
            if (bucketAddress == 0) {
                return null;
            }

            long entryAddress = getEntry(bucketAddress, key);
            if (entryAddress != 0) {
                entryRetrieved(entryAddress);
                return fromMemory(entryAddress);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    private long getEntry(long bucketAddress, byte[] k) {
        long address = bucketAddress;
        while (address != 0) {
            long nextAddress = getNextAddress(address);
            if (equalsKey(address, k)) {
                break;
            } else {
                address = nextAddress;
            }
        }
        return address;
    }

    public void put(Object key, Object value) {
        if (trace) {
            log.tracef("Cache put key=%s value=%s", key, value);
        }

        final byte[] keyBytes = marshalling.marshall().apply(key);
        final byte[] valueBytes = marshalling.marshall().apply(value);
        putBytes(keyBytes, valueBytes);
        ensureSize();
    }

    public void putBytes(byte[] key, byte[] value) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
            checkDeallocation();
            long entryAddress = toMemory(key, value);
            long bucketAddress = bucketMemory.getBucketAddress(key);
            putEntry(bucketAddress, entryAddress, key, 0);
        } finally {
            lock.unlock();
        }
    }

    private void putEntry(long bucketAddress, long entryAddress, byte[] key, long currentAddress) {
        // Have to start new linked node list
        if (bucketAddress == 0) {
            bucketMemory.putBucketAddress(key, entryAddress);
            entryCreated(entryAddress);
        } else {
            boolean replaceHead = false;
            boolean foundPrevious = false;

            long address = bucketAddress;
            // Holds the previous linked list address
            long prevAddress = 0;
            while (address != 0) {
                long nextAddress = getNextAddress(address);
                if (equalsKey(address, key, currentAddress)) {
                    foundPrevious = true;
                    entryReplaced(entryAddress, address);
                    // If this is true it means this was the first node in the linked list
                    if (prevAddress == 0) {
                        if (nextAddress == 0) {
                            // This branch is the case where our key is the only one in the linked list
                            replaceHead = true;
                        } else {
                            // This branch is the case where our key is the first with another after
                           bucketMemory.putBucketAddress(key, nextAddress);
                        }
                    } else {
                        // This branch means our node was not the first,
                        // so we have to update the address before ours
                        // to the one we previously referenced
                        setNextAddress(prevAddress, nextAddress);
                        // We purposely don't update prevAddress, because we have to keep it as the current pointer
                        // since we removed ours
                        address = nextAddress;
                        continue;
                    }
                }
                prevAddress = address;
                address = nextAddress;
            }
            // If we didn't find the key previous, it means we are a new entry
            if (!foundPrevious) {
                entryCreated(entryAddress);
            }
            if (replaceHead) {
                bucketMemory.putBucketAddress(key, entryAddress);
            } else {
                // Now prevAddress should be the last link so we fix our link
                setNextAddress(prevAddress, entryAddress);
            }
        }
    }

    private void entryRetrieved(long entryAddress) {
        lruLock.lock();
        try {
            if (trace) {
                log.tracef("Moving entry 0x%016x to the end of the LRU list", entryAddress);
            }
            moveToLruEnd(entryAddress);
        } finally {
            lruLock.unlock();
        }
    }

    private void entryRemoved(long removedAddress) {
        long removedSize = usedMemoryAt(removedAddress);
        lruLock.lock();
        try {
            // Current size has to be updated in the lock
            currentSize -=  removedSize;
            removeFromLru(removedAddress);
            deallocate(removedAddress);
        } finally {
            lruLock.unlock();
        }
    }

    private void entryReplaced(long newAddress, long oldAddress) {
        long oldSize = usedMemoryAt(oldAddress);
        long newSize = usedMemoryAt(newAddress);
        lruLock.lock();
        try {
            removeFromLru(oldAddress);
            appendToLru(newAddress);

            currentSize += newSize;
            currentSize -= oldSize;
            // Must only remove entry while holding lru lock now
            deallocate(oldAddress);
        } finally {
            lruLock.unlock();
        }
    }

    private void entryCreated(long newAddress) {
        long newSize = usedMemoryAt(newAddress);
        lruLock.lock();
        try {
            currentSize += newSize;
            appendToLru(newAddress);
        } finally {
            lruLock.unlock();
        }
        count.incrementAndGet();
    }

    /**
     * Method to be invoked when adding a new entry address to the end of the lru nodes.
     * This occurs for newly created entries.
     * 
     * This method should only be invoked after acquiring the lruLock.
     *
     * @param entryAddress the new entry address pointer *NOT* the lru node
     */
    private void appendToLru(long entryAddress) {
        if (trace) {
            log.tracef("Adding entry 0x%016x to the end of the LRU list", entryAddress);
        }
        // This means it is the first entry
        if (lastAddress == 0) {
            firstAddress = entryAddress;
            lastAddress = entryAddress;
            // Have to make sure the memory is cleared so we don't use unitialized values
            LruNode.setPrevious(entryAddress, 0);
        } else {
            // Writes back pointer to the old lastAddress
            LruNode.setPrevious(entryAddress, lastAddress);
            // Write the forward pointer in old lastAddress to point to us
            LruNode.setNext(lastAddress, entryAddress);
            // Finally make us the last address
            lastAddress = entryAddress;
        }
        // Since we are last there is no pointer after us
        LruNode.setNext(entryAddress, 0);
    }

    /**
     * Removes the address node and updates previous and next lru node pointers properly.
     *
     * The lruLock <b>must</b> be held when invoking this.
     */
    private void removeFromLru(long address) {
        boolean middleNode = true;
        if (address == lastAddress) {
            if (trace) {
                log.tracef("Removed entry 0x%016x from the end of the LRU list", address);
            }
            long previousLRUNode = LruNode.getPrevious(address);
            if (previousLRUNode != 0) {
                LruNode.setNext(previousLRUNode, 0);
            }
            lastAddress = previousLRUNode;
            middleNode = false;
        }
        if (address == firstAddress) {
            if (trace) {
                log.tracef("Removed entry 0x%016x from the beginning of the LRU list", address);
            }
            long nextLRUNode = LruNode.getNext(address);
            if (nextLRUNode != 0) {
                LruNode.setPrevious(nextLRUNode, 0);
            }
            firstAddress = nextLRUNode;
            middleNode = false;
        }
        if (middleNode) {
            if (trace) {
                log.tracef("Removed entry 0x%016x from the middle of the LRU list", address);
            }
            // We are a middle pointer so both of these have to be non zero
            long previousLRUNode = LruNode.getPrevious(address);
            long nextLRUNode = LruNode.getNext(address);
            assert previousLRUNode != 0;
            assert nextLRUNode != 0;
            LruNode.setNext(previousLRUNode, nextLRUNode);
            LruNode.setPrevious(nextLRUNode, previousLRUNode);
        }
    }

    /**
     * Method to be invoked when moving an existing lru node to the end.  This occurs when the entry is accessed for this
     * node.
     * This method should only be invoked after acquiring the lruLock.
     *
     * @param lruNode the node to move to the end
     */
    private void moveToLruEnd(long lruNode) {
        if (lruNode != lastAddress) {
            long nextLruNode = LruNode.getNext(lruNode);
            assert nextLruNode != 0;
            if (lruNode == firstAddress) {
                LruNode.setPrevious(nextLruNode, 0);
                firstAddress = nextLruNode;
            } else {
                long prevLruNode = LruNode.getPrevious(lruNode);
                assert prevLruNode != 0;
                LruNode.setNext(prevLruNode, nextLruNode);
                LruNode.setPrevious(nextLruNode, prevLruNode);
            }
            // Link the previous last node to our new last node
            LruNode.setNext(lastAddress, lruNode);
            // Sets the previous node of our new tail node to the previous tail node
            LruNode.setPrevious(lruNode, lastAddress);
            LruNode.setNext(lruNode, 0);
            lastAddress = lruNode;
        }
    }

    public long count() {
        if (isTransient) {
            long size = 0;
            long current = firstAddress;
            while (current != 0) {
                final long nextAddress = LruNode.getNext(current);
                final long lastUsed = getLastUsed(current);

                if (isExpired(lastUsed)) {
                    final byte[] keyBytes = getKeyAt(current);
                    invalidateBytesAt(keyBytes, current);
                } else {
                    size++;
                }

                current = nextAddress;
            }

            return size;
        }


        return count.get();
    }

    public void invalidate(Object key) {
        byte[] keyBytes = marshalling.marshall().apply(key);
        invalidateBytes(keyBytes);
    }

    private void invalidateBytes(byte[] key) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
            checkDeallocation();
            long bucketAddress = bucketMemory.getBucketAddress(key);
            if (bucketAddress != 0) {
                invalidateEntry(bucketAddress, key, 0);
            }
        } finally {
            lock.unlock();
        }
    }

    private void invalidateEntry(long bucketAddress, byte[] key, long currentAddress) {
        long prevAddress = 0;
        // We only use the head pointer for the first iteration
        long address = bucketAddress;
        while (address != 0) {
            long nextAddress = getNextAddress(address);
            boolean remove = equalsKey(address, key, currentAddress);
            if (remove) {
                entryRemoved(address);
                if (prevAddress != 0) {
                    setNextAddress(prevAddress, nextAddress);
                } else {
                    bucketMemory.putBucketAddress(key, nextAddress);
                }
                count.decrementAndGet();
                break;
            }
            prevAddress = address;
            address = nextAddress;
        }
    }

    // Assumes that write lock has already been acquired
    private void invalidateBytesAt(byte[] key, long currentAddress) {
        long bucketAddress = bucketMemory.getBucketAddress(key);
        assert bucketAddress != 0;
        invalidateEntry(bucketAddress, key, currentAddress);
    }

    private boolean equalsKey(long address, byte[] key, long currentAddress) {
        if (currentAddress == 0)
            return equalsKey(address, key);

        return currentAddress == address;
    }

    private boolean equalsKey(long address, byte[] key) {
        // 16 bytes for eviction
        // 8 bytes for linked pointer
        int headerOffset = 24;
        byte type = MEMORY.getByte(address, headerOffset);
        headerOffset++;
        // First if hashCode doesn't match then the key can't be equal
        int hashCode = Arrays.hashCode(key);
        if (hashCode != MEMORY.getInt(address, headerOffset)) {
            return false;
        }
        headerOffset += 4;

        // Skip last used time if transient
        if (isTransient)
            headerOffset += 8;

        // If the length of the key is not the same it can't match either!
        int keyLength = MEMORY.getInt(address, headerOffset);
        if (keyLength != key.length) {
            return false;
        }
        headerOffset += 4;
        // This is for the value size which we don't need to read
        headerOffset += 4;

        // Finally read each byte individually so we don't have to copy them into a byte[]
        for (int i = 0; i < keyLength; i++) {
            byte b = MEMORY.getByte(address, headerOffset + i);
            if (b != key[i])
                return false;
        }

        return true;
    }

    private static void setNextAddress(long address, long value) {
        MEMORY.putLong(address, 16, value);
    }

    private static long getNextAddress(long address) {
        return MEMORY.getLong(address, 16);
    }

    /**
     * Create an entry off-heap.
     *
     * The first 8 bytes will always be 0,
     * reserved for a future reference to another entry.
     */
    private long toMemory(byte[] key, byte[] value) {
        int keySize = key.length;
        int valueSize = value.length;

        // Eviction requires 2 additional pointers at the beginning
        int offset = 16;
        // Next 8 is for linked pointer to next address
        long totalSize = offset + 8 + HEADER_LENGTH + (isTransient ? 8 : 0) + keySize + valueSize;
        long memoryAddress = allocate(totalSize);

        // Write the empty linked address pointer first
        MEMORY.putLong(memoryAddress, offset, 0);
        offset += 8;

        MEMORY.putByte(memoryAddress, offset, IMMORTAL);
        offset += 1;

        MEMORY.putInt(memoryAddress, offset, bytesHashCode(key));
        offset += 4;

        if (isTransient) {
            MEMORY.putLong(memoryAddress, offset, time.nanoTime());
            offset += 8;
        }

        MEMORY.putInt(memoryAddress, offset, key.length);
        offset += 4;
        MEMORY.putInt(memoryAddress, offset, value.length);
        offset += 4;

        MEMORY.putBytes(key, 0, memoryAddress, offset, keySize);
        offset += keySize;
        MEMORY.putBytes(value, 0, memoryAddress, offset, valueSize);
        offset += valueSize;

        assert offset == totalSize;

        return memoryAddress;
    }

    private static long allocate(long totalSize) {
        return MEMORY.allocate(totalSize);
    }

    private void deallocate(long address) {
        MEMORY.deallocate(address, memorySize(address));
    }

    private byte[] fromMemory(final long address) {
        // Eviction pointers (16)
        // Next pointer (8)
        int offset = 24;

        byte metadataType = MEMORY.getByte(address, offset);
        offset += 1;
        int hashCode = MEMORY.getInt(address, offset);
        offset += 4;

        boolean isExpired = false;
        if (isTransient) {
            long lastUsed = MEMORY.getLong(address, offset);
            isExpired = isExpired(lastUsed);
            offset += 8;
        }

        byte[] keyBytes = new byte[MEMORY.getInt(address, offset)];
        offset += 4;

        byte[] valueBytes = new byte[MEMORY.getInt(address, offset)];
        offset += 4;

        MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
        offset += keyBytes.length;

        if (isExpired) {
            invalidateBytesAt(keyBytes, address);
            return null;
        }

        MEMORY.getBytes(address, offset, valueBytes, 0, valueBytes.length);
        // offset += valueBytes.length;

        return valueBytes;
    }

    private boolean isExpired(long lastUsed) {
        final long now = time.nanoTime();
        return now > lastUsed + maxIdleNanos;
    }

    private long memorySize(long address) {
        // Eviction pointers (16)
        // Next pointer (8)
        int offset = 24;

        // Skip metadata type
        offset += 1;
        // Skip the hashCode
        offset += 4;
        int keyLength = MEMORY.getInt(address, offset);
        offset += 4;

        int valueLength = MEMORY.getInt(address, offset);
        offset += 4;

        // Last used time, if transient
        if (isTransient)
            offset += 8;

        return offset + keyLength + valueLength;
    }

    // Size + overhead
    private long usedMemoryAt(long address) {
        return MEMORY.estimateSizeOverhead(memorySize(address));
    }

    private byte[] getKeyAt(long address) {
        // 16 bytes for eviction
        // 8 bytes for linked pointer
        // 1 for metadata type
        // 4 for hash code
        int offset = isTransient ? 37 : 29;

        byte[] keyBytes = new byte[MEMORY.getInt(address, offset)];
        offset += 4;

        // Ignore value bytes
        offset += 4;

        // Finally read the bytes and return
        MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
        return keyBytes;
    }

    private int getHashCodeAt(long address) {
        // 16 bytes for eviction
        // 8 bytes for linked pointer
        // 1 for type
        int offset = 25;
        return MEMORY.getInt(address, offset);
    }

    private long getLastUsed(long address) {
        // 16 bytes for eviction
        // 8 bytes for linked pointer
        // 1 for metadata type
        // 4 for hash code
        int offset = 29;
        return MEMORY.getLong(address, offset);
    }

    /**
     * This method repeatedly removes the head of the LRU list until there the current size is less than or equal to
     * `maxSize`.
     * <p>
     * We need to hold the LRU lock in order to check the current size and to read the head entry,
     * and then we need to hold the head entry's write lock in order to remove it.
     * The problem is that the correct acquisition order is entry write lock first, LRU lock second,
     * and we need to hold the LRU lock so that we know which entry write lock to acquire.
     * <p>
     * To work around it, we first try to acquire the entry write lock without blocking.
     * If that fails, we release the LRU lock and we acquire the locks in the correct order, hoping that
     * the LRU head doesn't change while we wait. Because the entry write locks are striped, we actually
     * tolerate a LRU head change as long as the new head entry is in the same lock stripe.
     * If the LRU list head changes, we release both locks and try again.
     */
    private void ensureSize() {
        while (true) {
            long addressToRemove;
            Lock entryWriteLock;
            byte[] key;
            lruLock.lock();
            try {
                if (currentSize <= maxSize) {
                    break;
                }
                // We shouldn't be able to get into this state
                assert firstAddress > 0;
                // We read the key before hashCode due to how off heap bytes are written (key requires reading metadata
                // which comes before hashCode, which should keep hashCode bytes in memory register in most cases)
                key = getKeyAt(firstAddress);

                // This is always non null
                entryWriteLock = locks.getLock(key).writeLock();
                if (entryWriteLock.tryLock()) {
                    addressToRemove = firstAddress;
                } else {
                    addressToRemove = 0;
                }
            } finally {
                lruLock.unlock();
            }

            // If we got here it means we were unable to acquire the write lock, so we have to attempt a blocking
            // write lock and then acquire the lruLock, since they have to be acquired in that order (exception using
            // try lock as above)
            if (addressToRemove == 0) {
                entryWriteLock.lock();
                try {
                    lruLock.lock();
                    try {
                        if (currentSize <= maxSize) {
                            break;
                        }

                        // Now that we have locks we have to verify the first address is protected by the same lock still
                        int hashCode = getHashCodeAt(firstAddress);
                        Lock innerLock = locks.getLockFromHashCode(hashCode).writeLock();
                        if (innerLock == entryWriteLock) {
                            addressToRemove = firstAddress;
                        }
                    } finally {
                        lruLock.unlock();
                    }
                } finally {
                    if (addressToRemove == 0) {
                        entryWriteLock.unlock();
                    }
                }
            }

            if (addressToRemove != 0) {
                if (trace) {
                    log.tracef("Removing entry: 0x%016x due to eviction due to size %d being larger than maximum of %d",
                            addressToRemove, currentSize, maxSize);
                }
                try {
                    invalidateBytesAt(key, addressToRemove);
                } finally {
                    entryWriteLock.unlock();
                }
            }
        }
    }

    private static int bytesHashCode(byte[] bytes) {
        return Arrays.hashCode(bytes);
    }

}
