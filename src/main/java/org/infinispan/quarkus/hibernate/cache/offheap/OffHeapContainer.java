package org.infinispan.quarkus.hibernate.cache.offheap;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class OffHeapContainer {

    // Max would be 1:1 ratio with memory addresses - must be a crazy machine to have that many processors
    private static final int MAX_ADDRESS_COUNT = 1 << 30;

    private static final int HEADER_LENGTH = 1 + 4 + 4 + 4;

    private static final byte IMMORTAL = 1 << 2;

    private static final Memory MEMORY = Memory.INSTANCE;

    private final Function<byte[], byte[]> GET_BYTES = this::getBytes;

    private final AtomicLong size = new AtomicLong();

    private final StripedLock locks;
    private final int memoryAddressCount;
    private final Marshalling marshalling;

    final BucketMemory bucketMemory;

    public OffHeapContainer(int desiredSize) {
        this(desiredSize, Marshalling.JAVA);
    }

    OffHeapContainer(int desiredSize, Marshalling marshalling) {
        this.memoryAddressCount = getActualAddressCount(desiredSize);
        this.locks = new StripedLock();
        this.bucketMemory = new BucketMemory(this.memoryAddressCount);
        this.marshalling = marshalling;
    }

    private static int getActualAddressCount(int desiredSize) {
        int memoryAddresses = desiredSize >= MAX_ADDRESS_COUNT
                ? MAX_ADDRESS_COUNT
                : Environment.BLOCK_COUNT;
        while (memoryAddresses < desiredSize) {
            memoryAddresses <<= 1;
        }
        return memoryAddresses;
    }

    // TODO a relatively expensive operation given all the marshall/unmarshall happening
    // TODO should strive to get remapping functions that can extract info out of the byte[] directly
    public Object compute(Object key, BiFunction<Object, Object, Object> remappingFunction) {
        final byte[] keyBytes = marshalling.marshall().apply(key);
        BiFunction<byte[], byte[], byte[]> remappingFnBytes = (k, prev) -> {
            final Object keyObj = marshalling.unmarshall().apply(k);
            final Object prevObj = marshalling.unmarshall().apply(prev);
            final Object result = remappingFunction.apply(keyObj, prevObj);
            return marshalling.marshall().apply(result);
        };

        final byte[] result = compute(keyBytes, remappingFnBytes);
        return marshalling.unmarshall().apply(result);
    }

    private byte[] compute(byte[] key, BiFunction<byte[], byte[], byte[]> remappingFunction) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
            long bucketAddress = bucketMemory.getBucketAddress(key);
            long entryAddress = bucketAddress == 0 ? 0 : getEntry(bucketAddress, key);

            byte[] prev;
            if (entryAddress != 0) {
                prev = fromMemory(entryAddress);
            } else {
                prev = null;
            }

            byte[] result = remappingFunction.apply(key, prev);
            if (prev != result) {
                if (result != null) {
                    final long resultAddress = toMemory(key, result);
                    putEntry(bucketAddress, resultAddress, key, entryAddress);
                } else {
                    invalidateEntry(bucketAddress, key, entryAddress);
                }
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    public Object get(Object key) {
        return marshalling.marshall()
                .andThen(GET_BYTES)
                .andThen(marshalling.unmarshall())
                .apply(key);
    }

    private byte[] getBytes(byte[] key) {
        Lock lock = locks.getLock(key).readLock();
        lock.lock();
        try {
            long bucketAddress = bucketMemory.getBucketAddress(key);
            if (bucketAddress == 0) {
                return null;
            }

            long entryAddress = getEntry(bucketAddress, key);
            if (entryAddress != 0) {
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
        final byte[] keyBytes = marshalling.marshall().apply(key);
        final byte[] valueBytes = marshalling.marshall().apply(value);
        putBytes(keyBytes, valueBytes);

        // TODO ensure size
    }

    public void putBytes(byte[] key, byte[] value) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
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
            size.incrementAndGet();
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
                size.incrementAndGet();
            }
            if (replaceHead) {
                bucketMemory.putBucketAddress(key, entryAddress);
            } else {
                // Now prevAddress should be the last link so we fix our link
                setNextAddress(prevAddress, entryAddress);
            }
        }
    }

    private void invalidate(byte[] key) {
        Lock lock = locks.getLock(key).writeLock();
        lock.lock();
        try {
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
                if (prevAddress != 0) {
                    setNextAddress(prevAddress, nextAddress);
                } else {
                    bucketMemory.putBucketAddress(key, nextAddress);
                }
            }
            prevAddress = address;
            address = nextAddress;
        }
    }

    private boolean equalsKey(long address, byte[] key, long currentAddress) {
        if (currentAddress == 0)
            return equalsKey(address, key);

        return currentAddress == address;
    }

    private boolean equalsKey(long address, byte[] key) {
        int headerOffset = 8;
        byte type = MEMORY.getByte(address, headerOffset);
        headerOffset++;
        // First if hashCode doesn't match then the key can't be equal
        int hashCode = Arrays.hashCode(key);
        if (hashCode != MEMORY.getInt(address, headerOffset)) {
            return false;
        }
        headerOffset += 4;
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

    private void setNextAddress(long address, long value) {
        MEMORY.putLong(address, 0, value);
    }

    private long getNextAddress(long address) {
        return MEMORY.getLong(address, 0);
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

        // Next 8 is for linked pointer to next address
        long totalSize = 8 + HEADER_LENGTH + + keySize + valueSize;
        long memoryAddress = MEMORY.allocate(totalSize);

        int offset = 0;

        // Write the empty linked address pointer first
        MEMORY.putLong(memoryAddress, offset, 0);
        offset += 8;

        MEMORY.putByte(memoryAddress, offset, IMMORTAL);
        offset += 1;

        MEMORY.putInt(memoryAddress, offset, bytesHashCode(key));
        offset += 4;
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

    private byte[] fromMemory(long address) {
        int offset = 8;

        byte metadataType = MEMORY.getByte(address, offset);
        offset += 1;
        int hashCode = MEMORY.getInt(address, offset);
        offset += 4;
        byte[] keyBytes = new byte[MEMORY.getInt(address, offset)];
        offset += 4;

        byte[] valueBytes = new byte[MEMORY.getInt(address, offset)];
        offset += 4;

        MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
        offset += keyBytes.length;
        MEMORY.getBytes(address, offset, valueBytes, 0, valueBytes.length);
        // offset += valueBytes.length;

        return valueBytes;
    }

    private int bytesHashCode(byte[] bytes) {
        return Arrays.hashCode(bytes);
    }
}