package org.infinispan.quarkus.hibernate.cache.offheap;

import java.util.Arrays;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class BucketMemory {

    private static final Memory MEMORY = Memory.INSTANCE;

    private final long memory;
    private final int pointerCount;

    BucketMemory(int pointers) {
        this.pointerCount = Math.findNextHighestPowerOfTwo(pointers);
        long bytes = ((long) pointerCount) << 3;
        memory = MEMORY.allocate(bytes);
        MEMORY.setMemory(memory, bytes, (byte) 0);
    }

    long getBucketAddress(byte[] bytes) {
        final long offset = offset(bytes);
        return MEMORY.getLong(memory, offset);
    }

     long offset(byte[] bytes) {
        final int hashCode = Arrays.hashCode(bytes);
        return Math.calculateOffset(hashCode) << 3;
    }

    void putBucketAddress(byte[] bytes, long address) {
        final long offset = offset(bytes);
        MEMORY.putLong(memory, offset, address);
    }

    void deallocateBuckets(LongConsumer action) {
        LongStream.iterate(memory, l -> l + 8)
                .limit(pointerCount)
                .map(MEMORY::zero)
                .filter(l -> l != 0)
                .forEach(action);
    }

    void deallocate() {
        MEMORY.deallocate(memory, pointerCount << 3);
    }

}
