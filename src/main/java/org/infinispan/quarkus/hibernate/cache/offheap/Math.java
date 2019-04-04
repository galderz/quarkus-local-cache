package org.infinispan.quarkus.hibernate.cache.offheap;

import java.util.function.Function;

class Math {

    // usable bits of normal node hash (only allow positive numbers)
    private static final int HASH_BITS = 0x7fffffff;

    static int findNextHighestPowerOfTwo(int num) {
        if (num <= 1) {
            return 1;
        } else if (num >= 0x40000000) {
            return 0x40000000;
        }
        int highestBit = Integer.highestOneBit(num);
        return num <= highestBit ? highestBit : highestBit << 1;
    }

    static int calculateOffset(int hashCode) {
        return spread(hashCode) % Environment.BLOCK_COUNT;
    }

    private static int spread(int h) {
        return (h ^ (h >>> 16)) & HASH_BITS;
    }

    private Math() {
    }

}