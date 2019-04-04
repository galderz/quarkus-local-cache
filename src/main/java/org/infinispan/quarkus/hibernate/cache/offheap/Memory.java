package org.infinispan.quarkus.hibernate.cache.offheap;

import org.jboss.logging.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

final class Memory {

    private static final Logger log = Logger.getLogger(Memory.class);
    private static final boolean trace = log.isTraceEnabled();

    private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;
    private static final int BYTE_ARRAY_BASE_OFFSET = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

    private static final ConcurrentHashMap<Long, Long> ALLOCATED_BLOCKS = new ConcurrentHashMap<>();
    private static final LongAdder ALLOCATED = new LongAdder();

    static final Memory INSTANCE = new Memory();

    private Memory() {
    }

    long allocate(long size) {
        long estimatedMemoryLength = estimateSizeOverhead(size);

        long address = UNSAFE.allocateMemory(size);
        if (trace) {
            Long prev = ALLOCATED_BLOCKS.put(address, size);
            if (prev != null) {
                throw new IllegalArgumentException();
            }
        }

        ALLOCATED.add(estimatedMemoryLength);
        if (trace) {
            log.tracef("Allocated off heap memory at 0x%016x with %d bytes. Total size: %d",
                    size, estimatedMemoryLength, ALLOCATED.sum());
        }

        return address;
    }

    void putLong(long destAddress, long offset, long value) {
        checkAddress(destAddress, offset + 8);
        if (trace) {
            log.tracef("Wrote long value 0x%016x to address 0x%016x+%d", value, destAddress, offset);
        }
        UNSAFE.putLong(destAddress + offset, value);
    }

    void putByte(long destAddress, long offset, byte value) {
        checkAddress(destAddress, offset + 1);
        if (trace) {
            log.tracef("Wrote byte value 0x%02x to address 0x%016x+%d", 
                    value, destAddress, offset);
        }
        UNSAFE.putByte(destAddress + offset, value);
    }

    void putInt(long destAddress, long offset, int value) {
        checkAddress(destAddress, offset + 4);
        if (trace) {
            log.tracef("Wrote int value 0x%08x to address 0x%016x+%d", 
                    value, destAddress, offset);
        }
        UNSAFE.putInt(destAddress + offset, value);
    }

    void putBytes(byte[] srcArray, long srcOffset, long destAddress, long destOffset, long length) {
        checkAddress(destAddress, destOffset + length);
        if (trace) {
            log.tracef("Wrote %d bytes from array %s+%d to address 0x%016x+%d", 
                    length, Arrays.toString(srcArray), srcOffset, destAddress, destOffset);
        }
        UNSAFE.copyMemory(srcArray, BYTE_ARRAY_BASE_OFFSET + srcOffset, null, destAddress + destOffset, length);
    }

    void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    byte getByte(long srcAddress, long offset) {
        checkAddress(srcAddress, offset + 1);
        byte value = UNSAFE.getByte(srcAddress + offset);
        if (trace) {
            log.tracef("Read byte value 0x%02x from address 0x%016x+%d", value, srcAddress, offset);
        }
        return value;
    }

    int getInt(long srcAddress, long offset) {
        checkAddress(srcAddress, offset + 4);
        int value = UNSAFE.getInt(srcAddress + offset);
        if (trace) {
            log.tracef("Read int value 0x%08x from address 0x%016x+%d", value, srcAddress, offset);
        }
        return value;
    }

    long getLong(long srcAddress, long offset) {
        return getLong(srcAddress, offset, true);
    }

    private long getLong(long srcAddress, long offset, boolean alwaysTrace) {
        checkAddress(srcAddress, offset + 8);
        long value = UNSAFE.getLong(srcAddress + offset);
        if (trace && (alwaysTrace || value != 0)) {
            log.tracef("Read long value 0x%016x from address 0x%016x+%d", value, srcAddress, offset);
        }
        return value;
    }

    void getBytes(long srcAddress, long srcOffset, byte[] destArray, long destOffset, long length) {
        checkAddress(srcAddress, srcOffset + length);
        if (trace) {
            log.tracef("Read %d bytes from address 0x%016x+%d into array %s+%d", length, srcAddress, srcOffset, destArray, destOffset);
        }
        UNSAFE.copyMemory(null, srcAddress + srcOffset, destArray, BYTE_ARRAY_BASE_OFFSET + destOffset, length);
    }

    private static long estimateSizeOverhead(long size) {
        // We take 8 and add the number provided and then round up to 16 (& operator has higher precedence than +)
        return (size + 8 + 15) & ~15;
    }

    private static void checkAddress(long address, long offset) {
        if (!trace)
            return;

        Long blockSize = ALLOCATED_BLOCKS.get(address);
        if (blockSize == null || blockSize < offset) {
            throw new IllegalArgumentException(String.format("Trying to access address 0x%016x+%d, but blockSize was %d",
                    address, offset, blockSize));
        }
    }

    private static class UnsafeHolder {
        static sun.misc.Unsafe UNSAFE = UnsafeHolder.getUnsafe();

        @SuppressWarnings("restriction")
        private static sun.misc.Unsafe getUnsafe() {
            // attempt to access field Unsafe#theUnsafe
            final Object maybeUnsafe = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    final Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                    unsafeField.setAccessible(true);
                    // the unsafe instance
                    return unsafeField.get(null);
                } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
                    return e;
                }
            });
            if (maybeUnsafe instanceof Exception) {
                throw new RuntimeException((Exception) maybeUnsafe);
            } else {
                return (sun.misc.Unsafe) maybeUnsafe;
            }
        }
    }

}
