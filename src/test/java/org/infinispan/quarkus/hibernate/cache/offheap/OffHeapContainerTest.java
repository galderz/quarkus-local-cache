package org.infinispan.quarkus.hibernate.cache.offheap;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OffHeapContainerTest {

    private static final Marshalling MARSHALL = Marshalling.UTF8;

    private OffHeapContainer container;

    @Before
    public void createContainer() {
        container = new OffHeapContainer(1024, MARSHALL);
    }

    @Test
    public void testNotFound() {
        assertNull(container.get("0"));
    }

    @Test
    public void testPut() {
        String key = "100";
        String putValue = "v100";
        container.put(key, putValue);

        Object getValue = container.get(key);
        assertEquals(putValue, getValue);
    }

    @Test
    public void testPutOverwrite() {
        String key = "110";
        container.put(key, "v110");

        String v2 = "v111";
        container.put(key, v2);

        Object getValue = container.get(key);
        assertEquals(v2, getValue);
    }

    @Test
    public void testPutInSameBucket() {
        String k1 = "120";
        String k2 = forBucket(k1, container);

        final String v1 = "v120";
        container.put(k1, v1);
        final String v2 = "v" + k2;
        container.put(k2, v2);

        assertEquals(v1, container.get(k1));
        assertEquals(v2, container.get(k2));
    }

    @Test
    public void testComputePutIfAbsent() {
        String key = "130";

        BiFunction<Object, Object, Object> fn1 = (k, prev) ->
                prev == null ? "v" + k : prev;

        container.compute(key, fn1);
        assertEquals("v130", container.get(key));

        BiFunction<Object, Object, Object> fn2 = (k, prev) ->
                prev == null ? "NA" : prev;

        container.compute(key, fn2);
        assertEquals("v130", container.get(key));
    }

    @Test
    public void testComputeReplace() {
        String key = "140";

        BiFunction<Object, Object, Object> fn1 = (k, prev) ->
                prev != null ? "v" + k : null;

        container.compute(key, fn1);
        assertNull(container.get(key));

        final String v1 = "v130";
        container.put(key, v1);
        assertEquals(v1, container.get(key));

        final String v2 = "v131";
        BiFunction<Object, Object, Object> fn2 = (k, prev) ->
                prev != null ? v2 : null;

        container.compute(key, fn2);
        assertEquals(v2, container.get(key));
    }

    @Test
    public void testComputeReplaceIfEquals() {
        String key = "150";
        String v1 = "v" + key;
        String v2 = "v151";

        container.put(key, v1);

        BiFunction<Object, Object, Object> fn1 = (k, prev) ->
                v1.equals(prev) ? v2 : prev;

        container.compute(key, fn1);
        assertEquals(v2, container.get(key));

        container.compute(key, fn1);
        assertEquals(v2, container.get(key));
    }

    @Test
    public void testComputeRemoveIfEquals() {
        String key = "160";
        String v = "v" + key;

        container.put(key, v);

        BiFunction<Object, Object, Object> fn1 = (k, prev) ->
                "NA".equals(prev) ? null : prev;

        container.compute(key, fn1);
        assertEquals(v, container.get(key));

        BiFunction<Object, Object, Object> fn2 = (k, prev) ->
                v.equals(prev) ? null : prev;

        container.compute(key, fn2);
        assertNull(container.get(key));
    }

    public static String forBucket(String s, OffHeapContainer container) {
        long offset = container.bucketMemory.offset(MARSHALL.marshall().apply(s));
        int attemptsLeft = 1_000_000;
        String dummy;
        do {
            dummy = generateRandomString(3);
            attemptsLeft--;
            if (attemptsLeft < 0) {
                throw new IllegalStateException(
                        "Could not find any key that hashes to same bucket as '" + s + "'");
            }
        } while (offset != container.bucketMemory.offset(MARSHALL.marshall().apply(dummy)));

        return dummy;
    }

    public static String generateRandomString(int numberOfChars) {
        Random r = new Random(System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfChars; i++) 
            sb.append((char) (64 + r.nextInt(26)));

        return sb.toString();
    }

}
