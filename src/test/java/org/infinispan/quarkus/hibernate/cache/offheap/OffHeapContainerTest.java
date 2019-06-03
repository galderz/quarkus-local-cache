package org.infinispan.quarkus.hibernate.cache.offheap;

import org.infinispan.quarkus.hibernate.cache.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OffHeapContainerTest {

    private static final Marshalling MARSHALL = Marshalling.UTF8;

    private OffHeapContainer container;

    @Before
    public void createContainer() {
        // Max size is at least number of bucket pointers, plus a little bit more
        int maxSize = (1 << 20) * 8 + 1024;
        container = new OffHeapContainer(maxSize, MARSHALL, null, Time.forever());
    }

    @After
    public void destroyContainer() {
        container.stop();
    }

    @Test
    public void testInvalidate() {
        String key = "100";
        String putValue = "v100";
        container.put(key, putValue);

        Object getValue = container.get(key);
        assertEquals(putValue, getValue);

        container.invalidate(key);
        assertNull(container.get(key));
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
    public void testPutIfAbsent() {
        String key = "130";
        String v1 = "v" + key;

        container.putIfAbsent(key, v1);
        assertEquals(v1, container.get(key));

        container.putIfAbsent(key, "v131");
        assertEquals(v1, container.get(key));
    }

    @Test
    public void testSize() {
        assertEquals(0, container.count());

        container.put("140", "v140");
        assertEquals(1, container.count());

        container.invalidate("140");
        assertEquals(0, container.count());

        container.putIfAbsent("141", "v141");
        assertEquals(1, container.count());

        container.putIfAbsent("142", "v142");
        assertEquals(2, container.count());

        container.invalidateAll();
        assertEquals(0, container.count());
    }

    @Test
    public void testInvalidateAll() {
        String k1 = "150", k2 = "151";
        String v1 = "v" + k1, v2 = "v" + k2;

        container.put(k1, "v" + k1);
        container.put(k2, "v" + k2);
        assertEquals(v1, container.get(k1));
        assertEquals(v2, container.get(k2));

        container.invalidateAll();
        assertNull(container.get(k1));
        assertNull(container.get(k2));
    }

    private static String forBucket(String s, OffHeapContainer container) {
        long offset = container.bucketMemory.offset(MARSHALL.marshall().apply(s));
        int attemptsLeft = 1_000_000;
        String dummy;
        do {
            dummy = generateRandomString();
            attemptsLeft--;
            if (attemptsLeft < 0) {
                throw new IllegalStateException(
                        "Could not find any key that hashes to same bucket as '" + s + "'");
            }
        } while (offset != container.bucketMemory.offset(MARSHALL.marshall().apply(dummy)));

        return dummy;
    }

    private static String generateRandomString() {
        Random r = new Random(System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        int numberOfChars = 3;
        for (int i = 0; i < numberOfChars; i++)
            sb.append((char) (64 + r.nextInt(26)));

        return sb.toString();
    }

}
