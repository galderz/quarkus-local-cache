package org.infinispan.quarkus.hibernate.cache.offheap;

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
