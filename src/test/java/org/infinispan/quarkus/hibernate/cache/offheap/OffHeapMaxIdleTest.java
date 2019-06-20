package org.infinispan.quarkus.hibernate.cache.offheap;

import org.infinispan.quarkus.hibernate.cache.ManualNanosService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OffHeapMaxIdleTest {

    private static final Marshalling MARSHALL = Marshalling.UTF8;

    private OffHeapContainer container;

    private ManualNanosService time = new ManualNanosService();

    @Before
    public void createContainer() {
        // Max size is at least number of bucket pointers, plus a little bit more
        int maxSize = (1 << 20) * 8 + 1024;
        container = new OffHeapContainer(maxSize, MARSHALL, time, Duration.ofSeconds(5));
    }

    @After
    public void destroyContainer() {
        container.stop();
    }

    @Test
    public void testMaxIdleOnGet() {
        final String key = "100";
        final String value = "v100";

        container.putIfAbsent(key, value);
        assertEquals(value, container.get(key));

        time.advance(10, TimeUnit.SECONDS);

        assertNull(container.get(key));
    }

    @Test
    public void testMaxIdleOnSize() {
        container.putIfAbsent("110", "v110");
        container.putIfAbsent("111", "v111");
        container.putIfAbsent("112", "v112");
        assertEquals(3, container.count());

        time.advance(10, TimeUnit.SECONDS);

        assertEquals(0, container.count());
    }

    @Test
    public void testInvalidateAll() {
        assertEquals(0, container.count());
        container.putIfAbsent("120", "v120");
        assertEquals(1, container.count());
        container.invalidateAll();
        assertEquals(0, container.count());
        container.putIfAbsent("121", "v121");
        assertEquals(1, container.count());
    }

    @Test
    public void testInvalidate() {
        assertEquals(0, container.count());
        final String key = "130";
        container.putIfAbsent(key, "v130");
        assertEquals(1, container.count());
        container.invalidate(key);
        assertEquals(0, container.count());
    }

}
