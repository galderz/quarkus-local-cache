package org.infinispan.quarkus.hibernate.cache.offheap;

import org.infinispan.quarkus.hibernate.cache.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OffHeapMaxSizeTest {

    private static final Marshalling MARSHALL = Marshalling.UTF8;

    @Test
    public void testEvictWhenMaxSizeReached() {
        // Enough size to put 3 entries, each rounded up to 64
        int maxSize = (1 << 20) * 8 + 208;
        int itemCount = 3;
        OffHeapContainer container = new OffHeapContainer(maxSize, MARSHALL, null, Time.forever());
        assertEquals(0, container.count());

        IntStream.range(0, itemCount)
                .forEach(i -> container.put("10" + i, "v10" + i));

        assertEquals(itemCount, container.count());

        IntStream.range(0, itemCount)
                .forEach(i -> assertEquals("v10" + i, container.get("10" + i)));

        // Add one extra to make it go over the size
        String spilloverKey = "10" + (itemCount + 1);
        String spilloverValue = "v10" + (itemCount + 1);
        container.put(spilloverKey, spilloverValue);
        assertEquals(spilloverValue, container.get(spilloverKey));
        assertEquals(itemCount, container.count());

        // Item stored first is gone
        assertNull(container.get("100"));

        container.stop();
    }

    @Test
    public void testEvictWhenMaxSizeExceeded() {
        // Only enough to put 2 entries, 3 * 64 = 208
        int maxSize = (1 << 20) * 8 + 207;

        int itemCount = 3;
        OffHeapContainer container = new OffHeapContainer(maxSize, MARSHALL, null, Time.forever());
        assertEquals(0, container.count());

        IntStream.range(0, itemCount)
                .forEach(i -> container.putIfAbsent("11" + i, "v11" + i));

        assertEquals(itemCount - 1, container.count());
        assertNull(container.get("110"));

        container.stop();
    }

    @Test
    public void testEvictLeastRecentlyRead() {
        // Enough size to put 3 entries, each rounded up to 64
        int maxSize = (1 << 20) * 8 + 208;
        int itemCount = 3;
        OffHeapContainer container = new OffHeapContainer(maxSize, MARSHALL, null, Time.forever());
        assertEquals(0, container.count());

        IntStream.range(0, itemCount)
                .forEach(i -> container.put("12" + i, "v12" + i));

        assertEquals(itemCount, container.count());

        IntStream.range(0, itemCount)
                .boxed().sorted(Collections.reverseOrder())
                .forEach(i -> assertEquals("v12" + i, container.get("12" + i)));

        String spilloverKey = "12" + (itemCount + 1);
        String spilloverValue = "v12" + (itemCount + 1);
        container.put(spilloverKey, spilloverValue);
        assertEquals(spilloverValue, container.get(spilloverKey));
        assertEquals(itemCount, container.count());

        // Item stored last is gone
        assertNull(container.get("12" + (itemCount - 1)));

        container.stop();
    }

}
