package org.infinispan.quarkus.hibernate.cache;

import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.quarkus.hibernate.cache.Eventually.eventually;
import static org.junit.Assert.assertEquals;

public class CacheRegionMaxSizeTest {

    @Test
    public void testMaxSize() {
        final Map configValues = new HashMap();
        configValues.put("hibernate.cache.com.acme.EntityC.memory.max-size", "207");

        CacheRegionTesting testing = CacheRegionTesting.cacheRegion("com.acme.EntityC", configValues);
        SharedSessionContractImplementor session = testing.session;

        EntityDataAccess cacheAccess = testing.entityCache(AccessType.READ_WRITE);
        putFromLoad(cacheAccess, session);
        assertEquals(2, testing.region().getElementCountInMemory());
        getCacheFound(session, cacheAccess);

        // Add another entity that makes the cache exceed its size
        cacheAccess.putFromLoad(session, "103", "v103", null, false);

        eventually(() -> assertEquals(2, testing.region().getElementCountInMemory()));
    }

    private static void getCacheFound(SharedSessionContractImplementor session, EntityDataAccess cacheAccess) {
        assertEquals("v100", cacheAccess.get(session, "100"));
        assertEquals("v101", cacheAccess.get(session, "101"));
    }

    private static void putFromLoad(EntityDataAccess cacheAccess, SharedSessionContractImplementor session) {
        cacheAccess.putFromLoad(session, "100", "v100", null, false);
        cacheAccess.putFromLoad(session, "101", "v101", null, false);
    }

}
