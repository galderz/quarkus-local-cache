package org.infinispan.quarkus.hibernate.cache;

import java.time.Duration;

final class InternalCacheConfig {

    long maxObjectCount;
    long maxMemorySize;
    Duration maxIdle;

}
