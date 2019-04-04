package org.infinispan.quarkus.hibernate.cache;

interface InternalRegion {

    boolean checkValid();

    void beginInvalidation();

    void endInvalidation();

    String getName();

    void clear();

}
