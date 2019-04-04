package org.infinispan.quarkus.hibernate.cache;

import org.hibernate.cache.cfg.spi.*;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.ExtendedStatisticsSupport;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.stat.CacheRegionStatistics;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class DomainDataRegionImpl implements DomainDataRegion, ExtendedStatisticsSupport {

    private static final Logger log = Logger.getLogger(DomainDataRegionImpl.class);

    private final InternalCache cache;
    private final DomainDataRegionConfig config;
    private final CacheKeysFactory cacheKeysFactory;
    private final RegionFactory regionFactory;
    private final InternalRegion internalRegion;

    @Override
    public long getElementCountInMemory() {
        return cache.count();
    }

    @Override
    public long getElementCountOnDisk() {
        return CacheRegionStatistics.NO_EXTENDED_STAT_SUPPORT_RETURN;
    }

    @Override
    public long getSizeInMemory() {
        return CacheRegionStatistics.NO_EXTENDED_STAT_SUPPORT_RETURN;
    }

    DomainDataRegionImpl(InternalCache cache, DomainDataRegionConfig config, CacheKeysFactory cacheKeysFactory, RegionFactory regionFactory) {
        this.cache = cache;
        this.config = config;
        this.cacheKeysFactory = cacheKeysFactory;
        this.regionFactory = regionFactory;
        this.internalRegion = new InternalRegionImpl(this);
    }

    CacheKeysFactory getCacheKeysFactory() {
        return cacheKeysFactory;
    }

    @Override
    public EntityDataAccess getEntityDataAccess(NavigableRole rootEntityRole) {
        EntityDataCachingConfig entityConfig = findConfig(config.getEntityCaching(), rootEntityRole);
        AccessType accessType = entityConfig.getAccessType();

        InternalDataAccess internal = createInternalDataAccess();

        if (accessType == AccessType.READ_ONLY || !entityConfig.isMutable())
            return new ReadOnlyEntityDataAccess(internal, this);

        return new ReadWriteEntityDataAccess(internal, this);
    }

    private <T extends DomainDataCachingConfig> T findConfig(List<T> configs, NavigableRole role) {
        return configs.stream()
                .filter(c -> c.getNavigableRole().equals(role))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Cannot find configuration for " + role));
    }

    private synchronized InternalDataAccess createInternalDataAccess() {
        return new InternalDataAccessImpl(cache, internalRegion);
    }

    @Override
    public NaturalIdDataAccess getNaturalIdDataAccess(NavigableRole rootEntityRole) {
        NaturalIdDataCachingConfig naturalIdConfig = findConfig(this.config.getNaturalIdCaching(), rootEntityRole);
        AccessType accessType = naturalIdConfig.getAccessType();
        if (accessType == AccessType.NONSTRICT_READ_WRITE) {
            // We don't support nonstrict read write for natural ids as NSRW requires versions;
            // natural ids aren't versioned by definition (as the values are primary keys).
            accessType = AccessType.READ_WRITE;
        }

        InternalDataAccess internal = createInternalDataAccess();
        if (accessType == AccessType.READ_ONLY || !naturalIdConfig.isMutable()) {
            return new ReadOnlyNaturalDataAccess(internal, this);
        } else {
            return new ReadWriteNaturalDataAccess(internal, this);
        }
    }

    @Override
    public CollectionDataAccess getCollectionDataAccess(NavigableRole collectionRole) {
        CollectionDataCachingConfig collectionConfig = findConfig(this.config.getCollectionCaching(), collectionRole);
        AccessType accessType = collectionConfig.getAccessType();
        InternalDataAccess accessDelegate = createInternalDataAccess();
        // No update/afterUpdate in CollectionDataAccess so a single impl works for all access types
        return new CollectionDataAccessImpl(accessType, accessDelegate, this);
    }

    @Override
    public String getName() {
        return config.getRegionName();
    }

    @Override
    public RegionFactory getRegionFactory() {
        return regionFactory;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void clear() {
        internalRegion.beginInvalidation();
        runInvalidation();
        internalRegion.endInvalidation();
    }

    private void runInvalidation() {
        cache.invalidateAll();
    }

}
