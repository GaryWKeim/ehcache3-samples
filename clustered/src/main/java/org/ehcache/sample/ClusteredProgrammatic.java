package org.ehcache.sample;

import static java.net.URI.create;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.slf4j.LoggerFactory.getLogger;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.slf4j.Logger;

public class ClusteredProgrammatic {
  private static final Logger LOGGER = getLogger(ClusteredProgrammatic.class);

  public static void main(String[] args) throws CachePersistenceException, InterruptedException {
    final List<String> argList = Arrays.asList(args);
    DefaultManagementRegistryConfiguration managementRegistryConfiguration = new DefaultManagementRegistryConfiguration()
        .setCacheManagerAlias("ClusteredCacheManager");
    DefaultManagementRegistryService managementRegistry = new DefaultManagementRegistryService(managementRegistryConfiguration);

    LOGGER.info("argList: " + argList);
    
    if (argList.contains("create")) {
      LOGGER.info("Creating clustered cache manager");
      final URI uri = create("terracotta://localhost:9410/clustered");

      try (CacheManager cacheManager = newCacheManagerBuilder()
              .with(cluster(uri).autoCreate().defaultServerResource("main"))
              .using(managementRegistry)
              .withCache("basicCache",
                      newCacheConfigurationBuilder(Long.class, String.class,
                              heap(100).offheap(1, MB).with(clusteredDedicated(5, MB))))
              .build(true)) {
        Thread.sleep(3000);
      }
    } else if (argList.contains("destroy")) {
      LOGGER.info("Destroying clustered cache manager");
      final URI uri = create("terracotta://localhost:9410/clustered");
      CacheManager cacheManager = newCacheManagerBuilder()
              .with(cluster(uri).autoCreate().defaultServerResource("main"))
              .using(managementRegistry)
              .withCache("basicCache",
                      newCacheConfigurationBuilder(Long.class, String.class,
                              heap(100).offheap(1, MB).with(clusteredDedicated(5, MB))))
              .build(true);
      Thread.sleep(3000);
      cacheManager.close();
      ((PersistentCacheManager)cacheManager).destroy();
    }
    else if (argList.contains("pound")) {
      LOGGER.info("Pounding clustered cache manager");
      final URI uri = create("terracotta://localhost:9410/clustered");
      CacheManager cacheManager = newCacheManagerBuilder()
              .with(cluster(uri).autoCreate().defaultServerResource("main"))
              .using(managementRegistry)
              .withCache("basicCache",
                      newCacheConfigurationBuilder(Long.class, String.class,
                              heap(100).offheap(1, MB).with(clusteredDedicated(5, MB))))
              .build(true);
      Cache<Long, String> cache = cacheManager.getCache("basicCache", Long.class, String.class);
      final Random random = new Random();
      final int KEY_SPACE = 50_000;
      while (true) {
        Long key = Long.valueOf(random.nextInt(KEY_SPACE));
        cache.put(key, UUID.randomUUID().toString());
        Thread.sleep(1000);
        cache.get(key);
        cache.get(Long.valueOf(random.nextInt(KEY_SPACE)));
      }
    }
  }
}
