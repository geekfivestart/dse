package org.apache.cassandra.auth;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Single;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.utils.flow.RxThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthCache<K, V> implements AuthCacheMBean {
   private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);
   private static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";
   private static final String RECORD_CACHE_STATS = "dse.authCache.recordStats";
   private volatile AsyncLoadingCache<K, V> cache;
   private final String name;
   private final IntConsumer setValidityDelegate;
   private final IntSupplier getValidityDelegate;
   private final IntConsumer setUpdateIntervalDelegate;
   private final IntSupplier getUpdateIntervalDelegate;
   private final IntConsumer setMaxEntriesDelegate;
   private final IntSupplier getMaxEntriesDelegate;
   private final IntConsumer setInitialCapacityDelegate;
   private final IntSupplier getInitialCapacityDelegate;
   private final Function<K, V> loadFunction;
   private final Function<Iterable<? extends K>, Map<K, V>> loadAllFunction;
   private final BooleanSupplier enableCache;

   protected AuthCache(String name, IntConsumer setValidityDelegate, IntSupplier getValidityDelegate, IntConsumer setUpdateIntervalDelegate, IntSupplier getUpdateIntervalDelegate, IntConsumer setMaxEntriesDelegate, IntSupplier getMaxEntriesDelegate, IntConsumer setInitialCapacityDelegate, IntSupplier getInitialCapacityDelegate, Function<K, V> loadFunction, Function<Iterable<? extends K>, Map<K, V>> loadAllFunction, BooleanSupplier enableCache) {
      this.name = name;
      this.setValidityDelegate = setValidityDelegate;
      this.getValidityDelegate = getValidityDelegate;
      this.setUpdateIntervalDelegate = setUpdateIntervalDelegate;
      this.getUpdateIntervalDelegate = getUpdateIntervalDelegate;
      this.setMaxEntriesDelegate = setMaxEntriesDelegate;
      this.getMaxEntriesDelegate = getMaxEntriesDelegate;
      this.setInitialCapacityDelegate = setInitialCapacityDelegate;
      this.getInitialCapacityDelegate = getInitialCapacityDelegate;
      this.loadFunction = loadFunction;
      this.loadAllFunction = loadAllFunction;
      this.enableCache = enableCache;
      this.init();
   }

   protected void init() {
      this.cache = this.initCache((AsyncLoadingCache)null);

      try {
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
         ObjectName objectName = this.getObjectName();

         try {
            mbs.unregisterMBean(objectName);
         } catch (InstanceNotFoundException var4) {
            ;
         }

         mbs.registerMBean(this, objectName);
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }
   }

   protected ObjectName getObjectName() throws MalformedObjectNameException {
      return new ObjectName("org.apache.cassandra.auth:type=" + this.name);
   }

   protected Single<V> get(K k) {
      Supplier<CompletableFuture<V>> cf = this.cache != null?() -> {
         return this.cache.get(k);
      }:() -> {
         return CompletableFuture.supplyAsync(() -> {
            return this.loadFunction.apply(k);
         }, TPC.ioScheduler().forTaskType(TPCTaskType.AUTHORIZATION));
      };
      return RxThreads.singleFromCompletableFuture(cf);
   }

   @Nullable
   @VisibleForTesting
   protected V getIfPresent(K k) {
      return this.cache == null?null:this.cache.synchronous().getIfPresent(k);
   }

   protected Map<K, V> getAllPresent(Collection<K> keys) {
      return this.cache == null?Collections.emptyMap():this.cache.synchronous().getAllPresent(keys);
   }

   public Single<Map<K, V>> getAll(Collection<K> keys) {
      Supplier<CompletableFuture<Map<K, V>>> cf = this.cache != null?() -> {
         return this.cache.getAll(keys);
      }:() -> {
         return CompletableFuture.supplyAsync(() -> {
            Map<K, V> map = new HashMap(keys.size());
            keys.forEach((key) -> {
               map.put(key, this.loadFunction.apply(key));
            });
            return map;
         }, TPC.ioScheduler().forTaskType(TPCTaskType.AUTHORIZATION));
      };
      return RxThreads.singleFromCompletableFuture(cf);
   }

   public void invalidate() {
      this.cache = this.initCache((AsyncLoadingCache)null);
   }

   public void invalidateAll() {
      if(this.cache != null) {
         this.cache.synchronous().invalidateAll();
      }

   }

   public void invalidate(K k) {
      if(this.cache != null) {
         this.cache.synchronous().invalidate(k);
      }

   }

   public void setValidity(int validityPeriod) {
      if(Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration")) {
         throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");
      } else {
         this.setValidityDelegate.accept(validityPeriod);
         this.cache = this.initCache(this.cache);
      }
   }

   public int getValidity() {
      return this.getValidityDelegate.getAsInt();
   }

   public void setUpdateInterval(int updateInterval) {
      if(Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration")) {
         throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");
      } else {
         this.setUpdateIntervalDelegate.accept(updateInterval);
         this.cache = this.initCache(this.cache);
      }
   }

   public int getUpdateInterval() {
      return this.getUpdateIntervalDelegate.getAsInt();
   }

   public void setMaxEntries(int maxEntries) {
      if(Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration")) {
         throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");
      } else {
         this.setMaxEntriesDelegate.accept(maxEntries);
         this.cache = this.initCache(this.cache);
      }
   }

   public int getMaxEntries() {
      return this.getMaxEntriesDelegate.getAsInt();
   }

   public void setInitialCapacity(int initialCapacity) {
      if(Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration")) {
         throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");
      } else {
         this.setInitialCapacityDelegate.accept(initialCapacity);
         this.cache = this.initCache(this.cache);
      }
   }

   public int getInitialCapacity() {
      return this.getInitialCapacityDelegate.getAsInt();
   }

   public Map<String, Number> getCacheStats() {
      Map<String, Number> r = new HashMap();
      CacheStats stats = this.cache.synchronous().stats();
      r.put("averageLoadPenalty", Double.valueOf(stats.averageLoadPenalty()));
      r.put("evictionCount", Long.valueOf(stats.evictionCount()));
      r.put("evictionWeight", Long.valueOf(stats.evictionWeight()));
      r.put("hitCount", Long.valueOf(stats.hitCount()));
      r.put("hitRate", Double.valueOf(stats.hitRate()));
      r.put("loadCount", Long.valueOf(stats.loadCount()));
      r.put("loadFailureCount", Long.valueOf(stats.loadFailureCount()));
      r.put("loadFailureRate", Double.valueOf(stats.loadFailureRate()));
      r.put("loadSuccessCount", Long.valueOf(stats.loadSuccessCount()));
      r.put("missCount", Long.valueOf(stats.missCount()));
      r.put("missRate", Double.valueOf(stats.missRate()));
      r.put("requestCount", Long.valueOf(stats.requestCount()));
      r.put("totalLoadTime", Long.valueOf(stats.totalLoadTime()));
      return r;
   }

   private AsyncLoadingCache<K, V> initCache(AsyncLoadingCache<K, V> existing) {
      if(!this.enableCache.getAsBoolean()) {
         logger.debug("{} cache is not enabled", this.name);
         return null;
      } else if(this.getValidity() <= 0) {
         logger.warn("{} cache is configured with a validity of {} ms, disabling cache - this will result in a performance degration!", this.name, Integer.valueOf(this.getValidity()));
         return null;
      } else {
         if(this.getValidity() <= '\uea60') {
            logger.info("{} cache is configured with a validity of less than 60 seconds ({} ms). This can result in a performance degration. Consider checking the settings for the auth caches.", this.name, Integer.valueOf(this.getValidity()));
         }

         logger.info("(Re)initializing {} (validity period/update interval/max entries/initial capacity) ({}/{}/{}/{})", new Object[]{this.name, Integer.valueOf(this.getValidity()), Integer.valueOf(this.getUpdateInterval()), Integer.valueOf(this.getMaxEntries()), Integer.valueOf(this.getInitialCapacity())});
         if(existing == null) {
            Caffeine builder = Caffeine.newBuilder().refreshAfterWrite((long)this.getUpdateInterval(), TimeUnit.MILLISECONDS).expireAfterWrite((long)this.getValidity(), TimeUnit.MILLISECONDS).initialCapacity(this.getInitialCapacity()).maximumSize((long)this.getMaxEntries()).executor(TPC.ioScheduler().forTaskType(TPCTaskType.AUTHORIZATION));
            if(Boolean.getBoolean("dse.authCache.recordStats")) {
               builder = builder.recordStats();
            }

            return builder.buildAsync(new AuthCache.AsyncLoader());
         } else {
            LoadingCache<K, V> sync = this.cache.synchronous();
            sync.policy().refreshAfterWrite().ifPresent((policy) -> {
               policy.setExpiresAfter((long)this.getUpdateInterval(), TimeUnit.MILLISECONDS);
            });
            sync.policy().expireAfterWrite().ifPresent((policy) -> {
               policy.setExpiresAfter((long)this.getValidity(), TimeUnit.MILLISECONDS);
            });
            sync.policy().eviction().ifPresent((policy) -> {
               policy.setMaximum((long)this.getMaxEntries());
            });
            return this.cache;
         }
      }
   }

   private class AsyncLoader implements AsyncCacheLoader<K, V> {
      private AsyncLoader() {
      }

      @Nonnull
      public CompletableFuture<V> asyncLoad(@Nonnull K key, @Nonnull Executor executor) {
         return CompletableFuture.supplyAsync(() -> {
            return AuthCache.this.loadFunction.apply(key);
         }, executor);
      }

      @Nonnull
      public CompletableFuture<Map<K, V>> asyncLoadAll(@Nonnull Iterable<? extends K> keys, @Nonnull Executor executor) {
         return CompletableFuture.supplyAsync(() -> {
            return (Map)AuthCache.this.loadAllFunction.apply(keys);
         }, executor);
      }
   }
}
