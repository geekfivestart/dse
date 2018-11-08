package org.apache.cassandra.service;

import java.util.concurrent.ExecutionException;

public interface CacheServiceMBean {
   int getRowCacheSavePeriodInSeconds();

   void setRowCacheSavePeriodInSeconds(int var1);

   int getKeyCacheSavePeriodInSeconds();

   void setKeyCacheSavePeriodInSeconds(int var1);

   int getCounterCacheSavePeriodInSeconds();

   void setCounterCacheSavePeriodInSeconds(int var1);

   int getRowCacheKeysToSave();

   void setRowCacheKeysToSave(int var1);

   int getKeyCacheKeysToSave();

   void setKeyCacheKeysToSave(int var1);

   int getCounterCacheKeysToSave();

   void setCounterCacheKeysToSave(int var1);

   void invalidateKeyCache();

   void invalidateRowCache();

   void invalidateCounterCache();

   void setRowCacheCapacityInMB(long var1);

   void setKeyCacheCapacityInMB(long var1);

   void setCounterCacheCapacityInMB(long var1);

   void saveCaches() throws ExecutionException, InterruptedException;
}
