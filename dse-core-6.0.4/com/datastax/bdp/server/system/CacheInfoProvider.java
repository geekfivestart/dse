package com.datastax.bdp.server.system;

import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.service.CacheService;

public interface CacheInfoProvider {
   long getEntries();

   long getSize();

   long getCapacity();

   long getHits();

   long getRequests();

   double getHitRate();

   public abstract static class AbstractProvider implements CacheInfoProvider {
      public AbstractProvider() {
      }

      abstract CacheMetrics getMetrics();

      public long getEntries() {
         return (long)((Integer)this.getMetrics().entries.getValue()).intValue();
      }

      public long getSize() {
         return ((Long)this.getMetrics().size.getValue()).longValue();
      }

      public long getCapacity() {
         return ((Long)this.getMetrics().capacity.getValue()).longValue();
      }

      public long getHits() {
         return this.getMetrics().hits.getCount();
      }

      public long getRequests() {
         return this.getMetrics().requests.getCount();
      }

      public double getHitRate() {
         return ((Double)this.getMetrics().hitRate.getValue()).doubleValue();
      }
   }

   public static class RowCacheInfoProvider extends CacheInfoProvider.AbstractProvider {
      public RowCacheInfoProvider() {
      }

      CacheMetrics getMetrics() {
         return CacheService.instance.rowCache.getMetrics();
      }
   }

   public static class KeyCacheInfoProvider extends CacheInfoProvider.AbstractProvider {
      public KeyCacheInfoProvider() {
      }

      CacheMetrics getMetrics() {
         return CacheService.instance.keyCache.getMetrics();
      }
   }
}
