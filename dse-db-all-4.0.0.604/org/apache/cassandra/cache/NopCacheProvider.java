package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;

public class NopCacheProvider implements CacheProvider<RowCacheKey, IRowCacheEntry> {
   public NopCacheProvider() {
   }

   public ICache<RowCacheKey, IRowCacheEntry> create() {
      return new NopCacheProvider.NopCache();
   }

   private static class NopCache implements ICache<RowCacheKey, IRowCacheEntry> {
      private NopCache() {
      }

      public long capacity() {
         return 0L;
      }

      public void setCapacity(long capacity) {
      }

      public void put(RowCacheKey key, IRowCacheEntry value) {
      }

      public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value) {
         return false;
      }

      public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value) {
         return false;
      }

      public IRowCacheEntry get(RowCacheKey key) {
         return null;
      }

      public void remove(RowCacheKey key) {
      }

      public int size() {
         return 0;
      }

      public long weightedSize() {
         return 0L;
      }

      public void clear() {
      }

      public Iterator<RowCacheKey> hotKeyIterator(int n) {
         return Collections.emptyIterator();
      }

      public Iterator<RowCacheKey> keyIterator() {
         return Collections.emptyIterator();
      }

      public boolean containsKey(RowCacheKey key) {
         return false;
      }
   }
}
