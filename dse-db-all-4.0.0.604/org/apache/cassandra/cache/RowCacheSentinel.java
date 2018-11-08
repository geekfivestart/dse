package org.apache.cassandra.cache;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class RowCacheSentinel implements IRowCacheEntry {
   private static final AtomicLong generator = new AtomicLong();
   final long sentinelId;

   public RowCacheSentinel() {
      this.sentinelId = generator.getAndIncrement();
   }

   RowCacheSentinel(long sentinelId) {
      this.sentinelId = sentinelId;
   }

   public boolean equals(Object o) {
      if(!(o instanceof RowCacheSentinel)) {
         return false;
      } else {
         RowCacheSentinel other = (RowCacheSentinel)o;
         return this.sentinelId == other.sentinelId;
      }
   }

   public int hashCode() {
      return Objects.hashCode(Long.valueOf(this.sentinelId));
   }
}
