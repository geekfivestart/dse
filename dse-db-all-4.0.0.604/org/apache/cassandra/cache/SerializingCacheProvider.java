package org.apache.cassandra.cache;

import java.io.IOException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SerializingCacheProvider implements CacheProvider<RowCacheKey, IRowCacheEntry> {
   public SerializingCacheProvider() {
   }

   public ICache<RowCacheKey, IRowCacheEntry> create() {
      return SerializingCache.create(DatabaseDescriptor.getRowCacheSizeInMB() * 1024L * 1024L, new SerializingCacheProvider.RowCacheSerializer());
   }

   public static class RowCacheSerializer implements ISerializer<IRowCacheEntry> {
      public RowCacheSerializer() {
      }

      public void serialize(IRowCacheEntry entry, DataOutputPlus out) throws IOException {
         assert entry != null;

         boolean isSentinel = entry instanceof RowCacheSentinel;
         out.writeBoolean(isSentinel);
         if(isSentinel) {
            out.writeLong(((RowCacheSentinel)entry).sentinelId);
         } else {
            CachedPartition.cacheSerializer.serialize((CachedPartition)entry, out);
         }

      }

      public IRowCacheEntry deserialize(DataInputPlus in) throws IOException {
         boolean isSentinel = in.readBoolean();
         return (IRowCacheEntry)(isSentinel?new RowCacheSentinel(in.readLong()):(IRowCacheEntry)CachedPartition.cacheSerializer.deserialize(in));
      }

      public long serializedSize(IRowCacheEntry entry) {
         int size = TypeSizes.sizeof(true);
         if(entry instanceof RowCacheSentinel) {
            size += TypeSizes.sizeof(((RowCacheSentinel)entry).sentinelId);
         } else {
            size = (int)((long)size + CachedPartition.cacheSerializer.serializedSize((CachedPartition)entry));
         }

         return (long)size;
      }
   }
}
