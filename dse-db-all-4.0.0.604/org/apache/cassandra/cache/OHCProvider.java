package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.schema.TableId;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public class OHCProvider implements CacheProvider<RowCacheKey, IRowCacheEntry> {
   public OHCProvider() {
   }

   public ICache<RowCacheKey, IRowCacheEntry> create() {
      OHCacheBuilder<RowCacheKey, IRowCacheEntry> builder = OHCacheBuilder.newBuilder();
      builder.capacity(DatabaseDescriptor.getRowCacheSizeInMB() * 1024L * 1024L).keySerializer(OHCProvider.KeySerializer.instance).valueSerializer(OHCProvider.ValueSerializer.instance).throwOOME(true);
      return new OHCProvider.OHCacheAdapter(builder.build());
   }

   private static class ValueSerializer implements CacheSerializer<IRowCacheEntry> {
      private static OHCProvider.ValueSerializer instance = new OHCProvider.ValueSerializer();

      private ValueSerializer() {
      }

      public void serialize(IRowCacheEntry entry, ByteBuffer buf) {
         assert entry != null;

         try {
            DataOutputBufferFixed out = new DataOutputBufferFixed(buf);
            Throwable var4 = null;

            try {
               boolean isSentinel = entry instanceof RowCacheSentinel;
               out.writeBoolean(isSentinel);
               if(isSentinel) {
                  out.writeLong(((RowCacheSentinel)entry).sentinelId);
               } else {
                  CachedPartition.cacheSerializer.serialize((CachedPartition)entry, out);
               }
            } catch (Throwable var14) {
               var4 = var14;
               throw var14;
            } finally {
               if(out != null) {
                  if(var4 != null) {
                     try {
                        out.close();
                     } catch (Throwable var13) {
                        var4.addSuppressed(var13);
                     }
                  } else {
                     out.close();
                  }
               }

            }

         } catch (IOException var16) {
            throw new RuntimeException(var16);
         }
      }

      public IRowCacheEntry deserialize(ByteBuffer buf) {
         try {
            RebufferingInputStream in = new DataInputBuffer(buf, false);
            boolean isSentinel = in.readBoolean();
            return (IRowCacheEntry)(isSentinel?new RowCacheSentinel(in.readLong()):(IRowCacheEntry)CachedPartition.cacheSerializer.deserialize(in));
         } catch (IOException var4) {
            throw new RuntimeException(var4);
         }
      }

      public int serializedSize(IRowCacheEntry entry) {
         int size = TypeSizes.sizeof(true);
         if(entry instanceof RowCacheSentinel) {
            size += TypeSizes.sizeof(((RowCacheSentinel)entry).sentinelId);
         } else {
            size = (int)((long)size + CachedPartition.cacheSerializer.serializedSize((CachedPartition)entry));
         }

         return size;
      }
   }

   private static class KeySerializer implements CacheSerializer<RowCacheKey> {
      private static OHCProvider.KeySerializer instance = new OHCProvider.KeySerializer();

      private KeySerializer() {
      }

      public void serialize(RowCacheKey rowCacheKey, ByteBuffer buf) {
         try {
            DataOutputBuffer dataOutput = new DataOutputBufferFixed(buf);
            Throwable var4 = null;

            try {
               rowCacheKey.tableId.serialize(dataOutput);
               dataOutput.writeUTF(rowCacheKey.indexName != null?rowCacheKey.indexName:"");
            } catch (Throwable var14) {
               var4 = var14;
               throw var14;
            } finally {
               if(dataOutput != null) {
                  if(var4 != null) {
                     try {
                        dataOutput.close();
                     } catch (Throwable var13) {
                        var4.addSuppressed(var13);
                     }
                  } else {
                     dataOutput.close();
                  }
               }

            }
         } catch (IOException var16) {
            throw new RuntimeException(var16);
         }

         buf.putInt(rowCacheKey.key.length);
         buf.put(rowCacheKey.key);
      }

      public RowCacheKey deserialize(ByteBuffer buf) {
         TableId tableId = null;
         String indexName = null;

         try {
            DataInputBuffer dataInput = new DataInputBuffer(buf, false);
            Throwable var5 = null;

            try {
               tableId = TableId.deserialize(dataInput);
               indexName = dataInput.readUTF();
               if(indexName.isEmpty()) {
                  indexName = null;
               }
            } catch (Throwable var15) {
               var5 = var15;
               throw var15;
            } finally {
               if(dataInput != null) {
                  if(var5 != null) {
                     try {
                        dataInput.close();
                     } catch (Throwable var14) {
                        var5.addSuppressed(var14);
                     }
                  } else {
                     dataInput.close();
                  }
               }

            }
         } catch (IOException var17) {
            throw new RuntimeException(var17);
         }

         byte[] key = new byte[buf.getInt()];
         buf.get(key);
         return new RowCacheKey(tableId, indexName, key);
      }

      public int serializedSize(RowCacheKey rowCacheKey) {
         return rowCacheKey.tableId.serializedSize() + TypeSizes.sizeof(rowCacheKey.indexName != null?rowCacheKey.indexName:"") + 4 + rowCacheKey.key.length;
      }
   }

   private static class OHCacheAdapter implements ICache<RowCacheKey, IRowCacheEntry> {
      private final OHCache<RowCacheKey, IRowCacheEntry> ohCache;

      public OHCacheAdapter(OHCache<RowCacheKey, IRowCacheEntry> ohCache) {
         this.ohCache = ohCache;
      }

      public long capacity() {
         return this.ohCache.capacity();
      }

      public void setCapacity(long capacity) {
         this.ohCache.setCapacity(capacity);
      }

      public void put(RowCacheKey key, IRowCacheEntry value) {
         this.ohCache.put(key, value);
      }

      public boolean putIfAbsent(RowCacheKey key, IRowCacheEntry value) {
         return this.ohCache.putIfAbsent(key, value);
      }

      public boolean replace(RowCacheKey key, IRowCacheEntry old, IRowCacheEntry value) {
         return this.ohCache.addOrReplace(key, old, value);
      }

      public IRowCacheEntry get(RowCacheKey key) {
         return (IRowCacheEntry)this.ohCache.get(key);
      }

      public void remove(RowCacheKey key) {
         this.ohCache.remove(key);
      }

      public int size() {
         return (int)this.ohCache.size();
      }

      public long weightedSize() {
         return this.ohCache.memUsed();
      }

      public void clear() {
         this.ohCache.clear();
      }

      public Iterator<RowCacheKey> hotKeyIterator(int n) {
         return this.ohCache.hotKeyIterator(n);
      }

      public Iterator<RowCacheKey> keyIterator() {
         return this.ohCache.keyIterator();
      }

      public boolean containsKey(RowCacheKey key) {
         return this.ohCache.containsKey(key);
      }
   }
}
