package org.apache.cassandra.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Iterator;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializingCache<K, V> implements ICache<K, V> {
   private static final Logger logger = LoggerFactory.getLogger(SerializingCache.class);
   private final Cache<K, RefCountedMemory> cache;
   private final ISerializer<V> serializer;

   private SerializingCache(long capacity, Weigher<K, RefCountedMemory> weigher, ISerializer<V> serializer) {
      this.serializer = serializer;
      this.cache = Caffeine.newBuilder().weigher(weigher).maximumWeight(capacity).executor(MoreExecutors.directExecutor()).removalListener((key, mem, cause) -> {
         if(cause.wasEvicted()) {
            mem.unreference();
         }

      }).build();
   }

   public static <K, V> SerializingCache<K, V> create(long weightedCapacity, Weigher<K, RefCountedMemory> weigher, ISerializer<V> serializer) {
      return new SerializingCache(weightedCapacity, weigher, serializer);
   }

   public static <K, V> SerializingCache<K, V> create(long weightedCapacity, ISerializer<V> serializer) {
      return create(weightedCapacity, (key, value) -> {
         long size = value.size();
         if(size > 2147483647L) {
            throw new IllegalArgumentException("Serialized size must not be more than 2GB");
         } else {
            return (int)size;
         }
      }, serializer);
   }

   private V deserialize(RefCountedMemory mem) {
      try {
         return this.serializer.deserialize(new MemoryInputStream(mem));
      } catch (IOException var3) {
         logger.trace("Cannot fetch in memory data, we will fallback to read from disk ", var3);
         return null;
      }
   }

   private RefCountedMemory serialize(V value) {
      long serializedSize = this.serializer.serializedSize(value);
      if(serializedSize > 2147483647L) {
         throw new IllegalArgumentException(String.format("Unable to allocate %s", new Object[]{FBUtilities.prettyPrintMemory(serializedSize)}));
      } else {
         RefCountedMemory freeableMemory;
         try {
            freeableMemory = new RefCountedMemory(serializedSize);
         } catch (OutOfMemoryError var7) {
            return null;
         }

         try {
            this.serializer.serialize(value, new WrappedDataOutputStreamPlus(new MemoryOutputStream(freeableMemory)));
            return freeableMemory;
         } catch (IOException var6) {
            freeableMemory.unreference();
            throw new RuntimeException(var6);
         }
      }
   }

   public long capacity() {
      return ((Eviction)this.cache.policy().eviction().get()).getMaximum();
   }

   public void setCapacity(long capacity) {
      ((Eviction)this.cache.policy().eviction().get()).setMaximum(capacity);
   }

   public boolean isEmpty() {
      return this.cache.asMap().isEmpty();
   }

   public int size() {
      return this.cache.asMap().size();
   }

   public long weightedSize() {
      return ((Eviction)this.cache.policy().eviction().get()).weightedSize().getAsLong();
   }

   public void clear() {
      this.cache.invalidateAll();
   }

   public V get(K key) {
      RefCountedMemory mem = (RefCountedMemory)this.cache.getIfPresent(key);
      if(mem == null) {
         return null;
      } else if(!mem.reference()) {
         return null;
      } else {
         Object var3;
         try {
            var3 = this.deserialize(mem);
         } finally {
            mem.unreference();
         }

         return var3;
      }
   }

   public void put(K key, V value) {
      RefCountedMemory mem = this.serialize(value);
      if(mem != null) {
         RefCountedMemory old;
         try {
            old = (RefCountedMemory)this.cache.asMap().put(key, mem);
         } catch (Throwable var6) {
            mem.unreference();
            throw var6;
         }

         if(old != null) {
            old.unreference();
         }

      }
   }

   public boolean putIfAbsent(K key, V value) {
      RefCountedMemory mem = this.serialize(value);
      if(mem == null) {
         return false;
      } else {
         RefCountedMemory old;
         try {
            old = (RefCountedMemory)this.cache.asMap().putIfAbsent(key, mem);
         } catch (Throwable var6) {
            mem.unreference();
            throw var6;
         }

         if(old != null) {
            mem.unreference();
         }

         return old == null;
      }
   }

   public boolean replace(K key, V oldToReplace, V value) {
      RefCountedMemory old = (RefCountedMemory)this.cache.getIfPresent(key);
      if(old == null) {
         return false;
      } else if(!old.reference()) {
         return false;
      } else {
         V oldValue = this.deserialize(old);
         old.unreference();
         if(!oldValue.equals(oldToReplace)) {
            return false;
         } else {
            RefCountedMemory mem = this.serialize(value);
            if(mem == null) {
               return false;
            } else {
               boolean success;
               try {
                  success = this.cache.asMap().replace(key, old, mem);
               } catch (Throwable var9) {
                  mem.unreference();
                  throw var9;
               }

               if(success) {
                  old.unreference();
               } else {
                  mem.unreference();
               }

               return success;
            }
         }
      }
   }

   public void remove(K key) {
      RefCountedMemory mem = (RefCountedMemory)this.cache.asMap().remove(key);
      if(mem != null) {
         mem.unreference();
      }

   }

   public Iterator<K> keyIterator() {
      return this.cache.asMap().keySet().iterator();
   }

   public Iterator<K> hotKeyIterator(int n) {
      return ((Eviction)this.cache.policy().eviction().get()).hottest(n).keySet().iterator();
   }

   public boolean containsKey(K key) {
      return this.cache.asMap().containsKey(key);
   }
}
