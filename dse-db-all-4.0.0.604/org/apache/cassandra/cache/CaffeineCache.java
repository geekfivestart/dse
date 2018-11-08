package org.apache.cassandra.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Iterator;
import java.util.function.Supplier;

public class CaffeineCache<K extends IMeasurableMemory, V extends IMeasurableMemory> implements ICache<K, V> {
   private final Cache<K, V> cache;
   private final Eviction<K, V> policy;

   private CaffeineCache(Cache<K, V> cache) {
      this.cache = cache;
      this.policy = (Eviction)cache.policy().eviction().orElseThrow(() -> {
         return new IllegalArgumentException("Expected a size bounded cache");
      });
      Preconditions.checkState(this.policy.isWeighted(), "Expected a weighted cache");
   }

   public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity, Weigher<K, V> weigher) {
      Cache<K, V> cache = Caffeine.newBuilder().maximumWeight(weightedCapacity).weigher(weigher).executor(MoreExecutors.directExecutor()).build();
      return new CaffeineCache(cache);
   }

   public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity) {
      return create(weightedCapacity, (key, value) -> {
         long size = key.unsharedHeapSize() + value.unsharedHeapSize();
         if(size > 2147483647L) {
            throw new IllegalArgumentException("Serialized size cannot be more than 2GB/Integer.MAX_VALUE");
         } else {
            return (int)size;
         }
      });
   }

   public long capacity() {
      return this.policy.getMaximum();
   }

   public void setCapacity(long capacity) {
      this.policy.setMaximum(capacity);
   }

   public boolean isEmpty() {
      return this.cache.asMap().isEmpty();
   }

   public int size() {
      return this.cache.asMap().size();
   }

   public long weightedSize() {
      return this.policy.weightedSize().getAsLong();
   }

   public void clear() {
      this.cache.invalidateAll();
   }

   public V get(K key) {
      return (IMeasurableMemory)this.cache.getIfPresent(key);
   }

   public void put(K key, V value) {
      this.cache.put(key, value);
   }

   public boolean putIfAbsent(K key, V value) {
      return this.cache.asMap().putIfAbsent(key, value) == null;
   }

   public boolean replace(K key, V old, V value) {
      return this.cache.asMap().replace(key, old, value);
   }

   public void remove(K key) {
      this.cache.invalidate(key);
   }

   public Iterator<K> keyIterator() {
      return this.cache.asMap().keySet().iterator();
   }

   public Iterator<K> hotKeyIterator(int n) {
      return this.policy.hottest(n).keySet().iterator();
   }

   public boolean containsKey(K key) {
      return this.cache.asMap().containsKey(key);
   }
}
