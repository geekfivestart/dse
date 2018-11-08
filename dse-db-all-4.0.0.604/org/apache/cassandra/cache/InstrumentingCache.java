package org.apache.cassandra.cache;

import java.util.Iterator;
import org.apache.cassandra.metrics.CacheMetrics;

public class InstrumentingCache<K, V> {
   private final ICache<K, V> map;
   private final String type;
   private CacheMetrics metrics;

   public InstrumentingCache(String type, ICache<K, V> map) {
      this.map = map;
      this.type = type;
      this.metrics = new CacheMetrics(type, map);
   }

   public void put(K key, V value) {
      this.map.put(key, value);
   }

   public boolean putIfAbsent(K key, V value) {
      return this.map.putIfAbsent(key, value);
   }

   public boolean replace(K key, V old, V value) {
      return this.map.replace(key, old, value);
   }

   public V get(K key) {
      V v = this.map.get(key);
      this.metrics.requests.mark();
      if(v != null) {
         this.metrics.hits.mark();
      }

      return v;
   }

   public V getInternal(K key) {
      return this.map.get(key);
   }

   public void remove(K key) {
      this.map.remove(key);
   }

   public long getCapacity() {
      return this.map.capacity();
   }

   public void setCapacity(long capacity) {
      this.map.setCapacity(capacity);
   }

   public int size() {
      return this.map.size();
   }

   public long weightedSize() {
      return this.map.weightedSize();
   }

   public void clear() {
      this.map.clear();
      this.metrics = new CacheMetrics(this.type, this.map);
   }

   public Iterator<K> keyIterator() {
      return this.map.keyIterator();
   }

   public Iterator<K> hotKeyIterator(int n) {
      return this.map.hotKeyIterator(n);
   }

   public boolean containsKey(K key) {
      return this.map.containsKey(key);
   }

   public CacheMetrics getMetrics() {
      return this.metrics;
   }
}
