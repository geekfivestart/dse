package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentBiMap<K, V> implements Map<K, V> {
   protected final Map<K, V> forwardMap;
   protected final Map<V, K> reverseMap;

   public ConcurrentBiMap() {
      this(new ConcurrentHashMap(16, 0.5F, 1), new ConcurrentHashMap(16, 0.5F, 1));
   }

   protected ConcurrentBiMap(Map<K, V> forwardMap, Map<V, K> reverseMap) {
      this.forwardMap = forwardMap;
      this.reverseMap = reverseMap;
   }

   public Map<V, K> inverse() {
      return Collections.unmodifiableMap(this.reverseMap);
   }

   public void clear() {
      this.forwardMap.clear();
      this.reverseMap.clear();
   }

   public boolean containsKey(Object key) {
      return this.forwardMap.containsKey(key);
   }

   public boolean containsValue(Object value) {
      return this.reverseMap.containsKey(value);
   }

   public Set<Entry<K, V>> entrySet() {
      return this.forwardMap.entrySet();
   }

   public V get(Object key) {
      return this.forwardMap.get(key);
   }

   public boolean isEmpty() {
      return this.forwardMap.isEmpty();
   }

   public Set<K> keySet() {
      return this.forwardMap.keySet();
   }

   public synchronized V put(K key, V value) {
      K oldKey = this.reverseMap.get(value);
      if(oldKey != null && !key.equals(oldKey)) {
         throw new IllegalArgumentException(value + " is already bound in reverseMap to " + oldKey);
      } else {
         V oldVal = this.forwardMap.put(key, value);
         if(oldVal != null && !Objects.equals(this.reverseMap.remove(oldVal), key)) {
            throw new IllegalStateException();
         } else {
            this.reverseMap.put(value, key);
            return oldVal;
         }
      }
   }

   public synchronized void putAll(Map<? extends K, ? extends V> m) {
      Iterator var2 = m.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<? extends K, ? extends V> entry = (Entry)var2.next();
         this.put(entry.getKey(), entry.getValue());
      }

   }

   public synchronized V remove(Object key) {
      V oldVal = this.forwardMap.remove(key);
      if(oldVal == null) {
         return null;
      } else {
         Object oldKey = this.reverseMap.remove(oldVal);
         if(oldKey != null && oldKey.equals(key)) {
            return oldVal;
         } else {
            throw new IllegalStateException();
         }
      }
   }

   public int size() {
      return this.forwardMap.size();
   }

   public Collection<V> values() {
      return this.reverseMap.keySet();
   }
}
