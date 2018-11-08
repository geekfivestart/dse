package org.apache.cassandra.utils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class BiMultiValMap<K, V> implements Map<K, V> {
   protected final Map<K, V> forwardMap;
   protected final Multimap<V, K> reverseMap;

   public BiMultiValMap() {
      this.forwardMap = new HashMap();
      this.reverseMap = HashMultimap.create();
   }

   protected BiMultiValMap(Map<K, V> forwardMap, Multimap<V, K> reverseMap) {
      this.forwardMap = forwardMap;
      this.reverseMap = reverseMap;
   }

   public BiMultiValMap(BiMultiValMap<K, V> map) {
      this();
      this.forwardMap.putAll(map);
      this.reverseMap.putAll(map.inverse());
   }

   public Multimap<V, K> inverse() {
      return Multimaps.unmodifiableMultimap(this.reverseMap);
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

   public V put(K key, V value) {
      V oldVal = this.forwardMap.put(key, value);
      if(oldVal != null) {
         this.reverseMap.remove(oldVal, key);
      }

      this.reverseMap.put(value, key);
      return oldVal;
   }

   public void putAll(Map<? extends K, ? extends V> m) {
      Iterator var2 = m.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<? extends K, ? extends V> entry = (Entry)var2.next();
         this.put(entry.getKey(), entry.getValue());
      }

   }

   public V remove(Object key) {
      V oldVal = this.forwardMap.remove(key);
      this.reverseMap.remove(oldVal, key);
      return oldVal;
   }

   public Collection<K> removeValue(V value) {
      Collection<K> keys = this.reverseMap.removeAll(value);
      Iterator var3 = keys.iterator();

      while(var3.hasNext()) {
         K key = var3.next();
         this.forwardMap.remove(key);
      }

      return keys;
   }

   public int size() {
      return this.forwardMap.size();
   }

   public Collection<V> values() {
      return this.reverseMap.keys();
   }

   public Collection<V> valueSet() {
      return this.reverseMap.keySet();
   }
}
