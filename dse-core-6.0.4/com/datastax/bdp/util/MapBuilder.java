package com.datastax.bdp.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class MapBuilder {
   private MapBuilder() {
   }

   public static <K, V> MapBuilder.ImmutableBuilder<K, V> immutable() {
      return new MapBuilder.ImmutableBuilder();
   }

   public static <K, V> MapBuilder.MutableBuilder<K, V> mutable() {
      return new MapBuilder.MutableBuilder();
   }

   public static <K, V> void putIfNotNull(Map<K, V> map, K key, V value) {
      if(value != null) {
         map.put(key, value);
      }

   }

   public static class ImmutableMap<K, V> extends AbstractMap<K, V> {
      private Entry<K, V>[] entries;
      private K[] keys;
      private V[] values;

      private ImmutableMap(Entry<K, V>[] entries, K[] keys, V[] values) {
         this.entries = entries;
         this.keys = keys;
         this.values = values;
      }

      public Set<Entry<K, V>> entrySet() {
         return new AbstractSet<Entry<K, V>>() {
            public Iterator<Entry<K, V>> iterator() {
               return new AbstractIterator<Entry<K, V>>() {
                  private int index = 0;

                  protected Entry<K, V> computeNext() {
                     if(ImmutableMap.this.keys.length > this.index && ImmutableMap.this.values.length > this.index) {
                        final int current = this.index++;
                        if(ImmutableMap.this.entries[current] == null) {
                           ImmutableMap.this.entries[current] = new Entry<K, V>() {
                              public K getKey() {
                                 return ImmutableMap.this.keys[current];
                              }

                              public V getValue() {
                                 return ImmutableMap.this.values[current];
                              }

                              public V setValue(V value) {
                                 throw new UnsupportedOperationException();
                              }
                           };
                        }

                        return ImmutableMap.this.entries[current];
                     } else {
                        return (Entry)this.endOfData();
                     }
                  }
               };
            }

            public int size() {
               return ImmutableMap.this.keys.length;
            }
         };
      }
   }

   public static class MutableBuilder<K, V> implements MapBuilder.Builder<K, V> {
      private K[] keys;
      private V[] values;

      public MutableBuilder() {
      }

      public MapBuilder.MutableBuilder<K, V> withKeys(K... keys) {
         this.keys = keys;
         return this;
      }

      public MapBuilder.MutableBuilder<K, V> withValues(V... values) {
         this.values = values;
         return this;
      }

      public Map<K, V> build() {
         assert this.keys != null && this.values != null;

         HashMap<K, V> map = new HashMap();

         for(int i = 0; i < this.keys.length && i < this.values.length; ++i) {
            map.put(this.keys[i], this.values[i]);
         }

         return map;
      }
   }

   public static class ImmutableBuilder<K, V> implements MapBuilder.Builder<K, V> {
      private Entry<K, V>[] entries;
      private K[] keys;
      private V[] values;

      public ImmutableBuilder() {
      }

      public MapBuilder.ImmutableBuilder<K, V> withKeys(K... keys) {
         this.keys = keys;
         return this;
      }

      public MapBuilder.ImmutableBuilder<K, V> withValues(V... values) {
         this.values = values;
         return this;
      }

      public MapBuilder.ImmutableMap<K, V> build() {
         Preconditions.checkState(this.keys != null);
         Preconditions.checkState(this.values != null);
         this.entries = new Entry[this.keys.length];
         return new MapBuilder.ImmutableMap(this.entries, this.keys, this.values);
      }
   }

   public interface Builder<K, V> {
      Map<K, V> build();

      MapBuilder.Builder<K, V> withKeys(K... var1);

      MapBuilder.Builder<K, V> withValues(V... var1);
   }
}
