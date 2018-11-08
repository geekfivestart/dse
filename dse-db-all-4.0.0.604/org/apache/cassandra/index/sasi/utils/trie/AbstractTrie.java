package org.apache.cassandra.index.sasi.utils.trie;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;

abstract class AbstractTrie<K, V> extends AbstractMap<K, V> implements Serializable, Trie<K, V> {
   private static final long serialVersionUID = -6358111100045408883L;
   protected final KeyAnalyzer<? super K> keyAnalyzer;

   public AbstractTrie(KeyAnalyzer<? super K> keyAnalyzer) {
      this.keyAnalyzer = (KeyAnalyzer)Tries.notNull(keyAnalyzer, "keyAnalyzer");
   }

   public K selectKey(K key) {
      Entry<K, V> entry = this.select(key);
      return entry != null?entry.getKey():null;
   }

   public V selectValue(K key) {
      Entry<K, V> entry = this.select(key);
      return entry != null?entry.getValue():null;
   }

   public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("Trie[").append(this.size()).append("]={\n");
      Iterator var2 = this.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<K, V> entry = (Entry)var2.next();
         buffer.append("  ").append(entry).append("\n");
      }

      buffer.append("}\n");
      return buffer.toString();
   }

   final int lengthInBits(K key) {
      return key == null?0:this.keyAnalyzer.lengthInBits(key);
   }

   final boolean isBitSet(K key, int bitIndex) {
      return key != null && this.keyAnalyzer.isBitSet(key, bitIndex);
   }

   final int bitIndex(K key, K otherKey) {
      return key != null && otherKey != null?this.keyAnalyzer.bitIndex(key, otherKey):(key != null?this.bitIndex(key):(otherKey != null?this.bitIndex(otherKey):-1));
   }

   private int bitIndex(K key) {
      int lengthInBits = this.lengthInBits(key);

      for(int i = 0; i < lengthInBits; ++i) {
         if(this.isBitSet(key, i)) {
            return i;
         }
      }

      return -1;
   }

   final boolean compareKeys(K key, K other) {
      return key == null?other == null:(other == null?false:this.keyAnalyzer.compare(key, other) == 0);
   }

   abstract static class BasicEntry<K, V> implements Entry<K, V>, Serializable {
      private static final long serialVersionUID = -944364551314110330L;
      protected K key;
      protected V value;
      private transient int hashCode = 0;

      public BasicEntry(K key, V value) {
         this.key = key;
         this.value = value;
      }

      public V setKeyValue(K key, V value) {
         this.key = key;
         this.hashCode = 0;
         return this.setValue(value);
      }

      public K getKey() {
         return this.key;
      }

      public V getValue() {
         return this.value;
      }

      public V setValue(V value) {
         V previous = this.value;
         this.value = value;
         return previous;
      }

      public int hashCode() {
         if(this.hashCode == 0) {
            this.hashCode = this.key != null?this.key.hashCode():0;
         }

         return this.hashCode;
      }

      public boolean equals(Object o) {
         if(o == this) {
            return true;
         } else if(!(o instanceof Entry)) {
            return false;
         } else {
            Entry<?, ?> other = (Entry)o;
            return Tries.areEqual(this.key, other.getKey()) && Tries.areEqual(this.value, other.getValue());
         }
      }

      public String toString() {
         return this.key + "=" + this.value;
      }
   }
}
