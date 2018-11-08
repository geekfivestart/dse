package org.apache.cassandra.cache;

import java.util.Iterator;

public interface ICache<K, V> extends CacheSize {
   void put(K var1, V var2);

   boolean putIfAbsent(K var1, V var2);

   boolean replace(K var1, V var2, V var3);

   V get(K var1);

   void remove(K var1);

   void clear();

   Iterator<K> keyIterator();

   Iterator<K> hotKeyIterator(int var1);

   boolean containsKey(K var1);
}
