package org.apache.cassandra.index.sasi.utils.trie;

import java.util.SortedMap;
import java.util.Map.Entry;

public interface Trie<K, V> extends SortedMap<K, V> {
   Entry<K, V> select(K var1);

   K selectKey(K var1);

   V selectValue(K var1);

   Entry<K, V> select(K var1, Cursor<? super K, ? super V> var2);

   Entry<K, V> traverse(Cursor<? super K, ? super V> var1);

   SortedMap<K, V> prefixMap(K var1);
}
