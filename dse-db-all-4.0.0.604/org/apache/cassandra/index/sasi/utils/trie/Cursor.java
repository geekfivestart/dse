package org.apache.cassandra.index.sasi.utils.trie;

import java.util.Map.Entry;

public interface Cursor<K, V> {
   Cursor.Decision select(Entry<? extends K, ? extends V> var1);

   public static enum Decision {
      EXIT,
      CONTINUE,
      REMOVE,
      REMOVE_AND_EXIT;

      private Decision() {
      }
   }
}
