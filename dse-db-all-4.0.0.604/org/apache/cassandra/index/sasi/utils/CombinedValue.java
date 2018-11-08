package org.apache.cassandra.index.sasi.utils;

public interface CombinedValue<V> extends Comparable<CombinedValue<V>> {
   void merge(CombinedValue<V> var1);

   V get();
}
