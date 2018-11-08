package org.apache.cassandra.utils;

public interface IndexedSearchIterator<K, V> extends SearchIterator<K, V> {
   boolean hasNext();

   V current();

   int indexOfCurrent();
}
