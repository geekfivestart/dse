package org.apache.cassandra.utils;

public interface SearchIterator<K, V> {
   V next(K var1);
}
