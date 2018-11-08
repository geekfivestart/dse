package org.apache.cassandra.utils.btree;

import java.util.Iterator;
import org.apache.cassandra.utils.IndexedSearchIterator;

public interface BTreeSearchIterator<K, V> extends IndexedSearchIterator<K, V>, Iterator<V> {
   void rewind();
}
