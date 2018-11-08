package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.CloseableIterator;

public interface KeyIterator extends CloseableIterator<DecoratedKey> {
   long getTotalBytes();

   long getBytesRead();

   default void remove() {
      throw new UnsupportedOperationException();
   }
}
