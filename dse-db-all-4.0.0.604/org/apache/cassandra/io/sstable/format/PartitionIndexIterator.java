package org.apache.cassandra.io.sstable.format;

import java.io.Closeable;
import java.io.IOException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.RowIndexEntry;

public interface PartitionIndexIterator extends Closeable {
   DecoratedKey key();

   RowIndexEntry entry() throws IOException;

   long dataPosition() throws IOException;

   void advance() throws IOException;

   void close();
}
