package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Unfiltered;

public interface SSTableFlushObserver {
   void begin();

   void startPartition(DecoratedKey var1, long var2);

   void nextUnfilteredCluster(Unfiltered var1);

   void complete();
}
