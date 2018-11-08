package org.apache.cassandra.index.internal;

import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.utils.UUIDGen;

public class CollatedViewIndexBuilder extends SecondaryIndexBuilder {
   private final ColumnFamilyStore cfs;
   private final Set<Index> indexers;
   private final KeyIterator iter;
   private final UUID compactionId;

   public CollatedViewIndexBuilder(ColumnFamilyStore cfs, Set<Index> indexers, KeyIterator iter) {
      this.cfs = cfs;
      this.indexers = indexers;
      this.iter = iter;
      this.compactionId = UUIDGen.getTimeUUID();
   }

   public CompactionInfo getCompactionInfo() {
      return new CompactionInfo(this.cfs.metadata(), OperationType.INDEX_BUILD, this.iter.getBytesRead(), this.iter.getTotalBytes(), this.compactionId);
   }

   public void build() {
      try {
         PageSize pageSize = this.cfs.indexManager.calculateIndexingPageSize();

         while(this.iter.hasNext()) {
            if(this.isStopRequested()) {
               throw new CompactionInterruptedException(this.getCompactionInfo());
            }

            DecoratedKey key = (DecoratedKey)this.iter.next();
            this.cfs.indexManager.indexPartition(key, this.indexers, pageSize);
         }
      } finally {
         this.iter.close();
      }

   }
}
