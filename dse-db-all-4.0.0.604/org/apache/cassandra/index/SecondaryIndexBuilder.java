package org.apache.cassandra.index;

import org.apache.cassandra.db.compaction.CompactionInfo;

public abstract class SecondaryIndexBuilder extends CompactionInfo.Holder {
   public SecondaryIndexBuilder() {
   }

   public abstract void build();
}
