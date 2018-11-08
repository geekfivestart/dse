package org.apache.cassandra.io.sstable;

import java.io.IOException;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry implements IMeasurableMemory {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0L));
   public final long position;

   public RowIndexEntry(long position) {
      this.position = position;
   }

   public boolean isIndexed() {
      return this.rowIndexCount() > 1;
   }

   public DeletionTime deletionTime() {
      throw new UnsupportedOperationException();
   }

   public int rowIndexCount() {
      return 0;
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE;
   }

   public void serialize(DataOutputPlus indexFile, long basePosition) throws IOException {
      throw new UnsupportedOperationException("This should only be called for indexed entries.");
   }
}
