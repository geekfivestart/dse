package com.datastax.bdp.cassandra.db.tiered;

import java.util.Collection;
import java.util.List;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;

public abstract class RangeAwareWriter extends AbstractRangeAwareWriter implements Transactional {
   private final DataDirectory[] locations;
   private final List<PartitionPosition> boundaries;
   protected boolean openResult = false;

   public RangeAwareWriter(ColumnFamilyStore cfs, Directories directories) {
      assert cfs != null;

      assert directories != null;

      this.locations = directories.getWriteableLocations();

      assert this.locations != null && this.locations.length > 0;

      DiskBoundaries db = cfs.getDiskBoundaries(directories);
      this.boundaries = db.positions;
   }

   protected List<PartitionPosition> getBoundaries() {
      return this.boundaries;
   }

   protected DataDirectory[] getLocations() {
      return this.locations;
   }

   public RangeAwareWriter setOpenResult(boolean openResult) {
      this.openResult = openResult;
      return this;
   }

   protected abstract boolean realAppend(UnfilteredRowIterator var1);

   public abstract Collection<SSTableReader> finish(boolean var1);

   public boolean append(UnfilteredRowIterator partition) {
      this.maybeSwitchWriter(partition.partitionKey());
      return this.realAppend(partition);
   }
}
