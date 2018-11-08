package org.apache.cassandra.db.compaction.writers;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompactionAwareWriter extends Transactional.AbstractTransactional implements Transactional {
   protected static final Logger logger = LoggerFactory.getLogger(CompactionAwareWriter.class);
   protected final ColumnFamilyStore cfs;
   protected final Directories directories;
   protected final Set<SSTableReader> nonExpiredSSTables;
   protected final long estimatedTotalKeys;
   protected final long maxAge;
   protected final long minRepairedAt;
   protected final UUID pendingRepair;
   protected final SSTableRewriter sstableWriter;
   protected final LifecycleTransaction txn;
   private final List<Directories.DataDirectory> locations;
   private final List<PartitionPosition> diskBoundaries;
   private int locationIndex;

   /** @deprecated */
   @Deprecated
   public CompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean offline, boolean keepOriginals) {
      this(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
   }

   public CompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals) {
      this.cfs = cfs;
      this.directories = directories;
      this.nonExpiredSSTables = nonExpiredSSTables;
      this.txn = txn;
      this.estimatedTotalKeys = SSTableReader.getApproximateKeyCount(nonExpiredSSTables);
      this.maxAge = CompactionTask.getMaxDataAge(nonExpiredSSTables);
      this.sstableWriter = SSTableRewriter.construct(cfs, txn, keepOriginals, this.maxAge);
      this.minRepairedAt = CompactionTask.getMinRepairedAt(nonExpiredSSTables);
      this.pendingRepair = CompactionTask.getPendingRepair(nonExpiredSSTables);
      DiskBoundaries db = cfs.getDiskBoundaries();
      this.diskBoundaries = db.positions;
      this.locations = db.directories;
      this.locationIndex = -1;
   }

   protected Throwable doAbort(Throwable accumulate) {
      return this.sstableWriter.abort(accumulate);
   }

   protected Throwable doCommit(Throwable accumulate) {
      return this.sstableWriter.commit(accumulate);
   }

   protected void doPrepare() {
      this.sstableWriter.prepareToCommit();
   }

   public Collection<SSTableReader> finish() {
      super.finish();
      return this.sstableWriter.finished();
   }

   public long estimatedKeys() {
      return this.estimatedTotalKeys;
   }

   public final boolean append(UnfilteredRowIterator partition) {
      this.maybeSwitchWriter(partition.partitionKey());
      return this.realAppend(partition);
   }

   protected Throwable doPostCleanup(Throwable accumulate) {
      this.sstableWriter.close();
      return super.doPostCleanup(accumulate);
   }

   protected abstract boolean realAppend(UnfilteredRowIterator var1);

   protected void maybeSwitchWriter(DecoratedKey key) {
      if(this.diskBoundaries == null) {
         if(this.locationIndex < 0) {
            Directories.DataDirectory defaultLocation = this.getWriteDirectory(this.nonExpiredSSTables, this.cfs.getExpectedCompactedFileSize(this.nonExpiredSSTables, OperationType.UNKNOWN));
            this.switchCompactionLocation(defaultLocation);
            this.locationIndex = 0;
         }

      } else if(this.locationIndex <= -1 || key.compareTo((PartitionPosition)this.diskBoundaries.get(this.locationIndex)) >= 0) {
         int prevIdx;
         for(prevIdx = this.locationIndex; this.locationIndex == -1 || key.compareTo((PartitionPosition)this.diskBoundaries.get(this.locationIndex)) > 0; ++this.locationIndex) {
            ;
         }

         if(prevIdx >= 0) {
            logger.debug("Switching write location from {} to {}", this.locations.get(prevIdx), this.locations.get(this.locationIndex));
         }

         this.switchCompactionLocation((Directories.DataDirectory)this.locations.get(this.locationIndex));
      }
   }

   protected abstract void switchCompactionLocation(Directories.DataDirectory var1);

   public Directories getDirectories() {
      return this.directories;
   }

   public Directories.DataDirectory getWriteDirectory(Iterable<SSTableReader> sstables, long estimatedWriteSize) {
      File directory = null;
      Iterator var5 = sstables.iterator();

      while(var5.hasNext()) {
         SSTableReader sstable = (SSTableReader)var5.next();
         if(directory == null) {
            directory = sstable.descriptor.directory;
         }

         if(!directory.equals(sstable.descriptor.directory)) {
            logger.trace("All sstables not from the same disk - putting results in {}", directory);
            break;
         }
      }

      Directories.DataDirectory d = this.getDirectories().getDataDirectoryForFile(directory);
      if(d != null) {
         long availableSpace = d.getAvailableSpace();
         if(availableSpace < estimatedWriteSize) {
            throw new RuntimeException(String.format("Not enough space to write %s to %s (%s available)", new Object[]{FBUtilities.prettyPrintMemory(estimatedWriteSize), d.location, FBUtilities.prettyPrintMemory(availableSpace)}));
         } else {
            logger.trace("putting compaction results in {}", directory);
            return d;
         }
      } else {
         d = this.getDirectories().getWriteableLocation(estimatedWriteSize);
         if(d == null) {
            throw new RuntimeException(String.format("Not enough disk space to store %s", new Object[]{FBUtilities.prettyPrintMemory(estimatedWriteSize)}));
         } else {
            return d;
         }
      }
   }

   public CompactionAwareWriter setRepairedAt(long repairedAt) {
      this.sstableWriter.setRepairedAt(repairedAt);
      return this;
   }
}
