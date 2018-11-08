package org.apache.cassandra.db.compaction.writers;

import java.util.Arrays;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplittingSizeTieredCompactionWriter extends CompactionAwareWriter {
   private static final Logger logger = LoggerFactory.getLogger(SplittingSizeTieredCompactionWriter.class);
   public static final long DEFAULT_SMALLEST_SSTABLE_BYTES = 50000000L;
   private final double[] ratios;
   private final long totalSize;
   private final Set<SSTableReader> allSSTables;
   private long currentBytesToWrite;
   private int currentRatioIndex;
   private Directories.DataDirectory location;

   public SplittingSizeTieredCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
      this(cfs, directories, txn, nonExpiredSSTables, 50000000L);
   }

   public SplittingSizeTieredCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long smallestSSTable) {
      super(cfs, directories, txn, nonExpiredSSTables, false, false);
      this.currentRatioIndex = 0;
      this.allSSTables = txn.originals();
      this.totalSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType());
      double[] potentialRatios = new double[20];
      double currentRatio = 1.0D;

      int noPointIndex;
      for(noPointIndex = 0; noPointIndex < potentialRatios.length; ++noPointIndex) {
         currentRatio /= 2.0D;
         potentialRatios[noPointIndex] = currentRatio;
      }

      noPointIndex = 0;
      double[] var11 = potentialRatios;
      int var12 = potentialRatios.length;

      for(int var13 = 0; var13 < var12; ++var13) {
         double ratio = var11[var13];
         ++noPointIndex;
         if(ratio * (double)this.totalSize < (double)smallestSSTable) {
            break;
         }
      }

      this.ratios = Arrays.copyOfRange(potentialRatios, 0, noPointIndex);
      this.currentBytesToWrite = Math.round((double)this.totalSize * this.ratios[this.currentRatioIndex]);
   }

   public boolean realAppend(UnfilteredRowIterator partition) {
      RowIndexEntry rie = this.sstableWriter.append(partition);
      if(this.sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > this.currentBytesToWrite && this.currentRatioIndex < this.ratios.length - 1) {
         ++this.currentRatioIndex;
         this.currentBytesToWrite = Math.round((double)this.totalSize * this.ratios[this.currentRatioIndex]);
         this.switchCompactionLocation(this.location);
         logger.debug("Switching writer, currentBytesToWrite = {}", Long.valueOf(this.currentBytesToWrite));
      }

      return rie != null;
   }

   public void switchCompactionLocation(Directories.DataDirectory location) {
      this.location = location;
      long currentPartitionsToWrite = Math.round(this.ratios[this.currentRatioIndex] * (double)this.estimatedTotalKeys);
      SSTableWriter writer = SSTableWriter.create(this.cfs.newSSTableDescriptor(this.getDirectories().getLocationForDisk(location)), Long.valueOf(currentPartitionsToWrite), Long.valueOf(this.minRepairedAt), this.pendingRepair, this.cfs.metadata, new MetadataCollector(this.allSSTables, this.cfs.metadata().comparator, 0), SerializationHeader.make(this.cfs.metadata(), this.nonExpiredSSTables), this.cfs.indexManager.listIndexes(), this.txn);
      logger.trace("Switching writer, currentPartitionsToWrite = {}", Long.valueOf(currentPartitionsToWrite));
      this.sstableWriter.switchWriter(writer);
   }
}
