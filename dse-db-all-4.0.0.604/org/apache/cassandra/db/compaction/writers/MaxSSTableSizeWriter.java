package org.apache.cassandra.db.compaction.writers;

import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MaxSSTableSizeWriter extends CompactionAwareWriter {
   private final long maxSSTableSize;
   private final int level;
   private final long estimatedSSTables;
   private final Set<SSTableReader> allSSTables;
   private Directories.DataDirectory sstableDirectory;

   public MaxSSTableSizeWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, int level) {
      this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, false);
   }

   /** @deprecated */
   @Deprecated
   public MaxSSTableSizeWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, int level, boolean offline, boolean keepOriginals) {
      this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, keepOriginals);
   }

   public MaxSSTableSizeWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, int level, boolean keepOriginals) {
      super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
      this.allSSTables = txn.originals();
      this.level = level;
      this.maxSSTableSize = maxSSTableSize;
      long totalSize = getTotalWriteSize(nonExpiredSSTables, this.estimatedTotalKeys, cfs, txn.opType());
      this.estimatedSSTables = Math.max(1L, totalSize / maxSSTableSize);
   }

   private static long getTotalWriteSize(Iterable<SSTableReader> nonExpiredSSTables, long estimatedTotalKeys, ColumnFamilyStore cfs, OperationType compactionType) {
      long estimatedKeysBeforeCompaction = 0L;

      SSTableReader sstable;
      for(Iterator var7 = nonExpiredSSTables.iterator(); var7.hasNext(); estimatedKeysBeforeCompaction += sstable.estimatedKeys()) {
         sstable = (SSTableReader)var7.next();
      }

      estimatedKeysBeforeCompaction = Math.max(1L, estimatedKeysBeforeCompaction);
      double estimatedCompactionRatio = (double)estimatedTotalKeys / (double)estimatedKeysBeforeCompaction;
      return Math.round(estimatedCompactionRatio * (double)cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
   }

   protected boolean realAppend(UnfilteredRowIterator partition) {
      RowIndexEntry rie = this.sstableWriter.append(partition);
      if(this.sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > this.maxSSTableSize) {
         this.switchCompactionLocation(this.sstableDirectory);
      }

      return rie != null;
   }

   public void switchCompactionLocation(Directories.DataDirectory location) {
      this.sstableDirectory = location;
      SSTableWriter writer = SSTableWriter.create(this.cfs.newSSTableDescriptor(this.getDirectories().getLocationForDisk(this.sstableDirectory)), Long.valueOf(this.estimatedTotalKeys / this.estimatedSSTables), Long.valueOf(this.minRepairedAt), this.pendingRepair, this.cfs.metadata, new MetadataCollector(this.allSSTables, this.cfs.metadata().comparator, this.level), SerializationHeader.make(this.cfs.metadata(), this.nonExpiredSSTables), this.cfs.indexManager.listIndexes(), this.txn);
      this.sstableWriter.switchWriter(writer);
   }
}
