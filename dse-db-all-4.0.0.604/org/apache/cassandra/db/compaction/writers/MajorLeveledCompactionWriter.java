package org.apache.cassandra.db.compaction.writers;

import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MajorLeveledCompactionWriter extends CompactionAwareWriter {
   private final long maxSSTableSize;
   private int currentLevel;
   private long averageEstimatedKeysPerSSTable;
   private long partitionsWritten;
   private long totalWrittenInLevel;
   private int sstablesWritten;
   private final long keysPerSSTable;
   private Directories.DataDirectory sstableDirectory;
   private final int levelFanoutSize;

   public MajorLeveledCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize) {
      this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, false);
   }

   /** @deprecated */
   @Deprecated
   public MajorLeveledCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, boolean offline, boolean keepOriginals) {
      this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, keepOriginals);
   }

   public MajorLeveledCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, boolean keepOriginals) {
      super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
      this.currentLevel = 1;
      this.partitionsWritten = 0L;
      this.totalWrittenInLevel = 0L;
      this.sstablesWritten = 0;
      this.maxSSTableSize = maxSSTableSize;
      this.levelFanoutSize = cfs.getLevelFanoutSize();
      long estimatedSSTables = Math.max(1L, SSTableReader.getTotalBytes(nonExpiredSSTables) / maxSSTableSize);
      this.keysPerSSTable = this.estimatedTotalKeys / estimatedSSTables;
   }

   public boolean realAppend(UnfilteredRowIterator partition) {
      RowIndexEntry rie = this.sstableWriter.append(partition);
      ++this.partitionsWritten;
      long totalWrittenInCurrentWriter = this.sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten();
      if(totalWrittenInCurrentWriter > this.maxSSTableSize) {
         this.totalWrittenInLevel += totalWrittenInCurrentWriter;
         if(this.totalWrittenInLevel > LeveledManifest.maxBytesForLevel(this.currentLevel, this.levelFanoutSize, this.maxSSTableSize)) {
            this.totalWrittenInLevel = 0L;
            ++this.currentLevel;
         }

         this.switchCompactionLocation(this.sstableDirectory);
      }

      return rie != null;
   }

   public void switchCompactionLocation(Directories.DataDirectory location) {
      this.sstableDirectory = location;
      this.averageEstimatedKeysPerSSTable = Math.round(((double)this.averageEstimatedKeysPerSSTable * (double)this.sstablesWritten + (double)this.partitionsWritten) / (double)(this.sstablesWritten + 1));
      this.sstableWriter.switchWriter(SSTableWriter.create(this.cfs.newSSTableDescriptor(this.getDirectories().getLocationForDisk(this.sstableDirectory)), Long.valueOf(this.keysPerSSTable), Long.valueOf(this.minRepairedAt), this.pendingRepair, this.cfs.metadata, new MetadataCollector(this.txn.originals(), this.cfs.metadata().comparator, this.currentLevel), SerializationHeader.make(this.cfs.metadata(), this.txn.originals()), this.cfs.indexManager.listIndexes(), this.txn));
      this.partitionsWritten = 0L;
      this.sstablesWritten = 0;
   }
}
