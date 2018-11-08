package org.apache.cassandra.db.compaction.writers;

import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCompactionWriter extends CompactionAwareWriter {
   protected static final Logger logger = LoggerFactory.getLogger(DefaultCompactionWriter.class);
   private final int sstableLevel;

   public DefaultCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
      this(cfs, directories, txn, nonExpiredSSTables, false, 0);
   }

   /** @deprecated */
   @Deprecated
   public DefaultCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean offline, boolean keepOriginals, int sstableLevel) {
      this(cfs, directories, txn, nonExpiredSSTables, keepOriginals, sstableLevel);
   }

   public DefaultCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals, int sstableLevel) {
      super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
      this.sstableLevel = sstableLevel;
   }

   public boolean realAppend(UnfilteredRowIterator partition) {
      return this.sstableWriter.append(partition) != null;
   }

   public void switchCompactionLocation(Directories.DataDirectory directory) {
      SSTableWriter writer = SSTableWriter.create(this.cfs.newSSTableDescriptor(this.getDirectories().getLocationForDisk(directory)), Long.valueOf(this.estimatedTotalKeys), Long.valueOf(this.minRepairedAt), this.pendingRepair, this.cfs.metadata, new MetadataCollector(this.txn.originals(), this.cfs.metadata().comparator, this.sstableLevel), SerializationHeader.make(this.cfs.metadata(), this.nonExpiredSSTables), this.cfs.indexManager.listIndexes(), this.txn);
      this.sstableWriter.switchWriter(writer);
   }

   public long estimatedKeys() {
      return this.estimatedTotalKeys;
   }
}
