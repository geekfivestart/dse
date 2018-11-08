package org.apache.cassandra.db.compaction;

import com.datastax.bdp.cassandra.db.tiered.RangeAwareWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredRangeAwareSSTableWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageStrategy;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.writers.AbstractTieredCompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class RelocationCompactionTask extends CompactionTask {
   private final TieredStorageStrategy strategy;

   public RelocationCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, TieredStorageStrategy strategy) {
      super(cfs, txn, gcBefore);
      this.strategy = strategy;
   }

   public String toString() {
      return "RelocationCompactionTask{strategy=" + this.strategy + '}';
   }

   public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction transaction, Set<SSTableReader> nonExpiredSSTables) {
      return new RelocationCompactionTask.Writer(this.strategy, cfs, directories, transaction, nonExpiredSSTables, this.keepOriginals);
   }

   private static class Writer extends AbstractTieredCompactionAwareWriter {
      private final LifecycleTransaction transaction;

      public Writer(TieredStorageStrategy strategy, ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals) {
         super(strategy, cfs, directories, txn, nonExpiredSSTables, keepOriginals);
         this.transaction = txn;
      }

      protected AbstractTieredCompactionAwareWriter.CompactionRowWriter createRowWriter(TieredStorageStrategy strategy, LifecycleTransaction transaction) {
         return new RelocationCompactionTask.Writer.RowWriter(strategy, transaction);
      }

      protected Throwable doAbort(Throwable accumulate) {
         accumulate = super.doAbort(accumulate);
         return this.transaction.abort(accumulate);
      }

      protected void doPrepare() {
         super.doPrepare();
         this.transaction.prepareToCommit();
      }

      protected Throwable doCommit(Throwable accumulate) {
         accumulate = super.doCommit(accumulate);
         return this.transaction.commit(accumulate);
      }

      private class RowWriter extends AbstractTieredCompactionAwareWriter.CompactionRowWriter {
         public RowWriter(TieredStorageStrategy strategy, LifecycleTransaction transaction) {
            super(strategy, transaction);
         }

         protected RangeAwareWriter createRangeAwareWriterForTier(TieredStorageStrategy.Tier tier) {
            long keyCount = Writer.this.estimatedKeys();
            long repairedAt = CompactionTask.getMinRepairedAt(Writer.this.nonExpiredSSTables);
            UUID pendingRepair = CompactionTask.getPendingRepair(Writer.this.nonExpiredSSTables);
            SerializationHeader header = SerializationHeader.make(this.strategy.getCfs().metadata(), Writer.this.nonExpiredSSTables);
            MetadataCollector collector = new MetadataCollector(Writer.this.cfs.metadata().comparator);
            return new TieredRangeAwareSSTableWriter(Writer.this.cfs, tier.getDirectories(), keyCount, repairedAt, pendingRepair, collector, header, Writer.this.txn, Writer.this.cfs.indexManager.listIndexes());
         }
      }
   }
}
