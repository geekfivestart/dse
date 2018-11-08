package org.apache.cassandra.db.compaction.writers;

import com.datastax.bdp.cassandra.db.tiered.RangeAwareWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredRangeAwareSSTableWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredRowWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageStrategy;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredCompactionAwareWriter extends AbstractTieredCompactionAwareWriter {
   private static final Logger logger = LoggerFactory.getLogger(TieredCompactionAwareWriter.class);
   private final TieredStorageStrategy.Tier tier;
   private final CompactionAwareWriter wrappedWriter;
   private final TieredCompactionAwareWriter.RewriterConsumer rewriterConsumer;

   public TieredCompactionAwareWriter(TieredStorageStrategy strategy, ColumnFamilyStore cfs, TieredStorageStrategy.Tier tier, CompactionAwareWriter wrappedWriter, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals) {
      super(strategy, cfs, wrappedWriter.directories, wrappedWriter.txn, nonExpiredSSTables, keepOriginals);
      this.tier = tier;
      this.wrappedWriter = wrappedWriter;
      this.rewriterConsumer = new TieredCompactionAwareWriter.RewriterConsumer(cfs, tier.getDirectories());
   }

   protected AbstractTieredCompactionAwareWriter.CompactionRowWriter createRowWriter(TieredStorageStrategy strategy, LifecycleTransaction transaction) {
      return new TieredCompactionAwareWriter.RowWriter(strategy, transaction);
   }

   private class RowWriter extends AbstractTieredCompactionAwareWriter.CompactionRowWriter {
      public RowWriter(TieredStorageStrategy strategy, LifecycleTransaction transaction) {
         super(strategy, transaction);
      }

      protected RangeAwareWriter createRangeAwareWriterForTier(TieredStorageStrategy.Tier tier) {
         if(TieredCompactionAwareWriter.this.tier.getLevel() == tier.getLevel()) {
            return TieredCompactionAwareWriter.this.rewriterConsumer;
         } else {
            long keyCount = TieredCompactionAwareWriter.this.wrappedWriter.estimatedKeys();
            long repairedAt = CompactionTask.getMinRepairedAt(TieredCompactionAwareWriter.this.nonExpiredSSTables);
            UUID pendingRepair = CompactionTask.getPendingRepair(TieredCompactionAwareWriter.this.nonExpiredSSTables);
            SerializationHeader header = SerializationHeader.make(this.strategy.getCfs().metadata(), TieredCompactionAwareWriter.this.nonExpiredSSTables);
            MetadataCollector collector = new MetadataCollector(TieredCompactionAwareWriter.this.cfs.metadata().comparator);
            return new TieredRangeAwareSSTableWriter(TieredCompactionAwareWriter.this.cfs, tier.getDirectories(), keyCount, repairedAt, pendingRepair, collector, header, TieredCompactionAwareWriter.this.txn, TieredCompactionAwareWriter.this.cfs.indexManager.listIndexes());
         }
      }

      public Collection<RangeAwareWriter> writersForTxn() {
         Collection<RangeAwareWriter> writers = super.writersForTxn();
         writers.remove(TieredCompactionAwareWriter.this.rewriterConsumer);
         return writers;
      }

      protected void doPrepare() {
         super.doPrepare();
         TieredCompactionAwareWriter.logger.debug("preparing to commit {}", TieredCompactionAwareWriter.this.wrappedWriter);
         TieredCompactionAwareWriter.this.wrappedWriter.prepareToCommit();
      }

      public Set<SSTableReader> finish() {
         Set<SSTableReader> sstables = super.finish();
         sstables.addAll(TieredCompactionAwareWriter.this.wrappedWriter.sstableWriter.finished());
         return sstables;
      }

      protected Throwable doAbort(Throwable accumulate) {
         accumulate = super.doAbort(accumulate);
         TieredCompactionAwareWriter.logger.debug("aborting {}", TieredCompactionAwareWriter.this.wrappedWriter);
         return TieredCompactionAwareWriter.this.wrappedWriter.abort(accumulate);
      }

      protected Throwable doCommit(Throwable accumulate) {
         accumulate = super.doCommit(accumulate);
         TieredCompactionAwareWriter.logger.debug("committing {}", TieredCompactionAwareWriter.this.wrappedWriter);
         return TieredCompactionAwareWriter.this.wrappedWriter.commit(accumulate);
      }
   }

   private class RewriterConsumer extends RangeAwareWriter implements TieredRowWriter.TierPartitionConsumer {
      RewriterConsumer(ColumnFamilyStore cfs, Directories directories) {
         super(cfs, directories);
      }

      public String toString() {
         return "RewriterConsumer{wrappedWriter=" + TieredCompactionAwareWriter.this.wrappedWriter + "}";
      }

      protected boolean realAppend(UnfilteredRowIterator partition) {
         return TieredCompactionAwareWriter.this.wrappedWriter.realAppend(partition);
      }

      protected void switchWriteLocation(DataDirectory directory) {
         LifecycleTransaction var2 = TieredCompactionAwareWriter.this.txn;
         synchronized(TieredCompactionAwareWriter.this.txn) {
            TieredCompactionAwareWriter.this.wrappedWriter.switchCompactionLocation(directory);
         }
      }

      public Collection<SSTableReader> finish(boolean openResult) {
         return TieredCompactionAwareWriter.this.wrappedWriter.sstableWriter.finished();
      }

      public Throwable commit(Throwable accumulate) {
         return TieredCompactionAwareWriter.this.wrappedWriter.commit(accumulate);
      }

      public Throwable abort(Throwable accumulate) {
         return TieredCompactionAwareWriter.this.wrappedWriter.abort(accumulate);
      }

      public void prepareToCommit() {
         TieredCompactionAwareWriter.this.wrappedWriter.prepareToCommit();
      }

      public void close() {
         TieredCompactionAwareWriter.this.wrappedWriter.close();
      }
   }
}
