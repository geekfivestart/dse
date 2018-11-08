package org.apache.cassandra.db.compaction;

import java.util.Set;
import java.util.function.LongPredicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableSplitter {
   private final SSTableSplitter.SplittingCompactionTask task;
   private CompactionInfo.Holder info;

   public SSTableSplitter(ColumnFamilyStore cfs, LifecycleTransaction transaction, int sstableSizeInMB) {
      this.task = new SSTableSplitter.SplittingCompactionTask(cfs, transaction, sstableSizeInMB);
   }

   public void split() {
      this.task.execute(new SSTableSplitter.StatsCollector());
   }

   public static class SplitController extends CompactionController {
      public SplitController(ColumnFamilyStore cfs) {
         super(cfs, -2147483648);
      }

      public LongPredicate getPurgeEvaluator(DecoratedKey key) {
         return (time) -> {
            return false;
         };
      }
   }

   public static class SplittingCompactionTask extends CompactionTask {
      private final int sstableSizeInMB;

      public SplittingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, int sstableSizeInMB) {
         super(cfs, transaction, -2147483648, false);
         this.sstableSizeInMB = sstableSizeInMB;
         if(sstableSizeInMB <= 0) {
            throw new IllegalArgumentException("Invalid target size for SSTables, must be > 0 (got: " + sstableSizeInMB + ")");
         }
      }

      protected CompactionController getCompactionController(Set<SSTableReader> toCompact) {
         return new SSTableSplitter.SplitController(this.cfs);
      }

      public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
         return new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, (long)this.sstableSizeInMB * 1024L * 1024L, 0, false);
      }

      protected boolean partialCompactionsAcceptable() {
         return true;
      }
   }

   public class StatsCollector implements CompactionManager.CompactionExecutorStatsCollector {
      public StatsCollector() {
      }

      public void beginCompaction(CompactionInfo.Holder ci) {
         SSTableSplitter.this.info = ci;
      }

      public void finishCompaction(CompactionInfo.Holder ci) {
      }
   }
}
