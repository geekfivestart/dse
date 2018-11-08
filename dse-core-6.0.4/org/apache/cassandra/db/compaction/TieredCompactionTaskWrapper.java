package org.apache.cassandra.db.compaction;

import com.datastax.bdp.cassandra.db.tiered.TieredStorageStrategy;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.TieredCompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class TieredCompactionTaskWrapper extends CompactionTask {
   private final TieredStorageStrategy strategy;
   private final TieredStorageStrategy.Tier tier;
   private final CompactionTask task;

   public TieredCompactionTaskWrapper(TieredStorageStrategy strategy, TieredStorageStrategy.Tier tier, CompactionTask task) {
      super(task.cfs, task.transaction, task.gcBefore, task.keepOriginals);
      this.strategy = strategy;
      this.tier = tier;
      this.task = task;
   }

   public String toString() {
      return "TieredCompactionTaskWrapper{tier=" + this.tier.getLevel() + ", task=" + this.task + ", strategy=" + this.strategy + '}';
   }

   public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction transaction, Set<SSTableReader> nonExpiredSSTables) {
      CompactionAwareWriter writer = this.task.getCompactionAwareWriter(cfs, this.tier.getDirectories(), transaction, nonExpiredSSTables);
      return new TieredCompactionAwareWriter(this.strategy, cfs, this.tier, writer, nonExpiredSSTables, this.keepOriginals);
   }

   protected Directories getDirectories() {
      return this.tier.getDirectories();
   }

   protected void buildCompactionCandidatesForAvailableDiskSpace(Set<SSTableReader> fullyExpiredSSTables) {
      super.buildCompactionCandidatesForAvailableDiskSpace(fullyExpiredSSTables);
   }

   protected int getLevel() {
      return this.task.getLevel();
   }

   protected boolean partialCompactionsAcceptable() {
      return this.task.partialCompactionsAcceptable();
   }

   protected CompactionController getCompactionController(Set<SSTableReader> toCompact) {
      return this.task.getCompactionController(toCompact);
   }
}
