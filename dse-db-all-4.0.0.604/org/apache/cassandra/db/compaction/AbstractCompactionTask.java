package org.apache.cassandra.db.compaction;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.WrappedRunnable;

public abstract class AbstractCompactionTask extends WrappedRunnable {
   protected final ColumnFamilyStore cfs;
   protected LifecycleTransaction transaction;
   protected boolean isUserDefined;
   protected OperationType compactionType;

   public AbstractCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction) {
      this.cfs = cfs;
      this.transaction = transaction;
      this.isUserDefined = false;
      this.compactionType = OperationType.COMPACTION;
      final Set<SSTableReader> compacting = transaction.tracker.getCompacting();
      for (final SSTableReader sstable : transaction.originals()) {
         assert compacting.contains(sstable) : sstable.getFilename() + " is not correctly marked compacting";
      }
      this.validateSSTables(transaction.originals());
   }

   private void validateSSTables(Set<SSTableReader> sstables) {
      if(!sstables.isEmpty()) {
         Iterator<SSTableReader> iter = sstables.iterator();
         SSTableReader first = (SSTableReader)iter.next();
         boolean isRepaired = first.isRepaired();
         UUID pendingRepair = first.getPendingRepair();

         while(iter.hasNext()) {
            SSTableReader next = (SSTableReader)iter.next();
            Preconditions.checkArgument(isRepaired == next.isRepaired(), "Cannot compact repaired and unrepaired sstables");
            if(pendingRepair == null) {
               Preconditions.checkArgument(!next.isPendingRepair(), "Cannot compact pending repair and non-pending repair sstables");
            } else {
               Preconditions.checkArgument(next.isPendingRepair(), "Cannot compact pending repair and non-pending repair sstables");
               Preconditions.checkArgument(pendingRepair.equals(next.getPendingRepair()), "Cannot compact sstables from different pending repairs");
            }
         }
      }

   }

   public int execute(CompactionManager.CompactionExecutorStatsCollector collector) {
      int var2;
      try {
         var2 = this.executeInternal(collector);
      } catch (FSDiskFullWriteError var7) {
         RuntimeException cause = new RuntimeException("Converted from FSDiskFullWriteError: " + var7.getMessage());
         cause.setStackTrace(var7.getStackTrace());
         throw new RuntimeException("Throwing new Runtime to bypass exception handler when disk is full", cause);
      } finally {
         this.transaction.close();
      }

      return var2;
   }

   public abstract CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore var1, Directories var2, LifecycleTransaction var3, Set<SSTableReader> var4);

   protected abstract int executeInternal(CompactionManager.CompactionExecutorStatsCollector var1);

   public AbstractCompactionTask setUserDefined(boolean isUserDefined) {
      this.isUserDefined = isUserDefined;
      return this;
   }

   public AbstractCompactionTask setCompactionType(OperationType compactionType) {
      this.compactionType = compactionType;
      return this;
   }

   public String toString() {
      return "CompactionTask(" + this.transaction + ")";
   }
}
