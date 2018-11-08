package org.apache.cassandra.repair.consistent;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingAntiCompaction {
   private static final Logger logger = LoggerFactory.getLogger(PendingAntiCompaction.class);
   private final UUID prsId;
   private final Collection<Range<Token>> ranges;
   private final ExecutorService executor;

   public PendingAntiCompaction(UUID prsId, Collection<Range<Token>> ranges, ExecutorService executor) {
      this.prsId = prsId;
      this.ranges = ranges;
      this.executor = executor;
   }

   public ListenableFuture run() {
      ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(this.prsId);
      Collection<ColumnFamilyStore> cfss = prs.getColumnFamilyStores();
      List<ListenableFutureTask<PendingAntiCompaction.AcquireResult>> tasks = new ArrayList(cfss.size());
      Iterator var4 = cfss.iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
         cfs.forceBlockingFlush();
         ListenableFutureTask<PendingAntiCompaction.AcquireResult> task = ListenableFutureTask.create(new PendingAntiCompaction.AcquisitionCallable(cfs, this.ranges, this.prsId));
         this.executor.submit(task);
         tasks.add(task);
      }

      ListenableFuture<List<PendingAntiCompaction.AcquireResult>> acquisitionResults = Futures.successfulAsList(tasks);
      ListenableFuture compactionResult = Futures.transform(acquisitionResults, new PendingAntiCompaction.AcquisitionCallback(this.prsId, this.ranges));
      return compactionResult;
   }

   static class AcquisitionCallback implements AsyncFunction<List<PendingAntiCompaction.AcquireResult>, Object> {
      private final UUID parentRepairSession;
      private final Collection<Range<Token>> ranges;

      public AcquisitionCallback(UUID parentRepairSession, Collection<Range<Token>> ranges) {
         this.parentRepairSession = parentRepairSession;
         this.ranges = ranges;
      }

      ListenableFuture<?> submitPendingAntiCompaction(PendingAntiCompaction.AcquireResult result) {
         return CompactionManager.instance.submitPendingAntiCompaction(result.cfs, this.ranges, result.refs, result.txn, this.parentRepairSession);
      }

      public ListenableFuture apply(List<PendingAntiCompaction.AcquireResult> results) throws Exception {
         if(Iterables.any(results, (t) -> {
            return t == null;
         })) {
            Iterator var6 = results.iterator();

            while(var6.hasNext()) {
               PendingAntiCompaction.AcquireResult result = (PendingAntiCompaction.AcquireResult)var6.next();
               if(result != null) {
                  PendingAntiCompaction.logger.info("Releasing acquired sstables for {}.{}", result.cfs.metadata.keyspace, result.cfs.metadata.name);
                  result.abort();
               }
            }

            return Futures.immediateFailedFuture(new PendingAntiCompaction.SSTableAcquisitionException());
         } else {
            List<ListenableFuture<?>> pendingAntiCompactions = new ArrayList(results.size());
            Iterator var3 = results.iterator();

            while(var3.hasNext()) {
               PendingAntiCompaction.AcquireResult result = (PendingAntiCompaction.AcquireResult)var3.next();
               if(result.txn != null) {
                  ListenableFuture<?> future = this.submitPendingAntiCompaction(result);
                  pendingAntiCompactions.add(future);
               }
            }

            return Futures.allAsList(pendingAntiCompactions);
         }
      }
   }

   static class AcquisitionCallable implements Callable<PendingAntiCompaction.AcquireResult> {
      private final ColumnFamilyStore cfs;
      private final Collection<Range<Token>> ranges;
      private final UUID sessionID;

      public AcquisitionCallable(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID sessionID) {
         this.cfs = cfs;
         this.ranges = ranges;
         this.sessionID = sessionID;
      }

      private Iterable<SSTableReader> getSSTables() {
         return Iterables.filter(this.cfs.getLiveSSTables(), (s) -> {
            return !s.isRepaired() && !s.isPendingRepair() && s.intersects(this.ranges);
         });
      }

      private PendingAntiCompaction.AcquireResult acquireTuple() {
         List<SSTableReader> sstables = Lists.newArrayList(this.getSSTables());
         if(sstables.isEmpty()) {
            return new PendingAntiCompaction.AcquireResult(this.cfs, (Refs)null, (LifecycleTransaction)null);
         } else {
            LifecycleTransaction txn = this.cfs.getTracker().tryModify((Iterable)sstables, OperationType.ANTICOMPACTION);
            return txn != null?new PendingAntiCompaction.AcquireResult(this.cfs, Refs.ref(sstables), txn):null;
         }
      }

      public PendingAntiCompaction.AcquireResult call() throws Exception {
         PendingAntiCompaction.logger.debug("acquiring sstables for pending anti compaction on session {}", this.sessionID);
         PendingAntiCompaction.AcquireResult refTxn = this.acquireTuple();
         return refTxn != null?refTxn:(PendingAntiCompaction.AcquireResult)this.cfs.runWithCompactionsDisabled(this::acquireTuple, false, false);
      }
   }

   static class SSTableAcquisitionException extends RuntimeException {
      SSTableAcquisitionException() {
      }
   }

   static class AcquireResult {
      final ColumnFamilyStore cfs;
      final Refs<SSTableReader> refs;
      final LifecycleTransaction txn;

      AcquireResult(ColumnFamilyStore cfs, Refs<SSTableReader> refs, LifecycleTransaction txn) {
         this.cfs = cfs;
         this.refs = refs;
         this.txn = txn;
      }

      void abort() {
         if(this.txn != null) {
            this.txn.abort();
         }

         if(this.refs != null) {
            this.refs.release();
         }

      }
   }
}
